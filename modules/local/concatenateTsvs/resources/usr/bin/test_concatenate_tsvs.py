#!/usr/bin/env python

import pytest

import concatenate_tsvs


class TestConcatenateTsvs:
    """Test the concatenate_tsvs function."""

    @pytest.mark.parametrize(
        "files_content,expected_output",
        [
            (
                [
                    "col1\tcol2\tcol3\nval1\tval2\tval3\n",
                    "col1\tcol2\tcol3\nval4\tval5\tval6\n",
                ],
                "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n",
            ),
            (
                [
                    "col1\tcol2\tcol3\nval1\tval2\tval3\n",
                    "col3\tcol1\tcol2\nval6\tval4\tval5\n",
                ],
                "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n",
            ),
            (
                ["", ""],
                "",
            ),
            (
                ["", "col1\tcol2\nval1\tval2\n", "col1\tcol2\nval3\tval4\n"],
                "col1\tcol2\nval1\tval2\nval3\tval4\n",
            ),
            (
                ["col1\tcol2\nval1\tval2\n"],
                "col1\tcol2\nval1\tval2\n",
            ),
        ],
        ids=[
            "basic_concatenation",
            "reordered_headers",
            "all_empty_files",
            "some_empty_files",
            "single_file",
        ],
    )
    def test_concatenate_success_cases(
        self, tsv_factory, files_content, expected_output
    ):
        """Test various successful concatenation scenarios."""
        input_files = [
            tsv_factory.create_plain(f"file{i}.tsv", content)
            for i, content in enumerate(files_content)
        ]
        output_file = tsv_factory.get_path("output.tsv")

        concatenate_tsvs.concatenate_tsvs(input_files, output_file)

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    @pytest.mark.parametrize(
        "file_format",
        ["plain", "gzip"],
        ids=["plain_tsv", "gzipped_tsv"],
    )
    def test_concatenate_file_formats(self, tsv_factory, file_format):
        """Test concatenating files in different formats."""
        file1_content = "col1\tcol2\nval1\tval2\n"
        file2_content = "col1\tcol2\nval3\tval4\n"
        expected = "col1\tcol2\nval1\tval2\nval3\tval4\n"

        if file_format == "plain":
            file1 = tsv_factory.create_plain("file1.tsv", file1_content)
            file2 = tsv_factory.create_plain("file2.tsv", file2_content)
            output_file = tsv_factory.get_path("output.tsv")
        else:
            file1 = tsv_factory.create_gzip("file1.tsv.gz", file1_content)
            file2 = tsv_factory.create_gzip("file2.tsv.gz", file2_content)
            output_file = tsv_factory.get_path("output.tsv.gz")

        concatenate_tsvs.concatenate_tsvs([file1, file2], output_file)

        if file_format == "plain":
            result = tsv_factory.read_plain(output_file)
        else:
            result = tsv_factory.read_gzip(output_file)

        assert result == expected

    def test_mismatched_headers(self, tsv_factory):
        """Test that mismatched headers raise ValueError."""
        file1 = tsv_factory.create_plain(
            "file1.tsv", "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        )
        file2 = tsv_factory.create_plain(
            "file2.tsv", "col1\tcol2\tcol4\nval4\tval5\tval6\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Headers do not match"):
            concatenate_tsvs.concatenate_tsvs([file1, file2], output_file)
