#!/usr/bin/env python

import pytest

import add_sample_column


class TestAddSampleColumn:
    """Test the add_sample_column function."""

    @pytest.mark.parametrize(
        "input_content,expected_output",
        [
            (
                "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n",
                "col1\tcol2\tcol3\tsample\nval1\tval2\tval3\tsample_001\nval4\tval5\tval6\tsample_001\n",
            ),
            ("", ""),
            ("col1\tcol2\tcol3\n", "col1\tcol2\tcol3\tsample\n"),
            (
                "col1\tcol2\nval1\tval2\n\nval3\tval4\n",
                "col1\tcol2\tsample\nval1\tval2\tsample_001\nval3\tval4\tsample_001\n",
            ),
        ],
        ids=["basic_functionality", "empty_file", "header_only", "empty_lines_skipped"],
    )
    def test_add_sample_column_success_cases(
        self, tsv_factory, input_content, expected_output
    ):
        """Test adding a sample column to various TSV file formats."""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        add_sample_column.add_sample_column(
            input_file, "sample_001", "sample", output_file
        )

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    @pytest.mark.parametrize(
        "file_format",
        ["plain", "gzip"],
        ids=["plain_tsv", "gzipped_tsv"],
    )
    def test_add_sample_column_file_formats(self, tsv_factory, file_format):
        """Test adding sample column with both plain and gzipped files."""
        input_content = "col1\tcol2\nval1\tval2\n"
        expected = "col1\tcol2\tsample\nval1\tval2\tsample_001\n"

        if file_format == "plain":
            input_file = tsv_factory.create_plain("input.tsv", input_content)
            output_file = tsv_factory.get_path("output.tsv")
        else:
            input_file = tsv_factory.create_gzip("input.tsv.gz", input_content)
            output_file = tsv_factory.get_path("output.tsv.gz")

        add_sample_column.add_sample_column(
            input_file, "sample_001", "sample", output_file
        )

        if file_format == "plain":
            result = tsv_factory.read_plain(output_file)
        else:
            result = tsv_factory.read_gzip(output_file)

        assert result == expected

    def test_column_already_exists(self, tsv_factory):
        """Test that adding an existing column raises ValueError."""
        input_file = tsv_factory.create_plain(
            "input.tsv", "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Sample name column already exists: col2"):
            add_sample_column.add_sample_column(
                input_file,
                "sample_001",
                "col2",  # Column that already exists
                output_file,
            )
