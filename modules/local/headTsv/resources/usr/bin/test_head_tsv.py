#!/usr/bin/env python

import pytest

import head_tsv


class TestAddHeaderLine:
    """Test the add_header_line function."""

    @pytest.mark.parametrize(
        "input_content,header_fields,expected_output",
        [
            (
                "val1\tval2\tval3\nval4\tval5\tval6\n",
                ["col1", "col2", "col3"],
                "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n",
            ),
            (
                "",
                ["col1", "col2", "col3"],
                "col1\tcol2\tcol3\n",
            ),
            (
                "val1\nval2\nval3\n",
                ["col1"],
                "col1\nval1\nval2\nval3\n",
            ),
        ],
        ids=["basic_functionality", "empty_file", "single_column"],
    )
    def test_add_header_success_cases(
        self, tsv_factory, input_content, header_fields, expected_output
    ):
        """Test adding headers to various TSV file formats."""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        head_tsv.add_header_line(input_file, header_fields, output_file)

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    @pytest.mark.parametrize(
        "file_format",
        ["plain", "gzip"],
        ids=["plain_tsv", "gzipped_tsv"],
    )
    def test_add_header_file_formats(self, tsv_factory, file_format):
        """Test adding header with both plain and gzipped files."""
        input_content = "val1\tval2\n"
        header_fields = ["col1", "col2"]
        expected = "col1\tcol2\nval1\tval2\n"

        if file_format == "plain":
            input_file = tsv_factory.create_plain("input.tsv", input_content)
            output_file = tsv_factory.get_path("output.tsv")
        else:
            input_file = tsv_factory.create_gzip("input.tsv.gz", input_content)
            output_file = tsv_factory.get_path("output.tsv.gz")

        head_tsv.add_header_line(input_file, header_fields, output_file)

        if file_format == "plain":
            result = tsv_factory.read_plain(output_file)
        else:
            result = tsv_factory.read_gzip(output_file)

        assert result == expected

    def test_mismatched_field_count(self, tsv_factory):
        """Test that mismatched field counts raise ValueError."""
        input_file = tsv_factory.create_plain("input.tsv", "val1\tval2\tval3\n")
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Number of header fields does not match"):
            head_tsv.add_header_line(
                input_file,
                ["col1", "col2"],  # Wrong number of columns
                output_file,
            )
