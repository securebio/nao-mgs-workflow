#!/usr/bin/env python

import pytest

import rehead_tsv


class TestRenameColumns:
    """Test the rename_columns function."""

    @pytest.mark.parametrize(
        "input_content,input_fields,output_fields,expected",
        [
            (
                "col1\tcol2\tcol3\nval1\tval2\tval3\n",
                ["col1", "col3"],
                ["new1", "new3"],
                "new1\tcol2\tnew3\nval1\tval2\tval3\n",
            ),
            (
                "",
                ["col1"],
                ["new1"],
                "",
            ),
            (
                "col1\tcol2\tcol3\n",
                ["col1", "col3"],
                ["new1", "new3"],
                "new1\tcol2\tnew3\n",
            ),
            (
                "col1\tcol2\tcol3\nval1\tval2\tval3\n",
                ["col2"],
                ["renamed"],
                "col1\trenamed\tcol3\nval1\tval2\tval3\n",
            ),
            (
                "col1\tcol2\tcol3\nval1\tval2\tval3\n",
                ["col1", "col2", "col3"],
                ["new1", "new2", "new3"],
                "new1\tnew2\tnew3\nval1\tval2\tval3\n",
            ),
            (
                "col1\tcol2\tcol3\nval1\tval2\tval3\n",
                ["col1", "col2"],
                ["col1", "col2"],
                "col1\tcol2\tcol3\nval1\tval2\tval3\n",
            ),
        ],
        ids=[
            "basic_rename",
            "empty_file",
            "header_only",
            "single_column",
            "all_columns",
            "no_change",
        ],
    )
    def test_rename_columns_success_cases(
        self, tsv_factory, input_content, input_fields, output_fields, expected
    ):
        """Test various successful column renaming scenarios."""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        rehead_tsv.rename_columns(input_file, input_fields, output_fields, output_file)

        result = tsv_factory.read_plain(output_file)
        assert result == expected

    @pytest.mark.parametrize(
        "file_format",
        ["plain", "gzip"],
        ids=["plain_tsv", "gzipped_tsv"],
    )
    def test_rename_columns_file_formats(self, tsv_factory, file_format):
        """Test renaming columns with different file formats."""
        input_content = "col1\tcol2\nval1\tval2\n"
        expected = "renamed\tcol2\nval1\tval2\n"

        if file_format == "plain":
            input_file = tsv_factory.create_plain("input.tsv", input_content)
            output_file = tsv_factory.get_path("output.tsv")
        else:
            input_file = tsv_factory.create_gzip("input.tsv.gz", input_content)
            output_file = tsv_factory.get_path("output.tsv.gz")

        rehead_tsv.rename_columns(input_file, ["col1"], ["renamed"], output_file)

        if file_format == "plain":
            result = tsv_factory.read_plain(output_file)
        else:
            result = tsv_factory.read_gzip(output_file)

        assert result == expected

    @pytest.mark.parametrize(
        "input_fields,output_fields,expected_match",
        [
            (
                ["col1", "col2"],
                ["new1"],
                "Input and output field lists must be the same length",
            ),
            (
                ["col1", "missing"],
                ["new1", "new_missing"],
                "Input field not found in file header",
            ),
        ],
        ids=["mismatched_lengths", "missing_input_field"],
    )
    def test_rename_columns_errors(
        self, tsv_factory, input_fields, output_fields, expected_match
    ):
        """Test error conditions for column renaming."""
        input_file = tsv_factory.create_plain(
            "input.tsv", "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match=expected_match):
            rehead_tsv.rename_columns(input_file, input_fields, output_fields, output_file)
