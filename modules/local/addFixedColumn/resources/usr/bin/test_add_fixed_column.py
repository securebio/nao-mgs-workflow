#!/usr/bin/env python

import pytest

import add_fixed_column


class TestAddColumn:
    """Test the add_column function."""

    @pytest.mark.parametrize(
        "input_content,expected_output",
        [
            (
                "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n",
                "col1\tcol2\tcol3\tnew_col\nval1\tval2\tval3\tnew_value\nval4\tval5\tval6\tnew_value\n",
            ),
            ("", ""),
            ("col1\tcol2\tcol3\n", "col1\tcol2\tcol3\tnew_col\n"),
            ("\n\n\n", ""),
        ],
        ids=["basic_functionality", "empty_file", "header_only", "whitespace_only"],
    )
    def test_add_column_success_cases(
        self, tsv_factory, input_content, expected_output
    ):
        """Test adding a column to various TSV file formats."""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        add_fixed_column.add_column(input_file, "new_col", "new_value", output_file)

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    def test_column_already_exists(self, tsv_factory):
        """Test that adding an existing column raises ValueError."""
        input_file = tsv_factory.create_plain(
            "input.tsv", "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Column already exists: col2"):
            add_fixed_column.add_column(
                input_file,
                "col2",  # Column that already exists
                "new_value",
                output_file,
            )
