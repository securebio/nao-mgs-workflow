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
        ],
        ids=["basic_functionality", "empty_file", "header_only"],
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

    @pytest.mark.parametrize(
        "input_content,columns,expected_output",
        [
            (
                "col1\tcol2\nval1\tval2\nval3\tval4\n",
                "new_a,new_b,new_c",
                "col1\tcol2\tnew_a\tnew_b\tnew_c\nval1\tval2\tNA\tNA\tNA\nval3\tval4\tNA\tNA\tNA\n",
            ),
            ("", "new_a,new_b", ""),
            (
                "col1\tcol2\n",
                "new_a,new_b",
                "col1\tcol2\tnew_a\tnew_b\n",
            ),
        ],
        ids=["multi_column_basic", "multi_column_empty_file", "multi_column_header_only"],
    )
    def test_add_multiple_columns(
        self, tsv_factory, input_content, columns, expected_output
    ):
        """Test adding multiple comma-separated columns at once."""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        add_fixed_column.add_column(input_file, columns, "NA", output_file)

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    def test_multi_column_already_exists(self, tsv_factory):
        """Test that adding multiple columns raises ValueError if any exists."""
        input_file = tsv_factory.create_plain(
            "input.tsv", "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Column already exists: col2"):
            add_fixed_column.add_column(
                input_file, "new_a,col2,new_b", "NA", output_file
            )
