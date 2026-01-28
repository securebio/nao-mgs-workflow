#!/usr/bin/env python

import pytest

import add_conditional_tsv_column


class TestAddConditionalColumn:
    """Test the add_conditional_column function."""

    def test_add_conditional_column_basic(self, tsv_factory):
        """Test adding a conditional column based on check column values."""
        input_content = "x\ty\tz\n0\t100\t200\n1\t300\t400\n0\t500\t600\n"
        expected_output = "x\ty\tz\tselected\n0\t100\t200\t100\n1\t300\t400\t400\n0\t500\t600\t500\n"

        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        add_conditional_tsv_column.add_conditional_column(
            input_file,
            chk_col="x",
            match_val="0",
            if_col="y",
            else_col="z",
            new_hdr="selected",
            out_path=output_file,
        )

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    def test_add_conditional_column_empty_file(self, tsv_factory):
        """Test that empty file is handled correctly."""
        input_file = tsv_factory.create_plain("input.tsv", "")
        output_file = tsv_factory.get_path("output.tsv")

        add_conditional_tsv_column.add_conditional_column(
            input_file,
            chk_col="taxid_species",
            match_val="NA",
            if_col="aligner_taxid_lca",
            else_col="taxid_species",
            new_hdr="selected_taxid",
            out_path=output_file,
        )

        result = tsv_factory.read_plain(output_file)
        assert result == ""

    def test_add_conditional_column_header_only(self, tsv_factory):
        """Test that header-only file gets new column added to header."""
        input_content = "x\ty\tz\n"
        expected_output = "x\ty\tz\tselected\n"

        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        add_conditional_tsv_column.add_conditional_column(
            input_file,
            chk_col="x",
            match_val="0",
            if_col="y",
            else_col="z",
            new_hdr="selected",
            out_path=output_file,
        )

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output
    
    @pytest.mark.parametrize(
        ("col_to_make_missing", "missing_col_name"),
        [
            ("chk_col", "nonexistent_col"),
            ("if_col", "missing_col"),
            ("else_col", "not_there"),
        ],
    )
    def test_missing_columns(self, tsv_factory, col_to_make_missing, missing_col_name):
        input_file = tsv_factory.create_plain(
            "input.tsv", "x\ty\tz\n0\t100\t200\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        kwargs = {
            "input_path": input_file,
            "match_val": "0",
            "new_hdr": "selected",
            "out_path": output_file,
            "chk_col": "x",
            "if_col": "y",
            "else_col": "z",
        }
        kwargs[col_to_make_missing] = missing_col_name

        with pytest.raises(ValueError, match="could not find all requested columns in header"):
            add_conditional_tsv_column.add_conditional_column(**kwargs)

    @pytest.mark.parametrize("quote_string", [
        'AAAA"BBBB',
        "AAAA'BBBB",
    ])
    def test_fields_with_quote_characters_not_escaped(self, tsv_factory, quote_string):
        """Test that fields containing quote characters are not CSV-escaped.

        This is important for FASTQ quality strings which may contain the '"'
        character (ASCII 34 = Phred quality score 1). CSV escaping would double
        the quotes and wrap the field, corrupting the quality string length.
        """
        input_content = f"x\ty\tz\n0\t{quote_string}\tother\n1\tno_quotes\t{quote_string}\n"
        expected_output = f"x\ty\tz\tselected\n0\t{quote_string}\tother\t{quote_string}\n1\tno_quotes\t{quote_string}\t{quote_string}\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")
        add_conditional_tsv_column.add_conditional_column(
            input_file,
            chk_col="x",
            match_val="0",
            if_col="y",
            else_col="z",
            new_hdr="selected",
            out_path=output_file,
        )
        result = tsv_factory.read_plain(output_file)
        assert result == expected_output
        # Explicitly verify no CSV escaping occurred (doubled quotes or wrapping)
        assert '""' not in result, "Quotes should not be doubled (CSV escaping)"
        assert "''" not in result, "Quotes should not be doubled (CSV escaping)"

