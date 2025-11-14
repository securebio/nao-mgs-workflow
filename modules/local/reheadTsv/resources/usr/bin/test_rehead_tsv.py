#!/usr/bin/env python

import pytest
import gzip

# Import the module to test
import rehead_tsv


class TestRenameColumns:
    """Test the rename_columns function."""

    def test_basic_rename(self, tmp_path):
        """Test renaming columns in a TSV file."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        rehead_tsv.rename_columns(
            str(input_file),
            ["col1", "col3"],
            ["new1", "new3"],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "new1\tcol2\tnew3\nval1\tval2\tval3\n"
        assert result == expected

    def test_mismatched_list_lengths(self, tmp_path):
        """Test that mismatched input/output field lists raise ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        with pytest.raises(ValueError, match="Input and output field lists must be the same length"):
            rehead_tsv.rename_columns(
                str(input_file),
                ["col1", "col2"],
                ["new1"],  # Wrong length
                str(output_file)
            )

    def test_missing_input_field(self, tmp_path):
        """Test that missing input field raises ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        with pytest.raises(ValueError, match="Input field not found in file header"):
            rehead_tsv.rename_columns(
                str(input_file),
                ["col1", "missing"],
                ["new1", "new_missing"],
                str(output_file)
            )

    def test_empty_file(self, tmp_path):
        """Test handling of empty file."""
        input_file = tmp_path / "empty.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("")

        rehead_tsv.rename_columns(
            str(input_file),
            ["col1"],
            ["new1"],
            str(output_file)
        )

        result = output_file.read_text()
        assert result == ""

    def test_header_only_file(self, tmp_path):
        """Test file with only header line and no data rows."""
        input_file = tmp_path / "header_only.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\n")

        rehead_tsv.rename_columns(
            str(input_file),
            ["col1", "col3"],
            ["new1", "new3"],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "new1\tcol2\tnew3\n"
        assert result == expected

    def test_rename_single_column(self, tmp_path):
        """Test renaming a single column."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        rehead_tsv.rename_columns(
            str(input_file),
            ["col2"],
            ["renamed"],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\trenamed\tcol3\nval1\tval2\tval3\n"
        assert result == expected

    def test_rename_all_columns(self, tmp_path):
        """Test renaming all columns."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        rehead_tsv.rename_columns(
            str(input_file),
            ["col1", "col2", "col3"],
            ["new1", "new2", "new3"],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "new1\tnew2\tnew3\nval1\tval2\tval3\n"
        assert result == expected

    def test_no_change_rename(self, tmp_path):
        """Test renaming columns to the same name (no-op)."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        rehead_tsv.rename_columns(
            str(input_file),
            ["col1", "col2"],
            ["col1", "col2"],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        assert result == expected

    def test_gzip_files(self, tmp_path):
        """Test with gzipped input and output files."""
        input_file = tmp_path / "input.tsv.gz"
        output_file = tmp_path / "output.tsv.gz"

        with gzip.open(input_file, "wt") as f:
            f.write("col1\tcol2\nval1\tval2\n")

        rehead_tsv.rename_columns(
            str(input_file),
            ["col1"],
            ["renamed"],
            str(output_file)
        )

        with gzip.open(output_file, "rt") as f:
            result = f.read()

        expected = "renamed\tcol2\nval1\tval2\n"
        assert result == expected
