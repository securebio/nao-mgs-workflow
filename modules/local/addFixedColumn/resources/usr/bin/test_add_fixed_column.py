#!/usr/bin/env python

import pytest
import tempfile
import os
from pathlib import Path
import gzip
import bz2

# Import the module to test
import add_fixed_column

class TestAddColumn:
    """Test the add_column function."""

    def test_basic_functionality(self, tmp_path):
        """Test adding a column to a basic TSV file."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n")
        add_fixed_column.add_column(
            str(input_file),
            "new_col",
            "new_value",
            str(output_file)
        )
        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\tnew_col\nval1\tval2\tval3\tnew_value\nval4\tval5\tval6\tnew_value\n"
        assert result == expected

    def test_empty_file(self, tmp_path):
        """Test handling of completely empty file."""
        input_file = tmp_path / "empty.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("")
        add_fixed_column.add_column(
            str(input_file),
            "new_col",
            "new_value",
            str(output_file)
        )
        result = output_file.read_text()
        assert result == ""

    def test_header_only_file(self, tmp_path):
        """Test file with only header line and no data rows."""
        input_file = tmp_path / "header_only.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\n")
        add_fixed_column.add_column(
            str(input_file),
            "new_col",
            "new_value",
            str(output_file)
        )
        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\tnew_col\n"
        assert result == expected

    def test_column_already_exists(self, tmp_path):
        """Test that adding an existing column raises ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")
        with pytest.raises(ValueError, match="Column already exists: col2"):
            add_fixed_column.add_column(
                str(input_file),
                "col2",  # Column that already exists
                "new_value",
                str(output_file)
            )

    def test_whitespace_only_file(self, tmp_path):
        """Test file with only whitespace (newlines, spaces)."""
        input_file = tmp_path / "whitespace.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("\n\n\n")
        add_fixed_column.add_column(
            str(input_file),
            "new_col",
            "new_value",
            str(output_file)
        )
        result = output_file.read_text()
        # Empty lines should be skipped after header processing
        assert result == ""
