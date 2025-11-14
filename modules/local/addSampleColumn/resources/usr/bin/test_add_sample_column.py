#!/usr/bin/env python

import pytest
import gzip

# Import the module to test
import add_sample_column


class TestAddSampleColumn:
    """Test the add_sample_column function."""

    def test_basic_functionality(self, tmp_path):
        """Test adding a sample column to a basic TSV file."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n")

        add_sample_column.add_sample_column(
            str(input_file),
            "sample_001",
            "sample",
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\tsample\nval1\tval2\tval3\tsample_001\nval4\tval5\tval6\tsample_001\n"
        assert result == expected

    def test_empty_file(self, tmp_path):
        """Test handling of completely empty file."""
        input_file = tmp_path / "empty.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("")

        add_sample_column.add_sample_column(
            str(input_file),
            "sample_001",
            "sample",
            str(output_file)
        )

        result = output_file.read_text()
        assert result == ""

    def test_header_only_file(self, tmp_path):
        """Test file with only header line and no data rows."""
        input_file = tmp_path / "header_only.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\n")

        add_sample_column.add_sample_column(
            str(input_file),
            "sample_001",
            "sample",
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\tsample\n"
        assert result == expected

    def test_column_already_exists(self, tmp_path):
        """Test that adding an existing column raises ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        with pytest.raises(ValueError, match="Sample name column already exists: col2"):
            add_sample_column.add_sample_column(
                str(input_file),
                "sample_001",
                "col2",  # Column that already exists
                str(output_file)
            )

    def test_empty_lines_skipped(self, tmp_path):
        """Test that empty lines in the file are skipped."""
        input_file = tmp_path / "with_empty_lines.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\nval1\tval2\n\nval3\tval4\n")

        add_sample_column.add_sample_column(
            str(input_file),
            "sample_001",
            "sample",
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\tsample\nval1\tval2\tsample_001\nval3\tval4\tsample_001\n"
        assert result == expected

    def test_gzip_input_output(self, tmp_path):
        """Test with gzipped input and output files."""
        input_file = tmp_path / "input.tsv.gz"
        output_file = tmp_path / "output.tsv.gz"

        with gzip.open(input_file, "wt") as f:
            f.write("col1\tcol2\nval1\tval2\n")

        add_sample_column.add_sample_column(
            str(input_file),
            "sample_001",
            "sample",
            str(output_file)
        )

        with gzip.open(output_file, "rt") as f:
            result = f.read()

        expected = "col1\tcol2\tsample\nval1\tval2\tsample_001\n"
        assert result == expected
