#!/usr/bin/env python

import pytest
import gzip

# Import the module to test
import head_tsv


class TestAddHeaderLine:
    """Test the add_header_line function."""

    def test_basic_functionality(self, tmp_path):
        """Test adding a header to a basic TSV file."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("val1\tval2\tval3\nval4\tval5\tval6\n")

        head_tsv.add_header_line(
            str(input_file),
            ["col1", "col2", "col3"],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n"
        assert result == expected

    def test_empty_file(self, tmp_path):
        """Test handling of completely empty file."""
        input_file = tmp_path / "empty.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("")

        head_tsv.add_header_line(
            str(input_file),
            ["col1", "col2", "col3"],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\n"
        assert result == expected

    def test_mismatched_field_count(self, tmp_path):
        """Test that mismatched field counts raise ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("val1\tval2\tval3\n")

        with pytest.raises(ValueError, match="Number of header fields does not match"):
            head_tsv.add_header_line(
                str(input_file),
                ["col1", "col2"],  # Wrong number of columns
                str(output_file)
            )

    def test_single_column(self, tmp_path):
        """Test file with single column."""
        input_file = tmp_path / "single_col.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("val1\nval2\nval3\n")

        head_tsv.add_header_line(
            str(input_file),
            ["col1"],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\nval1\nval2\nval3\n"
        assert result == expected

    def test_gzip_input_output(self, tmp_path):
        """Test with gzipped input and output files."""
        input_file = tmp_path / "input.tsv.gz"
        output_file = tmp_path / "output.tsv.gz"

        with gzip.open(input_file, "wt") as f:
            f.write("val1\tval2\n")

        head_tsv.add_header_line(
            str(input_file),
            ["col1", "col2"],
            str(output_file)
        )

        with gzip.open(output_file, "rt") as f:
            result = f.read()

        expected = "col1\tcol2\nval1\tval2\n"
        assert result == expected
