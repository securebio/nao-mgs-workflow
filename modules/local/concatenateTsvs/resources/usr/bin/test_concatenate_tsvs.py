#!/usr/bin/env python

import pytest
import gzip

# Import the module to test
import concatenate_tsvs


class TestConcatenateTsvs:
    """Test the concatenate_tsvs function."""

    def test_basic_concatenation(self, tmp_path):
        """Test concatenating multiple TSV files with matching headers."""
        file1 = tmp_path / "file1.tsv"
        file2 = tmp_path / "file2.tsv"
        output_file = tmp_path / "output.tsv"

        file1.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")
        file2.write_text("col1\tcol2\tcol3\nval4\tval5\tval6\n")

        concatenate_tsvs.concatenate_tsvs(
            [str(file1), str(file2)],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n"
        assert result == expected

    def test_reordered_headers(self, tmp_path):
        """Test concatenating files with different column orders."""
        file1 = tmp_path / "file1.tsv"
        file2 = tmp_path / "file2.tsv"
        output_file = tmp_path / "output.tsv"

        file1.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")
        file2.write_text("col3\tcol1\tcol2\nval6\tval4\tval5\n")

        concatenate_tsvs.concatenate_tsvs(
            [str(file1), str(file2)],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n"
        assert result == expected

    def test_mismatched_headers(self, tmp_path):
        """Test that mismatched headers raise ValueError."""
        file1 = tmp_path / "file1.tsv"
        file2 = tmp_path / "file2.tsv"
        output_file = tmp_path / "output.tsv"

        file1.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")
        file2.write_text("col1\tcol2\tcol4\nval4\tval5\tval6\n")

        with pytest.raises(ValueError, match="Headers do not match"):
            concatenate_tsvs.concatenate_tsvs(
                [str(file1), str(file2)],
                str(output_file)
            )

    def test_all_empty_files(self, tmp_path):
        """Test concatenating all empty files produces empty output."""
        file1 = tmp_path / "empty1.tsv"
        file2 = tmp_path / "empty2.tsv"
        output_file = tmp_path / "output.tsv"

        file1.write_text("")
        file2.write_text("")

        concatenate_tsvs.concatenate_tsvs(
            [str(file1), str(file2)],
            str(output_file)
        )

        result = output_file.read_text()
        assert result == ""

    def test_some_empty_files(self, tmp_path):
        """Test skipping empty files during concatenation."""
        file1 = tmp_path / "empty.tsv"
        file2 = tmp_path / "file2.tsv"
        file3 = tmp_path / "file3.tsv"
        output_file = tmp_path / "output.tsv"

        file1.write_text("")
        file2.write_text("col1\tcol2\nval1\tval2\n")
        file3.write_text("col1\tcol2\nval3\tval4\n")

        concatenate_tsvs.concatenate_tsvs(
            [str(file1), str(file2), str(file3)],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\nval1\tval2\nval3\tval4\n"
        assert result == expected

    def test_single_file(self, tmp_path):
        """Test concatenating a single file."""
        file1 = tmp_path / "file1.tsv"
        output_file = tmp_path / "output.tsv"

        file1.write_text("col1\tcol2\nval1\tval2\n")

        concatenate_tsvs.concatenate_tsvs(
            [str(file1)],
            str(output_file)
        )

        result = output_file.read_text()
        expected = "col1\tcol2\nval1\tval2\n"
        assert result == expected

    def test_gzip_files(self, tmp_path):
        """Test concatenating gzipped files."""
        file1 = tmp_path / "file1.tsv.gz"
        file2 = tmp_path / "file2.tsv.gz"
        output_file = tmp_path / "output.tsv.gz"

        with gzip.open(file1, "wt") as f:
            f.write("col1\tcol2\nval1\tval2\n")
        with gzip.open(file2, "wt") as f:
            f.write("col1\tcol2\nval3\tval4\n")

        concatenate_tsvs.concatenate_tsvs(
            [str(file1), str(file2)],
            str(output_file)
        )

        with gzip.open(output_file, "rt") as f:
            result = f.read()

        expected = "col1\tcol2\nval1\tval2\nval3\tval4\n"
        assert result == expected
