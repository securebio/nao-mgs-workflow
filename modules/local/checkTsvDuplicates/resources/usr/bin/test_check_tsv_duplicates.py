#!/usr/bin/env python

import pytest
import gzip

# Import the module to test
import check_tsv_duplicates


class TestCheckDuplicates:
    """Test the check_duplicates function."""

    def test_no_duplicates(self, tmp_path):
        """Test file with no duplicates passes successfully."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("id\tname\tvalue\n1\talice\t10\n2\tbob\t20\n3\tcharlie\t30\n")

        check_tsv_duplicates.check_duplicates(
            str(input_file),
            str(output_file),
            "id"
        )

        result = output_file.read_text()
        expected = "id\tname\tvalue\n1\talice\t10\n2\tbob\t20\n3\tcharlie\t30\n"
        assert result == expected

    def test_duplicate_values(self, tmp_path):
        """Test that duplicate values raise ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("id\tname\tvalue\n1\talice\t10\n1\tbob\t20\n")

        with pytest.raises(ValueError, match="Duplicate value found"):
            check_tsv_duplicates.check_duplicates(
                str(input_file),
                str(output_file),
                "id"
            )

    def test_unsorted_file(self, tmp_path):
        """Test that unsorted file raises ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("id\tname\tvalue\n3\tcharlie\t30\n1\talice\t10\n")

        with pytest.raises(ValueError, match="File is not sorted"):
            check_tsv_duplicates.check_duplicates(
                str(input_file),
                str(output_file),
                "id"
            )

    def test_missing_field(self, tmp_path):
        """Test that missing field raises ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("id\tname\tvalue\n1\talice\t10\n")

        with pytest.raises(ValueError, match="Field not found in header"):
            check_tsv_duplicates.check_duplicates(
                str(input_file),
                str(output_file),
                "missing_field"
            )

    def test_empty_header(self, tmp_path):
        """Test that empty file raises ValueError."""
        input_file = tmp_path / "empty.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("")

        with pytest.raises(ValueError, match="No header to select fields from"):
            check_tsv_duplicates.check_duplicates(
                str(input_file),
                str(output_file),
                "id"
            )

    def test_gzip_input_output(self, tmp_path):
        """Test with gzipped input and output files."""
        input_file = tmp_path / "input.tsv.gz"
        output_file = tmp_path / "output.tsv.gz"

        with gzip.open(input_file, "wt") as f:
            f.write("id\tname\n1\talice\n2\tbob\n")

        check_tsv_duplicates.check_duplicates(
            str(input_file),
            str(output_file),
            "id"
        )

        with gzip.open(output_file, "rt") as f:
            result = f.read()

        expected = "id\tname\n1\talice\n2\tbob\n"
        assert result == expected
