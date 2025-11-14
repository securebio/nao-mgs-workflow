#!/usr/bin/env python

import pytest
import os
import glob

# Import the module to test
import partition_tsv


class TestReadLine:
    """Test the read_line function."""

    def test_read_normal_line(self, tmp_path):
        """Test reading a normal line."""
        test_file = tmp_path / "test.tsv"
        test_file.write_text("col1\tcol2\tcol3\n")

        with open(test_file) as f:
            result = partition_tsv.read_line(f)

        assert result == ["col1", "col2", "col3"]

    def test_read_empty_file(self, tmp_path):
        """Test reading from empty file returns None."""
        test_file = tmp_path / "empty.tsv"
        test_file.write_text("")

        with open(test_file) as f:
            result = partition_tsv.read_line(f)

        assert result is None


class TestWriteLine:
    """Test the write_line function."""

    def test_write_line(self, tmp_path):
        """Test writing a line."""
        test_file = tmp_path / "test.tsv"

        with open(test_file, "w") as f:
            partition_tsv.write_line(f, ["col1", "col2", "col3"])

        result = test_file.read_text()
        assert result == "col1\tcol2\tcol3\n"


class TestPartition:
    """Test the partition function."""

    def test_basic_partition(self, tmp_path):
        """Test basic partitioning by column."""
        os.chdir(tmp_path)  # Change to tmp directory for output files

        input_file = tmp_path / "input.tsv"
        input_file.write_text("id\tname\tvalue\n1\talice\t10\n1\tbob\t20\n2\tcharlie\t30\n")

        # Use just the filename, not the full path
        partition_tsv.partition("input.tsv", "id")

        # Check partition files were created
        partition_files = sorted(glob.glob("partition_*_input.tsv"))
        assert len(partition_files) == 2

        # Check first partition
        with open(partition_files[0]) as f:
            content1 = f.read()
        assert "id\tname\tvalue\n" in content1
        assert "1\talice\t10\n" in content1
        assert "1\tbob\t20\n" in content1

        # Check second partition
        with open(partition_files[1]) as f:
            content2 = f.read()
        assert "id\tname\tvalue\n" in content2
        assert "2\tcharlie\t30\n" in content2

    def test_partition_empty_file(self, tmp_path):
        """Test that empty file raises ValueError."""
        os.chdir(tmp_path)

        input_file = tmp_path / "empty.tsv"
        input_file.write_text("")

        with pytest.raises(ValueError, match="Input file is empty"):
            partition_tsv.partition("empty.tsv", "id")

    def test_partition_missing_column(self, tmp_path):
        """Test that missing column raises ValueError."""
        os.chdir(tmp_path)

        input_file = tmp_path / "input.tsv"
        input_file.write_text("id\tname\tvalue\n1\talice\t10\n")

        with pytest.raises(ValueError, match="Required column is missing from header"):
            partition_tsv.partition("input.tsv", "missing_col")

    def test_partition_header_only(self, tmp_path):
        """Test partitioning file with only header."""
        os.chdir(tmp_path)

        input_file = tmp_path / "header_only.tsv"
        input_file.write_text("id\tname\tvalue\n")

        partition_tsv.partition("header_only.tsv", "id")

        # Should create one empty partition file
        partition_files = glob.glob("partition_*_header_only.tsv")
        assert len(partition_files) == 1
        assert "empty" in partition_files[0]

    def test_partition_unsorted_file(self, tmp_path):
        """Test that unsorted file raises ValueError."""
        os.chdir(tmp_path)

        input_file = tmp_path / "unsorted.tsv"
        input_file.write_text("id\tname\tvalue\n2\tcharlie\t30\n1\talice\t10\n")

        with pytest.raises(ValueError, match="Input file is not sorted"):
            partition_tsv.partition("unsorted.tsv", "id")

    def test_partition_single_group(self, tmp_path):
        """Test partitioning with all rows in one group."""
        os.chdir(tmp_path)

        input_file = tmp_path / "input.tsv"
        input_file.write_text("id\tname\tvalue\n1\talice\t10\n1\tbob\t20\n1\tcharlie\t30\n")

        partition_tsv.partition("input.tsv", "id")

        # Should create one partition file
        partition_files = glob.glob("partition_*_input.tsv")
        assert len(partition_files) == 1

        with open(partition_files[0]) as f:
            lines = f.readlines()
        assert len(lines) == 4  # header + 3 data rows
