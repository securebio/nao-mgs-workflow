#!/usr/bin/env python

import glob
import os

import pytest

import partition_tsv


class TestReadLine:
    """Test the read_line function."""

    @pytest.mark.parametrize(
        "content,expected",
        [
            ("col1\tcol2\tcol3\n", ["col1", "col2", "col3"]),
            ("", None),
        ],
        ids=["normal_line", "empty_file"],
    )
    def test_read_line(self, tmp_path, content, expected):
        """Test reading lines from files."""
        test_file = tmp_path / "test.tsv"
        test_file.write_text(content)

        with open(test_file) as f:
            result = partition_tsv.read_line(f)

        assert result == expected


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
        os.chdir(tmp_path)

        input_file = tmp_path / "input.tsv"
        input_file.write_text(
            "id\tname\tvalue\n1\talice\t10\n1\tbob\t20\n2\tcharlie\t30\n"
        )

        partition_tsv.partition("input.tsv", "id")

        partition_files = sorted(glob.glob("partition_*_input.tsv"))
        assert len(partition_files) == 2

        with open(partition_files[0]) as f:
            content1 = f.read()
        assert "id\tname\tvalue\n" in content1
        assert "1\talice\t10\n" in content1
        assert "1\tbob\t20\n" in content1

        with open(partition_files[1]) as f:
            content2 = f.read()
        assert "id\tname\tvalue\n" in content2
        assert "2\tcharlie\t30\n" in content2

    def test_partition_single_group(self, tmp_path):
        """Test partitioning with all rows in one group."""
        os.chdir(tmp_path)

        input_file = tmp_path / "input.tsv"
        input_file.write_text(
            "id\tname\tvalue\n1\talice\t10\n1\tbob\t20\n1\tcharlie\t30\n"
        )

        partition_tsv.partition("input.tsv", "id")

        partition_files = glob.glob("partition_*_input.tsv")
        assert len(partition_files) == 1

        with open(partition_files[0]) as f:
            lines = f.readlines()
        assert len(lines) == 4  # header + 3 data rows

    def test_partition_header_only(self, tmp_path):
        """Test partitioning file with only header."""
        os.chdir(tmp_path)

        input_file = tmp_path / "header_only.tsv"
        input_file.write_text("id\tname\tvalue\n")

        partition_tsv.partition("header_only.tsv", "id")

        partition_files = glob.glob("partition_*_header_only.tsv")
        assert len(partition_files) == 1
        assert "empty" in partition_files[0]

    @pytest.mark.parametrize(
        "content,column,expected_match",
        [
            ("", "id", "Input file is empty"),
            (
                "id\tname\tvalue\n1\talice\t10\n",
                "missing_col",
                "Required column is missing from header",
            ),
            (
                "id\tname\tvalue\n2\tcharlie\t30\n1\talice\t10\n",
                "id",
                "Input file is not sorted",
            ),
        ],
        ids=["empty_file", "missing_column", "unsorted_file"],
    )
    def test_partition_errors(self, tmp_path, content, column, expected_match):
        """Test various error conditions."""
        os.chdir(tmp_path)

        input_file = tmp_path / "input.tsv"
        input_file.write_text(content)

        with pytest.raises(ValueError, match=expected_match):
            partition_tsv.partition("input.tsv", column)
