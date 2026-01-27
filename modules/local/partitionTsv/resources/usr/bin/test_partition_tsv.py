#!/usr/bin/env python

# TODO: Add unit tests for individual functions in a future pass

import pytest
import os
import glob

import partition_tsv


class TestPartitionTsv:
    """Test the partition_tsv module."""

    def test_empty_file_raises_error(self, tsv_factory):
        """Test that empty file raises ValueError."""
        input_file = tsv_factory.create_plain("input.tsv", "")

        with pytest.raises(ValueError, match="Input file is empty"):
            partition_tsv.partition(input_file, "x")

    def test_missing_column_raises_error(self, tsv_factory):
        """Test that missing partition column raises ValueError."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)

        with pytest.raises(ValueError, match="Required column is missing from header line: test"):
            partition_tsv.partition(input_file, "test")

    def test_unsorted_file_raises_error(self, tsv_factory, tmp_path):
        """Test that unsorted file raises ValueError."""
        input_content = "x\tv\tw\n3\t4\t5\n6\t7\t8\n0\t1\t2\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)

        # Change to the directory containing the input file
        original_cwd = os.getcwd()
        input_dir = os.path.dirname(input_file)
        os.chdir(input_dir)

        try:
            with pytest.raises(ValueError, match="Input file is not sorted by partition column"):
                partition_tsv.partition(os.path.basename(input_file), "x")
        finally:
            os.chdir(original_cwd)

    def test_header_only_input(self, tsv_factory, tmp_path):
        """Test that header-only input produces no output files."""
        input_content = "x\ty\tz\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)

        # Change to the directory containing the input file
        original_cwd = os.getcwd()
        input_dir = os.path.dirname(input_file)
        os.chdir(input_dir)

        try:
            partition_tsv.partition(os.path.basename(input_file), "x")

            # Check that no partition files were created
            partition_files = glob.glob("partition_*_input.tsv")
            assert len(partition_files) == 0
        finally:
            os.chdir(original_cwd)

    def test_single_value_partition(self, tsv_factory, tmp_path):
        """Test partitioning file with single partition value across multiple rows."""
        input_content = "x\ty\tz\n3\t4\t5\n3\t5\t6\n3\t7\t8\n3\t9\t10\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)

        # Change to the directory containing the input file
        original_cwd = os.getcwd()
        input_dir = os.path.dirname(input_file)
        os.chdir(input_dir)

        try:
            partition_tsv.partition(os.path.basename(input_file), "x")

            # Check that exactly one partition file was created
            partition_files = sorted(glob.glob("partition_*_input.tsv"))
            assert len(partition_files) == 1
            assert partition_files[0] == "partition_3_input.tsv"

            # Check that output matches input
            with open(partition_files[0], "r") as f:
                content = f.read()
            assert content == input_content
        finally:
            os.chdir(original_cwd)

    def test_multi_value_partition(self, tsv_factory, tmp_path):
        """Test partitioning file with multiple partition values."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)

        # Change to the directory containing the input file
        original_cwd = os.getcwd()
        input_dir = os.path.dirname(input_file)
        os.chdir(input_dir)

        try:
            partition_tsv.partition(os.path.basename(input_file), "x")

            # Check that three partition files were created (for x=0, 3, 6)
            partition_files = sorted(glob.glob("partition_*_input.tsv"))
            assert len(partition_files) == 3

            expected_files = ["partition_0_input.tsv", "partition_3_input.tsv", "partition_6_input.tsv"]
            assert partition_files == expected_files

            # Check content of each partition
            with open("partition_0_input.tsv", "r") as f:
                content_0 = f.read()
            assert content_0 == "x\ty\tz\n0\t1\t2\n"

            with open("partition_3_input.tsv", "r") as f:
                content_3 = f.read()
            assert content_3 == "x\ty\tz\n3\t4\t5\n3\t5\t6\n"

            with open("partition_6_input.tsv", "r") as f:
                content_6 = f.read()
            assert content_6 == "x\ty\tz\n6\t7\t8\n"
        finally:
            os.chdir(original_cwd)
