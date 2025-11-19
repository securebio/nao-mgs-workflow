#!/usr/bin/env python

# TODO: Add unit tests for individual functions in a future pass

import pytest

import sort_tsv


class TestSortTsv:
    """Test the sort_tsv module."""

    def test_missing_sort_field_raises_error(self, tsv_factory):
        """Test that missing sort field raises ValueError."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Could not find sort field in input header"):
            sort_tsv.sort_tsv_file(input_file, output_file, "a", memory_limit=1)

    def test_already_sorted_plaintext(self, tsv_factory):
        """Test that already-sorted file remains unchanged."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        sort_tsv.sort_tsv_file(input_file, output_file, "x", memory_limit=1)

        result = tsv_factory.read_plain(output_file)
        assert result == input_content

    def test_sort_unsorted_plaintext(self, tsv_factory):
        """Test sorting an unsorted plaintext file."""
        input_content = "x\tv\tw\n3\t4\t5\n6\t7\t8\n0\t1\t2\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        sort_tsv.sort_tsv_file(input_file, output_file, "x", memory_limit=1)

        result = tsv_factory.read_plain(output_file)
        lines = result.strip().split("\n")

        # Check header
        assert lines[0] == "x\tv\tw"

        # Check sorted data
        assert len(lines) == 4  # header + 3 data rows
        assert lines[1].startswith("0\t")
        assert lines[2].startswith("3\t")
        assert lines[3].startswith("6\t")

        # Verify dimensions match
        input_lines = input_content.strip().split("\n")
        assert len(lines) == len(input_lines)

    def test_sort_unsorted_gzipped(self, tsv_factory):
        """Test sorting an unsorted gzipped file."""
        input_content = "x\tv\tw\n3\t4\t5\n6\t7\t8\n0\t1\t2\n"
        input_file = tsv_factory.create_gzip("input.tsv.gz", input_content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        sort_tsv.sort_tsv_file(input_file, output_file, "x", memory_limit=1)

        result = tsv_factory.read_gzip(output_file)
        lines = result.strip().split("\n")

        # Check header
        assert lines[0] == "x\tv\tw"

        # Check sorted data
        assert len(lines) == 4
        assert lines[1].startswith("0\t")
        assert lines[2].startswith("3\t")
        assert lines[3].startswith("6\t")

    def test_empty_input_file(self, tsv_factory):
        """Test that empty input file produces empty output."""
        input_file = tsv_factory.create_plain("input.tsv", "")
        output_file = tsv_factory.get_path("output.tsv")

        sort_tsv.sort_tsv_file(input_file, output_file, "x", memory_limit=1)

        result = tsv_factory.read_plain(output_file)
        assert result == ""
