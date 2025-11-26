#!/usr/bin/env python

# TODO: Add unit tests for individual functions in a future pass

import pytest

import rehead_tsv


class TestReheadTsv:
    """Test the rehead_tsv module."""

    def test_missing_first_input_field_raises_error(self, tsv_factory):
        """Test that missing first/only input field raises ValueError."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Input field not found in file header: xyz"):
            rehead_tsv.rename_columns(input_file, ["xyz"], ["abc"], output_file)

    def test_missing_later_input_field_raises_error(self, tsv_factory):
        """Test that missing later input field raises ValueError."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Input field not found in file header: xyz"):
            rehead_tsv.rename_columns(input_file, ["x", "xyz"], ["a", "abc"], output_file)

    def test_more_input_fields_than_output_raises_error(self, tsv_factory):
        """Test that mismatched field list lengths raises ValueError."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Input and output field lists must be the same length"):
            rehead_tsv.rename_columns(input_file, ["x", "y"], ["a"], output_file)

    def test_more_output_fields_than_input_raises_error(self, tsv_factory):
        """Test that mismatched field list lengths raises ValueError."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Input and output field lists must be the same length"):
            rehead_tsv.rename_columns(input_file, ["x"], ["a", "b"], output_file)

    def test_no_change_when_headers_match_one_field(self, tsv_factory):
        """Test that output matches input when old and new headers are same (one field)."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        rehead_tsv.rename_columns(input_file, ["x"], ["x"], output_file)

        result = tsv_factory.read_plain(output_file)
        assert result == input_content

    def test_no_change_when_headers_match_multiple_fields(self, tsv_factory):
        """Test that output matches input when old and new headers are same (multiple fields)."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        rehead_tsv.rename_columns(input_file, ["x", "y"], ["x", "y"], output_file)

        result = tsv_factory.read_plain(output_file)
        assert result == input_content

    def test_successfully_change_header_one_field(self, tsv_factory):
        """Test successfully changing header with one field."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        rehead_tsv.rename_columns(input_file, ["x"], ["test"], output_file)

        result = tsv_factory.read_plain(output_file)
        lines = result.strip().split("\n")

        # Check header changed
        assert lines[0] == "test\ty\tz"

        # Check body unchanged
        assert lines[1] == "0\t1\t2"
        assert lines[2] == "3\t4\t5"
        assert lines[3] == "3\t5\t6"
        assert lines[4] == "6\t7\t8"

        # Verify dimensions
        assert len(lines) == 5

    def test_successfully_change_header_multiple_fields(self, tsv_factory):
        """Test successfully changing header with multiple fields."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        rehead_tsv.rename_columns(input_file, ["x", "y"], ["a", "b"], output_file)

        result = tsv_factory.read_plain(output_file)
        lines = result.strip().split("\n")

        # Check header changed
        assert lines[0] == "a\tb\tz"

        # Check body unchanged
        assert lines[1] == "0\t1\t2"
        assert lines[2] == "3\t4\t5"
        assert lines[3] == "3\t5\t6"
        assert lines[4] == "6\t7\t8"

        # Verify dimensions
        assert len(lines) == 5

    def test_empty_input_file(self, tsv_factory):
        """Test that empty input file produces empty output."""
        input_file = tsv_factory.create_plain("input.tsv", "")
        output_file = tsv_factory.get_path("output.tsv")

        rehead_tsv.rename_columns(input_file, ["x"], ["new_col"], output_file)

        result = tsv_factory.read_plain(output_file)
        assert result == ""
