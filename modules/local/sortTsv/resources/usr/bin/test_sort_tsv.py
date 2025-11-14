#!/usr/bin/env python

import pytest

import sort_tsv


class TestProcessHeader:
    """Test the process_header function."""

    @pytest.mark.parametrize(
        "header_line,sort_field,expected_index",
        [
            ("col1\tcol2\tcol3", "col1", 0),
            ("col1\tcol2\tcol3", "col2", 1),
            ("col1\tcol2\tcol3", "col3", 2),
            ("id\tname\tvalue\tdate", "value", 2),
        ],
        ids=["first_col", "middle_col", "last_col", "four_cols"],
    )
    def test_valid_headers(self, header_line, sort_field, expected_index):
        """Test finding sort field in header."""
        result = sort_tsv.process_header(header_line, sort_field)
        assert result == expected_index

    def test_empty_header(self):
        """Test empty header returns None."""
        result = sort_tsv.process_header("", "col1")
        assert result is None

    @pytest.mark.parametrize(
        "header_line,sort_field",
        [
            ("col1\tcol2\tcol3", "missing"),
            ("id\tname", "value"),
        ],
        ids=["not_present", "value"],
    )
    def test_missing_field(self, header_line, sort_field):
        """Test missing sort field raises ValueError."""
        with pytest.raises(ValueError, match="Could not find sort field"):
            sort_tsv.process_header(header_line, sort_field)


class TestSortTsvFile:
    """Test the sort_tsv_file function."""

    @pytest.mark.parametrize(
        "input_content,sort_field,expected_output",
        [
            # Basic sort on first column
            (
                "id\tname\n3\tcharlie\n1\talice\n2\tbob\n",
                "id",
                "id\tname\n1\talice\n2\tbob\n3\tcharlie\n",
            ),
            # Sort by non-first column
            (
                "id\tname\tvalue\n1\tcharlie\t30\n2\talice\t10\n3\tbob\t20\n",
                "name",
                "id\tname\tvalue\n2\talice\t10\n3\tbob\t20\n1\tcharlie\t30\n",
            ),
            # Already sorted file
            (
                "id\tname\n1\talice\n2\tbob\n3\tcharlie\n",
                "id",
                "id\tname\n1\talice\n2\tbob\n3\tcharlie\n",
            ),
            # Header-only file
            ("id\tname\tvalue\n", "id", "id\tname\tvalue\n"),
            # Single row file
            ("id\tname\n1\talice\n", "id", "id\tname\n1\talice\n"),
            # Lexicographic (string) sort: "1" < "10" < "2" (not numeric)
            (
                "id\tname\n10\talice\n2\tbob\n1\tcharlie\n",
                "id",
                "id\tname\n1\tcharlie\n10\talice\n2\tbob\n",
            ),
            # Empty file
            ("", "id", ""),
        ],
        ids=[
            "basic_sort",
            "non_first_column",
            "already_sorted",
            "header_only",
            "single_row",
            "string_sort",
            "empty_file",
        ],
    )
    def test_sort_variations(self, tsv_factory, input_content, sort_field, expected_output):
        """Test various sorting scenarios."""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        sort_tsv.sort_tsv_file(input_file, output_file, sort_field, memory_limit=1)

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    def test_missing_sort_field(self, tsv_factory):
        """Test that missing sort field raises ValueError."""
        input_content = "id\tname\n1\talice\n2\tbob\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Could not find sort field"):
            sort_tsv.sort_tsv_file(input_file, output_file, "missing_col", memory_limit=1)

    def test_gzipped_input_output(self, tsv_factory):
        """Test sorting with gzipped input and output."""
        input_content = "id\tname\n3\tcharlie\n1\talice\n2\tbob\n"
        expected_output = "id\tname\n1\talice\n2\tbob\n3\tcharlie\n"

        input_file = tsv_factory.create_gzip("input.tsv.gz", input_content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        sort_tsv.sort_tsv_file(input_file, output_file, "id", memory_limit=1)

        result = tsv_factory.read_gzip(output_file)
        assert result == expected_output
