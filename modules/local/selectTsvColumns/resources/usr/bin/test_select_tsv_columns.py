#!/usr/bin/env python

import pytest

import select_tsv_columns


class TestGetHeaderIndex:
    """Test the get_header_index function."""

    def test_field_found(self):
        """Test finding a field in headers."""
        headers = ["col1", "col2", "col3"]
        index = select_tsv_columns.get_header_index(headers, "col2", "keep")
        assert index == 1

    @pytest.mark.parametrize(
        "mode,expected_error",
        [("keep", ValueError), ("drop", type(None))],
        ids=["keep_mode_raises_error", "drop_mode_returns_none"],
    )
    def test_field_not_found(self, mode, expected_error):
        """Test missing field behavior in different modes."""
        headers = ["col1", "col2", "col3"]
        if expected_error is ValueError:
            with pytest.raises(ValueError, match="Field not found in header"):
                select_tsv_columns.get_header_index(headers, "missing", mode)
        else:
            index = select_tsv_columns.get_header_index(headers, "missing", mode)
            assert index is None


class TestJoinLine:
    """Test the join_line function."""

    @pytest.mark.parametrize(
        "inputs,expected",
        [
            (["a", "b", "c"], "a\tb\tc\n"),
            (["a"], "a\n"),
            ([], "\n"),
        ],
        ids=["basic", "single", "empty"],
    )
    def test_join_line(self, inputs, expected):
        """Test joining strings with tabs."""
        result = select_tsv_columns.join_line(inputs)
        assert result == expected


class TestSubsetLine:
    """Test the subset_line function."""

    @pytest.mark.parametrize(
        "inputs,indices,expected",
        [
            (["a", "b", "c", "d"], [0, 2], ["a", "c"]),
            (["a", "b", "c"], [1], ["b"]),
            (["a", "b", "c"], [2, 0, 1], ["c", "a", "b"]),
        ],
        ids=["basic", "single", "reorder"],
    )
    def test_subset_line(self, inputs, indices, expected):
        """Test subsetting a list by indices."""
        result = select_tsv_columns.subset_line(inputs, indices)
        assert result == expected


class TestSelectColumns:
    """Test the select_columns function."""

    @pytest.mark.parametrize(
        "mode,columns,expected_output",
        [
            ("keep", ["col1", "col3"], "col1\tcol3\nval1\tval3\n"),
            ("drop", ["col2", "col4"], "col1\tcol3\nval1\tval3\n"),
            ("keep", ["col2"], "col2\nval2\n"),
        ],
        ids=["keep_mode_basic", "drop_mode_basic", "keep_single_column"],
    )
    def test_select_columns_success_cases(
        self, tsv_factory, mode, columns, expected_output
    ):
        """Test successful column selection scenarios."""
        input_file = tsv_factory.create_plain(
            "input.tsv", "col1\tcol2\tcol3\tcol4\nval1\tval2\tval3\tval4\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        select_tsv_columns.select_columns(input_file, output_file, columns, mode)

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    @pytest.mark.parametrize(
        "input_content,columns,mode,expected_output",
        [
            ("", ["col1"], "keep", ""),
            ("col1\tcol2\tcol3\n", ["col1", "col3"], "keep", "col1\tcol3\n"),
        ],
        ids=["empty_file", "header_only_file"],
    )
    def test_select_columns_edge_cases(
        self, tsv_factory, input_content, columns, mode, expected_output
    ):
        """Test edge cases for column selection."""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        select_tsv_columns.select_columns(input_file, output_file, columns, mode)

        result = tsv_factory.read_plain(output_file)
        assert result == expected_output

    @pytest.mark.parametrize(
        "file_format",
        ["plain", "gzip"],
        ids=["plain_tsv", "gzipped_tsv"],
    )
    def test_select_columns_file_formats(self, tsv_factory, file_format):
        """Test column selection with different file formats."""
        input_content = "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        expected = "col1\tcol3\nval1\tval3\n"

        if file_format == "plain":
            input_file = tsv_factory.create_plain("input.tsv", input_content)
            output_file = tsv_factory.get_path("output.tsv")
        else:
            input_file = tsv_factory.create_gzip("input.tsv.gz", input_content)
            output_file = tsv_factory.get_path("output.tsv.gz")

        select_tsv_columns.select_columns(
            input_file, output_file, ["col1", "col3"], "keep"
        )

        if file_format == "plain":
            result = tsv_factory.read_plain(output_file)
        else:
            result = tsv_factory.read_gzip(output_file)

        assert result == expected

    def test_keep_missing_field(self, tsv_factory):
        """Test that missing field in keep mode raises ValueError."""
        input_file = tsv_factory.create_plain(
            "input.tsv", "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Field not found in header"):
            select_tsv_columns.select_columns(
                input_file, output_file, ["col1", "missing_col"], "keep"
            )

    def test_drop_missing_field(self, tsv_factory):
        """Test that missing field in drop mode is ignored with warning."""
        input_file = tsv_factory.create_plain(
            "input.tsv", "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        # Should not raise error, just warn
        select_tsv_columns.select_columns(
            input_file, output_file, ["col2", "missing_col"], "drop"
        )

        result = tsv_factory.read_plain(output_file)
        expected = "col1\tcol3\nval1\tval3\n"
        assert result == expected

    def test_drop_all_fields(self, tsv_factory):
        """Test that dropping all fields raises ValueError."""
        input_file = tsv_factory.create_plain("input.tsv", "col1\tcol2\nval1\tval2\n")
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match="Dropping all fields"):
            select_tsv_columns.select_columns(
                input_file, output_file, ["col1", "col2"], "drop"
            )
