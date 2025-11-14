#!/usr/bin/env python

import pytest
import gzip

# Import the module to test
import select_tsv_columns


class TestGetHeaderIndex:
    """Test the get_header_index function."""

    def test_field_found(self):
        """Test finding a field in headers."""
        headers = ["col1", "col2", "col3"]
        index = select_tsv_columns.get_header_index(headers, "col2", "keep")
        assert index == 1

    def test_field_not_found_keep_mode(self):
        """Test missing field in keep mode raises ValueError."""
        headers = ["col1", "col2", "col3"]
        with pytest.raises(ValueError, match="Field not found in header"):
            select_tsv_columns.get_header_index(headers, "missing", "keep")

    def test_field_not_found_drop_mode(self):
        """Test missing field in drop mode returns None."""
        headers = ["col1", "col2", "col3"]
        index = select_tsv_columns.get_header_index(headers, "missing", "drop")
        assert index is None


class TestJoinLine:
    """Test the join_line function."""

    def test_join_basic(self):
        """Test joining strings with tabs."""
        result = select_tsv_columns.join_line(["a", "b", "c"])
        assert result == "a\tb\tc\n"

    def test_join_single(self):
        """Test joining a single string."""
        result = select_tsv_columns.join_line(["a"])
        assert result == "a\n"

    def test_join_empty(self):
        """Test joining empty list."""
        result = select_tsv_columns.join_line([])
        assert result == "\n"


class TestSubsetLine:
    """Test the subset_line function."""

    def test_subset_basic(self):
        """Test subsetting a list by indices."""
        inputs = ["a", "b", "c", "d"]
        result = select_tsv_columns.subset_line(inputs, [0, 2])
        assert result == ["a", "c"]

    def test_subset_single(self):
        """Test subsetting to a single element."""
        inputs = ["a", "b", "c"]
        result = select_tsv_columns.subset_line(inputs, [1])
        assert result == ["b"]

    def test_subset_reorder(self):
        """Test that subsetting can reorder elements."""
        inputs = ["a", "b", "c"]
        result = select_tsv_columns.subset_line(inputs, [2, 0, 1])
        assert result == ["c", "a", "b"]


class TestSelectColumns:
    """Test the select_columns function."""

    def test_keep_mode_basic(self, tmp_path):
        """Test keeping specified columns."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\tcol4\nval1\tval2\tval3\tval4\n")

        select_tsv_columns.select_columns(
            str(input_file),
            str(output_file),
            ["col1", "col3"],
            "keep"
        )

        result = output_file.read_text()
        expected = "col1\tcol3\nval1\tval3\n"
        assert result == expected

    def test_drop_mode_basic(self, tmp_path):
        """Test dropping specified columns."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\tcol4\nval1\tval2\tval3\tval4\n")

        select_tsv_columns.select_columns(
            str(input_file),
            str(output_file),
            ["col2", "col4"],
            "drop"
        )

        result = output_file.read_text()
        expected = "col1\tcol3\nval1\tval3\n"
        assert result == expected

    def test_keep_missing_field(self, tmp_path):
        """Test that missing field in keep mode raises ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        with pytest.raises(ValueError, match="Field not found in header"):
            select_tsv_columns.select_columns(
                str(input_file),
                str(output_file),
                ["col1", "missing_col"],
                "keep"
            )

    def test_drop_missing_field(self, tmp_path):
        """Test that missing field in drop mode is ignored with warning."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        # Should not raise error, just warn
        select_tsv_columns.select_columns(
            str(input_file),
            str(output_file),
            ["col2", "missing_col"],
            "drop"
        )

        result = output_file.read_text()
        expected = "col1\tcol3\nval1\tval3\n"
        assert result == expected

    def test_drop_all_fields(self, tmp_path):
        """Test that dropping all fields raises ValueError."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\nval1\tval2\n")

        with pytest.raises(ValueError, match="Dropping all fields"):
            select_tsv_columns.select_columns(
                str(input_file),
                str(output_file),
                ["col1", "col2"],
                "drop"
            )

    def test_empty_file(self, tmp_path):
        """Test handling of empty file."""
        input_file = tmp_path / "empty.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("")

        select_tsv_columns.select_columns(
            str(input_file),
            str(output_file),
            ["col1"],
            "keep"
        )

        result = output_file.read_text()
        assert result == ""
    
    def test_header_only_file(self, tmp_path):
        """Test handling of a file with only a header line."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\n")

        select_tsv_columns.select_columns(
            str(input_file),
            str(output_file),
            ["col1", "col3"],
            "keep"
        )

        result = output_file.read_text()
        expected = "col1\tcol3\n"
        assert result == expected

    def test_keep_single_column(self, tmp_path):
        """Test keeping a single column."""
        input_file = tmp_path / "input.tsv"
        output_file = tmp_path / "output.tsv"
        input_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        select_tsv_columns.select_columns(
            str(input_file),
            str(output_file),
            ["col2"],
            "keep"
        )

        result = output_file.read_text()
        expected = "col2\nval2\n"
        assert result == expected

    def test_gzip_files(self, tmp_path):
        """Test with gzipped input and output files."""
        input_file = tmp_path / "input.tsv.gz"
        output_file = tmp_path / "output.tsv.gz"

        with gzip.open(input_file, "wt") as f:
            f.write("col1\tcol2\tcol3\nval1\tval2\tval3\n")

        select_tsv_columns.select_columns(
            str(input_file),
            str(output_file),
            ["col1", "col3"],
            "keep"
        )

        with gzip.open(output_file, "rt") as f:
            result = f.read()

        expected = "col1\tcol3\nval1\tval3\n"
        assert result == expected
