#!/usr/bin/env python

import pytest
import os
import logging

# Import the module to test
import join_tsvs


class TestWriteLine:
    """Test the write_line function."""

    def test_write_line(self, tmp_path):
        """Test writing a line."""
        test_file = tmp_path / "test.tsv"

        with open(test_file, "w") as f:
            join_tsvs.write_line(["col1", "col2", "col3"], f)

        result = test_file.read_text()
        assert result == "col1\tcol2\tcol3\n"


class TestFillRight:
    """Test the fill_right function."""

    def test_fill_right(self):
        """Test filling right side with placeholders."""
        row_1 = ["a", "b", "c"]
        placeholder_2 = ["NA", "NA"]

        result = join_tsvs.fill_right(row_1, placeholder_2)

        assert result == ["a", "b", "c", "NA", "NA"]


class TestFillLeft:
    """Test the fill_left function."""

    def test_fill_left_field_at_start(self):
        """Test filling left side when join field is at position 0."""
        placeholder_1 = ["NA", "NA"]
        row_2 = ["id1", "val1", "val2"]
        id_2 = "id1"
        field_index_1 = 0
        field_index_2 = 0

        result = join_tsvs.fill_left(placeholder_1, row_2, id_2, field_index_1, field_index_2)

        assert result == ["id1", "NA", "NA", "val1", "val2"]

    def test_fill_left_field_in_middle(self):
        """Test filling left side when join field is in middle."""
        placeholder_1 = ["NA", "NA"]
        row_2 = ["val1", "id1", "val2"]
        id_2 = "id1"
        field_index_1 = 1
        field_index_2 = 1

        result = join_tsvs.fill_left(placeholder_1, row_2, id_2, field_index_1, field_index_2)

        assert result == ["NA", "id1", "NA", "val1", "val2"]


class TestProcessHeaders:
    """Test the process_headers function."""

    def test_valid_headers(self):
        """Test processing valid headers."""
        header_1 = ["id", "col1", "col2"]
        header_2 = ["id", "col3", "col4"]
        field = "id"

        merged_header, field_index_1, field_index_2 = join_tsvs.process_headers(
            header_1, header_2, field
        )

        assert merged_header == ["id", "col1", "col2", "col3", "col4"]
        assert field_index_1 == 0
        assert field_index_2 == 0

    def test_missing_field_in_file1(self):
        """Test that missing field in file 1 raises ValueError."""
        header_1 = ["col1", "col2"]
        header_2 = ["id", "col3"]
        field = "id"

        with pytest.raises(ValueError, match="Join field missing from file 1"):
            join_tsvs.process_headers(header_1, header_2, field)

    def test_missing_field_in_file2(self):
        """Test that missing field in file 2 raises ValueError."""
        header_1 = ["id", "col1"]
        header_2 = ["col2", "col3"]
        field = "id"

        with pytest.raises(ValueError, match="Join field missing from file 2"):
            join_tsvs.process_headers(header_1, header_2, field)

    def test_duplicate_field_names(self):
        """Test that duplicate non-join field names raise ValueError."""
        header_1 = ["id", "col1", "col2"]
        header_2 = ["id", "col1", "col3"]
        field = "id"

        with pytest.raises(ValueError, match="Duplicate non-join field name found"):
            join_tsvs.process_headers(header_1, header_2, field)


class TestCheckSorting:
    """Test the check_sorting function."""

    def test_sorted_correctly(self):
        """Test that no error is raised when file is sorted."""
        join_tsvs.check_sorting("1", "2", "1", "test.tsv")
        # Should not raise error

    def test_unsorted_file(self):
        """Test that unsorted file raises ValueError."""
        with pytest.raises(ValueError, match="File 1 is not sorted"):
            join_tsvs.check_sorting("2", "1", "1", "test.tsv")

    def test_end_of_file(self):
        """Test that None as next ID doesn't raise error."""
        join_tsvs.check_sorting("5", None, "1", "test.tsv")
        # Should not raise error


class TestJoinTsvs:
    """Test the join_tsvs function."""

    def test_basic_inner_join(self, tmp_path):
        """Test basic inner join."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n3\tc\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "inner", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        assert len(lines) == 3  # header + 2 data rows
        assert lines[0] == "id\tcol1\tcol2"
        assert lines[1] == "1\ta\tx"
        assert lines[2] == "2\tb\ty"

    def test_left_join_with_missing_right(self, tmp_path):
        """Test left join with IDs missing in right file."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n3\tc\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "left", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        assert len(lines) == 4  # header + 3 data rows
        assert lines[0] == "id\tcol1\tcol2"
        assert lines[1] == "1\ta\tx"
        assert lines[2] == "2\tb\ty"
        assert lines[3] == "3\tc\tNA"

    def test_right_join_with_missing_left(self, tmp_path):
        """Test right join with IDs missing in left file."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n3\tz\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "right", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        assert len(lines) == 4  # header + 3 data rows
        assert lines[0] == "id\tcol1\tcol2"
        assert lines[1] == "1\ta\tx"
        assert lines[2] == "2\tb\ty"
        assert lines[3] == "3\tNA\tz"

    def test_outer_join(self, tmp_path):
        """Test outer join with IDs in both files."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n4\td\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n3\tz\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "outer", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        assert len(lines) == 5  # header + 4 data rows
        assert lines[0] == "id\tcol1\tcol2"
        assert lines[1] == "1\ta\tx"
        assert lines[2] == "2\tb\ty"
        assert lines[3] == "3\tNA\tz"
        assert lines[4] == "4\td\tNA"

    def test_strict_join_success(self, tmp_path):
        """Test strict join with matching IDs."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "strict", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        assert len(lines) == 3  # header + 2 data rows

    def test_strict_join_missing_in_file2(self, tmp_path):
        """Test strict join fails when ID missing in file 2."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n3\tc\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n")

        output = tmp_path / "output.tsv"

        with pytest.raises(ValueError, match="Strict join failed: ID 3 missing from file 2"):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "strict", "output.tsv")

    def test_strict_join_missing_in_file1(self, tmp_path):
        """Test strict join fails when ID missing in file 1."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n3\tz\n")

        output = tmp_path / "output.tsv"

        with pytest.raises(ValueError, match="Strict join failed: ID 3 missing from file 1"):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "strict", "output.tsv")

    def test_unsorted_file1(self, tmp_path):
        """Test that unsorted file 1 raises ValueError."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n2\tb\n1\ta\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n")

        output = tmp_path / "output.tsv"

        with pytest.raises(ValueError, match="File 1 is not sorted"):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "inner", "output.tsv")

    def test_unsorted_file2(self, tmp_path):
        """Test that unsorted file 2 raises ValueError."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n2\ty\n1\tx\n")

        output = tmp_path / "output.tsv"

        with pytest.raises(ValueError, match="File 2 is not sorted"):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "inner", "output.tsv")

    def test_both_files_empty(self, tmp_path):
        """Test join with both files empty."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "inner", "output.tsv")

        result = output.read_text()
        assert result == ""

    def test_empty_file1_inner_join(self, tmp_path):
        """Test inner join with empty file 1."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "inner", "output.tsv")

        result = output.read_text()
        assert result == ""

    def test_empty_file2_inner_join(self, tmp_path):
        """Test inner join with empty file 2."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "inner", "output.tsv")

        result = output.read_text()
        assert result == ""

    def test_empty_file1_left_join(self, tmp_path):
        """Test left join with empty file 1."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "left", "output.tsv")

        result = output.read_text()
        # Left join with empty left side should produce empty output
        assert result == ""

    def test_empty_file2_left_join(self, tmp_path):
        """Test left join with empty file 2."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "left", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        assert len(lines) == 2  # header + 1 data row
        assert lines[0] == "id\tcol1"

    def test_empty_file1_right_join(self, tmp_path):
        """Test right join with empty file 1."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n2\ty\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "right", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        # For right join with empty left side, output should match file 2
        assert len(lines) == 3  # header + 2 data rows
        assert lines[0] == "id\tcol2"

    def test_empty_file2_right_join(self, tmp_path):
        """Test right join with empty file 2."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "right", "output.tsv")

        result = output.read_text()
        # Right join with empty right side should produce empty output
        assert result == ""

    def test_empty_file1_outer_join(self, tmp_path):
        """Test outer join with empty file 1."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "outer", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        # Outer join with empty left side should match file 2
        assert len(lines) == 2  # header + 1 data row
        assert lines[0] == "id\tcol2"

    def test_empty_file2_outer_join(self, tmp_path):
        """Test outer join with empty file 2."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "outer", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        # Outer join with empty right side should match file 1
        assert len(lines) == 2  # header + 1 data row
        assert lines[0] == "id\tcol1"

    def test_empty_file2_strict_join(self, tmp_path):
        """Test strict join with empty file 2 raises error."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("")

        output = tmp_path / "output.tsv"

        with pytest.raises(ValueError, match="Strict join cannot be performed with empty file"):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "strict", "output.tsv")

    def test_empty_file1_strict_join(self, tmp_path):
        """Test strict join with empty file 1 raises error."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n")

        output = tmp_path / "output.tsv"

        with pytest.raises(ValueError, match="Strict join cannot be performed with empty file"):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "strict", "output.tsv")

    def test_many_to_many_join(self, tmp_path):
        """Test that many-to-many join raises error."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n1\tb\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n1\ty\n")

        output = tmp_path / "output.tsv"

        with pytest.raises(ValueError, match="Unsupported many-to-many join detected"):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "inner", "output.tsv")

    def test_one_to_many_join(self, tmp_path):
        """Test one-to-many join (supported)."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text("id\tcol1\n1\ta\n2\tb\n")

        file2 = tmp_path / "file2.tsv"
        file2.write_text("id\tcol2\n1\tx\n1\ty\n2\tz\n")

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "inner", "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        assert len(lines) == 4  # header + 3 data rows
        assert lines[0] == "id\tcol1\tcol2"
        assert lines[1] == "1\ta\tx"
        assert lines[2] == "1\ta\ty"
        assert lines[3] == "2\tb\tz"
