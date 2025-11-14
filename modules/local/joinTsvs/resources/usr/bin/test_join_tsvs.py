#!/usr/bin/env python

import os

import pytest

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

    @pytest.mark.parametrize(
        "field_index_1,field_index_2,row_2,id_2,expected",
        [
            (0, 0, ["id1", "val1", "val2"], "id1", ["id1", "NA", "NA", "val1", "val2"]),
            (1, 1, ["val1", "id1", "val2"], "id1", ["NA", "id1", "NA", "val1", "val2"]),
        ],
        ids=["field_at_start", "field_in_middle"],
    )
    def test_fill_left(self, field_index_1, field_index_2, row_2, id_2, expected):
        """Test filling left side with different field positions."""
        placeholder_1 = ["NA", "NA"]

        result = join_tsvs.fill_left(
            placeholder_1, row_2, id_2, field_index_1, field_index_2
        )

        assert result == expected


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

    @pytest.mark.parametrize(
        "header_1,header_2,field,expected_match",
        [
            (["col1", "col2"], ["id", "col3"], "id", "Join field missing from file 1"),
            (["id", "col1"], ["col2", "col3"], "id", "Join field missing from file 2"),
            (
                ["id", "col1", "col2"],
                ["id", "col1", "col3"],
                "id",
                "Duplicate non-join field name found",
            ),
        ],
        ids=["missing_in_file1", "missing_in_file2", "duplicate_field_names"],
    )
    def test_process_headers_errors(
        self, header_1, header_2, field, expected_match
    ):
        """Test error conditions in header processing."""
        with pytest.raises(ValueError, match=expected_match):
            join_tsvs.process_headers(header_1, header_2, field)


class TestCheckSorting:
    """Test the check_sorting function."""

    @pytest.mark.parametrize(
        "id_1,id_next_1,id_2",
        [
            ("1", "2", "1"),
            ("5", None, "1"),
        ],
        ids=["sorted_correctly", "end_of_file"],
    )
    def test_sorting_ok(self, id_1, id_next_1, id_2):
        """Test that no error is raised when file is sorted."""
        join_tsvs.check_sorting(id_1, id_next_1, id_2, "test.tsv")

    def test_unsorted_file(self):
        """Test that unsorted file raises ValueError."""
        with pytest.raises(ValueError, match="File 1 is not sorted"):
            join_tsvs.check_sorting("2", "1", "1", "test.tsv")


class TestJoinTsvs:
    """Test the join_tsvs function."""

    @pytest.mark.parametrize(
        "file1_content,file2_content,join_type,expected_lines",
        [
            (
                "id\tcol1\n1\ta\n2\tb\n3\tc\n",
                "id\tcol2\n1\tx\n2\ty\n",
                "inner",
                ["id\tcol1\tcol2", "1\ta\tx", "2\tb\ty"],
            ),
            (
                "id\tcol1\n1\ta\n2\tb\n3\tc\n",
                "id\tcol2\n1\tx\n2\ty\n",
                "left",
                ["id\tcol1\tcol2", "1\ta\tx", "2\tb\ty", "3\tc\tNA"],
            ),
            (
                "id\tcol1\n1\ta\n2\tb\n",
                "id\tcol2\n1\tx\n2\ty\n3\tz\n",
                "right",
                ["id\tcol1\tcol2", "1\ta\tx", "2\tb\ty", "3\tNA\tz"],
            ),
            (
                "id\tcol1\n1\ta\n2\tb\n4\td\n",
                "id\tcol2\n1\tx\n2\ty\n3\tz\n",
                "outer",
                ["id\tcol1\tcol2", "1\ta\tx", "2\tb\ty", "3\tNA\tz", "4\td\tNA"],
            ),
            (
                "id\tcol1\n1\ta\n2\tb\n",
                "id\tcol2\n1\tx\n2\ty\n",
                "strict",
                ["id\tcol1\tcol2", "1\ta\tx", "2\tb\ty"],
            ),
        ],
        ids=["inner_join", "left_join", "right_join", "outer_join", "strict_join"],
    )
    def test_join_types(
        self, tmp_path, file1_content, file2_content, join_type, expected_lines
    ):
        """Test different join types."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text(file1_content)

        file2 = tmp_path / "file2.tsv"
        file2.write_text(file2_content)

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", join_type, "output.tsv")

        result = output.read_text()
        lines = result.strip().split("\n")
        assert lines == expected_lines

    @pytest.mark.parametrize(
        "file1_content,file2_content,expected_match",
        [
            (
                "id\tcol1\n1\ta\n2\tb\n3\tc\n",
                "id\tcol2\n1\tx\n2\ty\n",
                "Strict join failed: ID 3 missing from file 2",
            ),
            (
                "id\tcol1\n1\ta\n2\tb\n",
                "id\tcol2\n1\tx\n2\ty\n3\tz\n",
                "Strict join failed: ID 3 missing from file 1",
            ),
            (
                "id\tcol1\n2\tb\n1\ta\n",
                "id\tcol2\n1\tx\n2\ty\n",
                "File 1 is not sorted",
            ),
            (
                "id\tcol1\n1\ta\n2\tb\n",
                "id\tcol2\n2\ty\n1\tx\n",
                "File 2 is not sorted",
            ),
            (
                "id\tcol1\n1\ta\n1\tb\n",
                "id\tcol2\n1\tx\n1\ty\n",
                "Unsupported many-to-many join detected",
            ),
        ],
        ids=[
            "strict_missing_file2",
            "strict_missing_file1",
            "unsorted_file1",
            "unsorted_file2",
            "many_to_many_join",
        ],
    )
    def test_join_errors(self, tmp_path, file1_content, file2_content, expected_match):
        """Test error conditions for joins."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text(file1_content)

        file2 = tmp_path / "file2.tsv"
        file2.write_text(file2_content)

        output = tmp_path / "output.tsv"

        with pytest.raises(ValueError, match=expected_match):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "strict", "output.tsv")

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

    @pytest.mark.parametrize(
        "file1_content,file2_content,join_type,expected_empty",
        [
            ("", "", "inner", True),
            ("", "id\tcol2\n1\tx\n", "inner", True),
            ("id\tcol1\n1\ta\n", "", "inner", True),
            ("", "id\tcol2\n1\tx\n", "left", True),
            ("id\tcol1\n1\ta\n", "", "left", False),
            ("", "id\tcol2\n1\tx\n2\ty\n", "right", False),
            ("id\tcol1\n1\ta\n", "", "right", True),
            ("", "id\tcol2\n1\tx\n", "outer", False),
            ("id\tcol1\n1\ta\n", "", "outer", False),
        ],
        ids=[
            "both_empty_inner",
            "empty_file1_inner",
            "empty_file2_inner",
            "empty_file1_left",
            "empty_file2_left",
            "empty_file1_right",
            "empty_file2_right",
            "empty_file1_outer",
            "empty_file2_outer",
        ],
    )
    def test_empty_file_joins(
        self, tmp_path, file1_content, file2_content, join_type, expected_empty
    ):
        """Test joins with empty files."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text(file1_content)

        file2 = tmp_path / "file2.tsv"
        file2.write_text(file2_content)

        output = tmp_path / "output.tsv"

        join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", join_type, "output.tsv")

        result = output.read_text()
        if expected_empty:
            assert result == ""
        else:
            assert result != ""

    @pytest.mark.parametrize(
        "file1_content,file2_content",
        [
            ("", "id\tcol2\n1\tx\n"),
            ("id\tcol1\n1\ta\n", ""),
        ],
        ids=["empty_file1", "empty_file2"],
    )
    def test_empty_file_strict_join_errors(
        self, tmp_path, file1_content, file2_content
    ):
        """Test strict join with empty files raises error."""
        os.chdir(tmp_path)

        file1 = tmp_path / "file1.tsv"
        file1.write_text(file1_content)

        file2 = tmp_path / "file2.tsv"
        file2.write_text(file2_content)

        output = tmp_path / "output.tsv"

        with pytest.raises(
            ValueError, match="Strict join cannot be performed with empty file"
        ):
            join_tsvs.join_tsvs("file1.tsv", "file2.tsv", "id", "strict", "output.tsv")
