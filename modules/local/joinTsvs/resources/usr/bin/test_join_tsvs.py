#!/usr/bin/env python

import pytest
import gzip

import join_tsvs


class TestJoinTsvs:
    """Test the join_tsvs module."""

    def test_missing_join_field_in_file1(self, tsv_factory, tmp_path):
        """Test that missing join field in file 1 raises ValueError."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = "x\tv\tw\n0\t1\t2\n3\t4\t5\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv")

        with pytest.raises(ValueError, match="Join field missing from file 1"):
            join_tsvs.join_tsvs(tsv1, tsv2, "v", "inner", output)

    def test_missing_join_field_in_file2(self, tsv_factory, tmp_path):
        """Test that missing join field in file 2 raises ValueError."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = "x\tv\tw\n0\t1\t2\n3\t4\t5\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv")

        with pytest.raises(ValueError, match="Join field missing from file 2"):
            join_tsvs.join_tsvs(tsv1, tsv2, "y", "inner", output)

    def test_unsorted_file1_raises_error(self, tsv_factory, tmp_path):
        """Test that unsorted file 1 raises ValueError."""
        tsv1_content = "x\tv\tw\n3\t4\t5\n6\t7\t8\n0\t1\t2\n"
        tsv2_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n6\t7\t8\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv")

        with pytest.raises(ValueError, match="File 1 is not sorted"):
            join_tsvs.join_tsvs(tsv1, tsv2, "x", "inner", output)

    def test_unsorted_file2_raises_error(self, tsv_factory, tmp_path):
        """Test that unsorted file 2 raises ValueError."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n6\t7\t8\n"
        tsv2_content = "x\tv\tw\n3\t4\t5\n6\t7\t8\n0\t1\t2\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv")

        with pytest.raises(ValueError, match="File 2 is not sorted"):
            join_tsvs.join_tsvs(tsv1, tsv2, "x", "inner", output)

    def test_duplicate_column_names_raises_error(self, tsv_factory, tmp_path):
        """Test that duplicate column names across files raises ValueError."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = "x\ty\tw\n0\t1\t2\n3\t4\t5\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv")

        with pytest.raises(ValueError, match="Duplicate non-join field name found across both files"):
            join_tsvs.join_tsvs(tsv1, tsv2, "x", "inner", output)

    def test_many_to_many_join_raises_error(self, tsv_factory, tmp_path):
        """Test that many-to-many join raises ValueError."""
        # Both files have duplicate values in join column
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n"
        tsv2_content = "x\tv\tw\n0\t1\t2\n3\t4\t5\n3\t6\t7\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv")

        with pytest.raises(ValueError, match="Unsupported many-to-many join detected"):
            join_tsvs.join_tsvs(tsv1, tsv2, "x", "inner", output)

    def test_strict_join_with_missing_values_raises_error(self, tsv_factory, tmp_path):
        """Test that strict join with missing values raises ValueError."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = "x\tv\tw\n0\t1\t2\n6\t4\t5\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv")

        with pytest.raises(ValueError, match="Strict join failed"):
            join_tsvs.join_tsvs(tsv1, tsv2, "x", "strict", output)

    def test_inner_join_success(self, tsv_factory, tmp_path):
        """Test successful inner join."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n6\t7\t8\n"
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "inner", output)

        # Read output
        with gzip.open(output, "rt") as f:
            result = f.read()

        expected = "x\ty\tz\tv\tw\n0\t1\t2\t10\t20\n3\t4\t5\t40\t50\n"
        assert result == expected

    def test_left_join_success(self, tsv_factory, tmp_path):
        """Test successful left join."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n6\t7\t8\n"
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "left", output)

        # Read output
        with gzip.open(output, "rt") as f:
            result = f.read()

        expected = "x\ty\tz\tv\tw\n0\t1\t2\t10\t20\n3\t4\t5\t40\t50\n6\t7\t8\tNA\tNA\n"
        assert result == expected

    def test_right_join_success(self, tsv_factory, tmp_path):
        """Test successful right join."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n6\t70\t80\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "right", output)

        # Read output
        with gzip.open(output, "rt") as f:
            result = f.read()

        expected = "x\ty\tz\tv\tw\n0\t1\t2\t10\t20\n3\t4\t5\t40\t50\n6\tNA\tNA\t70\t80\n"
        assert result == expected

    def test_outer_join_success(self, tsv_factory, tmp_path):
        """Test successful outer join."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n9\t10\t11\n"
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n6\t70\t80\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "outer", output)

        # Read output
        with gzip.open(output, "rt") as f:
            result = f.read()

        expected = "x\ty\tz\tv\tw\n0\t1\t2\t10\t20\n3\t4\t5\t40\t50\n6\tNA\tNA\t70\t80\n9\t10\t11\tNA\tNA\n"
        assert result == expected

    def test_strict_join_success(self, tsv_factory, tmp_path):
        """Test successful strict join with matching keys."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n6\t7\t8\n"
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n6\t70\t80\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "strict", output)

        # Read output
        with gzip.open(output, "rt") as f:
            result = f.read()

        expected = "x\ty\tz\tv\tw\n0\t1\t2\t10\t20\n3\t4\t5\t40\t50\n6\t7\t8\t70\t80\n"
        assert result == expected

    def test_empty_file1_inner_join(self, tsv_factory, tmp_path):
        """Test inner join with empty first file."""
        tsv1_content = ""
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "inner", output)

        # Read output - should be empty
        with gzip.open(output, "rt") as f:
            result = f.read()

        assert result == ""

    def test_empty_file2_inner_join(self, tsv_factory, tmp_path):
        """Test inner join with empty second file."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = ""
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "inner", output)

        # Read output - should be empty
        with gzip.open(output, "rt") as f:
            result = f.read()

        assert result == ""

    def test_empty_file1_left_join(self, tsv_factory, tmp_path):
        """Test left join with empty first file."""
        tsv1_content = ""
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "left", output)

        # Read output - should be empty (left side is empty)
        with gzip.open(output, "rt") as f:
            result = f.read()

        assert result == ""

    def test_empty_file2_left_join(self, tsv_factory, tmp_path):
        """Test left join with empty second file."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = ""
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "left", output)

        # Read output - should match tsv1 (left side preserved)
        with gzip.open(output, "rt") as f:
            result = f.read()

        assert result == tsv1_content

    def test_empty_file1_right_join(self, tsv_factory, tmp_path):
        """Test right join with empty first file."""
        tsv1_content = ""
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "right", output)

        # Read output - should match tsv2 (right side preserved)
        with gzip.open(output, "rt") as f:
            result = f.read()

        assert result == tsv2_content

    def test_empty_file2_right_join(self, tsv_factory, tmp_path):
        """Test right join with empty second file."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = ""
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "right", output)

        # Read output - should be empty (right side is empty)
        with gzip.open(output, "rt") as f:
            result = f.read()

        assert result == ""

    def test_empty_file1_outer_join(self, tsv_factory, tmp_path):
        """Test outer join with empty first file."""
        tsv1_content = ""
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "outer", output)

        # Read output - should match tsv2
        with gzip.open(output, "rt") as f:
            result = f.read()

        assert result == tsv2_content

    def test_empty_file2_outer_join(self, tsv_factory, tmp_path):
        """Test outer join with empty second file."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = ""
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        join_tsvs.join_tsvs(tsv1, tsv2, "x", "outer", output)

        # Read output - should match tsv1
        with gzip.open(output, "rt") as f:
            result = f.read()

        assert result == tsv1_content

    def test_empty_file1_strict_join_raises_error(self, tsv_factory, tmp_path):
        """Test strict join with empty first file raises error."""
        tsv1_content = ""
        tsv2_content = "x\tv\tw\n0\t10\t20\n3\t40\t50\n"
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        with pytest.raises(ValueError, match="Strict join cannot be performed with empty file"):
            join_tsvs.join_tsvs(tsv1, tsv2, "x", "strict", output)

    def test_empty_file2_strict_join_raises_error(self, tsv_factory, tmp_path):
        """Test strict join with empty second file raises error."""
        tsv1_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        tsv2_content = ""
        tsv1 = tsv_factory.create_plain("tsv1.tsv", tsv1_content)
        tsv2 = tsv_factory.create_plain("tsv2.tsv", tsv2_content)
        output = str(tmp_path / "output.tsv.gz")

        with pytest.raises(ValueError, match="Strict join cannot be performed with empty file"):
            join_tsvs.join_tsvs(tsv1, tsv2, "x", "strict", output)
