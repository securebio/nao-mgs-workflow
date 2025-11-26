#!/usr/bin/env python

# TODO: Add unit tests for individual functions in a future pass

import pytest

import filter_tsv_column_by_value


class TestFilterTsvColumnByValue:
    """Test the filter_tsv_column_by_value module."""

    def test_empty_file_keep_matching(self, tsv_factory):
        """Test handling of completely empty file with keep_matching=True."""
        input_file = tsv_factory.create_plain("input.tsv", "")
        output_file = tsv_factory.get_path("output.tsv")

        # Mock logger
        import logging
        logger = logging.getLogger()

        with open(input_file, "r") as inf, open(output_file, "w") as outf:
            filter_tsv_column_by_value.stream_and_filter_tsv(
                inf,
                outf,
                "test_column",
                "test_value",
                keep_matching=True,
                logger=logger
            )


        result = tsv_factory.read_plain(output_file)
        assert result == ""

    def test_header_only_keep_matching(self, tsv_factory):
        """Test handling of header-only file with keep_matching=True."""
        input_content = "x\ty\tz\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        import logging
        logger = logging.getLogger()

        with open(input_file, "r") as inf, open(output_file, "w") as outf:
            filter_tsv_column_by_value.stream_and_filter_tsv(
                inf,
                outf,
                "x",
                "test_value",
                keep_matching=True,
                logger=logger
            )

        result = tsv_factory.read_plain(output_file)
        assert result == input_content

    def test_missing_column_raises_error(self, tsv_factory):
        """Test that missing column raises ValueError."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        import logging
        logger = logging.getLogger()

        with pytest.raises(ValueError, match="Column 'nonexistent_column' not found in header"):
            with open(input_file, "r") as inf, open(output_file, "w") as outf:
                filter_tsv_column_by_value.stream_and_filter_tsv(
                    inf,
                    outf,
                    "nonexistent_column",
                    "false",
                    keep_matching=True,
                    logger=logger
                )

    def test_filter_keep_no_rows(self, tsv_factory):
        """Test filtering that removes all rows (keep_matching=True, no matching values)."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        import logging
        logger = logging.getLogger()

        with open(input_file, "r") as inf, open(output_file, "w") as outf:
            filter_tsv_column_by_value.stream_and_filter_tsv(
                inf,
                outf,
                "x",
                5,  # No rows have x=5
                keep_matching=True,
                logger=logger
            )

        result = tsv_factory.read_plain(output_file)
        lines = result.strip().split("\n")

        # Should have only header
        assert len(lines) == 1
        assert lines[0] == "x\ty\tz"

    def test_filter_keep_matching_rows(self, tsv_factory):
        """Test keeping rows with matching value (keep_matching=True)."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        import logging
        logger = logging.getLogger()

        with open(input_file, "r") as inf, open(output_file, "w") as outf:
            filter_tsv_column_by_value.stream_and_filter_tsv(
                inf,
                outf,
                "x",
                6,  # Only one row has x=6
                keep_matching=True,
                logger=logger
            )

        result = tsv_factory.read_plain(output_file)
        lines = result.strip().split("\n")

        # Should have header + 1 data row
        assert len(lines) == 2
        assert lines[0] == "x\ty\tz"
        assert lines[1] == "6\t7\t8"

    def test_filter_discard_matching_rows(self, tsv_factory):
        """Test discarding rows with matching value (keep_matching=False)."""
        input_content = "x\ty\tz\n0\t1\t2\n3\t4\t5\n3\t5\t6\n6\t7\t8\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        import logging
        logger = logging.getLogger()

        with open(input_file, "r") as inf, open(output_file, "w") as outf:
            filter_tsv_column_by_value.stream_and_filter_tsv(
                inf,
                outf,
                "x",
                6,  # Discard the row with x=6
                keep_matching=False,
                logger=logger
            )

        result = tsv_factory.read_plain(output_file)
        lines = result.strip().split("\n")

        # Should have header + 3 data rows (all except x=6)
        assert len(lines) == 4
        assert lines[0] == "x\ty\tz"
        assert lines[1] == "0\t1\t2"
        assert lines[2] == "3\t4\t5"
        assert lines[3] == "3\t5\t6"

        # Verify no row has x=6
        for line in lines[1:]:
            fields = line.split("\t")
            assert int(fields[0]) != 6
