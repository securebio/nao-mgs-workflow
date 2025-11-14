#!/usr/bin/env python

import pytest
import gzip
import logging
from io import StringIO

# Import the module to test
import filter_tsv_column_by_value


class TestConvertValue:
    """Test the convert_value function."""

    def test_convert_boolean_true(self):
        """Test converting 'true' to boolean."""
        assert filter_tsv_column_by_value.convert_value("true") is True
        assert filter_tsv_column_by_value.convert_value("True") is True
        assert filter_tsv_column_by_value.convert_value("TRUE") is True

    def test_convert_boolean_false(self):
        """Test converting 'false' to boolean."""
        assert filter_tsv_column_by_value.convert_value("false") is False
        assert filter_tsv_column_by_value.convert_value("False") is False
        assert filter_tsv_column_by_value.convert_value("FALSE") is False

    def test_convert_integer(self):
        """Test converting integer strings."""
        assert filter_tsv_column_by_value.convert_value("123") == 123
        assert filter_tsv_column_by_value.convert_value("-456") == -456
        assert filter_tsv_column_by_value.convert_value("0") == 0

    def test_convert_float(self):
        """Test converting float strings."""
        assert filter_tsv_column_by_value.convert_value("123.45") == 123.45
        assert filter_tsv_column_by_value.convert_value("-67.89") == -67.89
        assert filter_tsv_column_by_value.convert_value("0.0") == 0.0

    def test_convert_string(self):
        """Test converting plain string."""
        assert filter_tsv_column_by_value.convert_value("hello") == "hello"
        assert filter_tsv_column_by_value.convert_value("test_value") == "test_value"

    def test_convert_empty_string(self):
        """Test converting empty string."""
        assert filter_tsv_column_by_value.convert_value("") == ""
        assert filter_tsv_column_by_value.convert_value("  ") == "  "


class TestValidateInputColumns:
    """Test the validate_input_columns function."""

    def test_valid_column(self):
        """Test finding a valid column."""
        header = ["col1", "col2", "col3"]
        logger = logging.getLogger("test")
        index = filter_tsv_column_by_value.validate_input_columns(header, "col2", logger)
        assert index == 1

    def test_missing_column(self):
        """Test that missing column raises ValueError."""
        header = ["col1", "col2", "col3"]
        logger = logging.getLogger("test")
        with pytest.raises(ValueError, match="Column 'missing' not found in header"):
            filter_tsv_column_by_value.validate_input_columns(header, "missing", logger)


class TestStreamAndFilterTsv:
    """Test the stream_and_filter_tsv function."""

    def test_filter_keep_matching(self):
        """Test filtering to keep matching rows."""
        input_data = "col1\tcol2\tcol3\nval1\tmatch\tval3\nval4\tnomatch\tval6\nval7\tmatch\tval9\n"
        input_file = StringIO(input_data)
        output_file = StringIO()
        logger = logging.getLogger("test")

        filter_tsv_column_by_value.stream_and_filter_tsv(
            input_file,
            output_file,
            "col2",
            "match",
            True,
            logger
        )

        result = output_file.getvalue()
        expected = "col1\tcol2\tcol3\nval1\tmatch\tval3\nval7\tmatch\tval9\n"
        assert result == expected

    def test_filter_keep_non_matching(self):
        """Test filtering to keep non-matching rows."""
        input_data = "col1\tcol2\tcol3\nval1\tmatch\tval3\nval4\tnomatch\tval6\nval7\tmatch\tval9\n"
        input_file = StringIO(input_data)
        output_file = StringIO()
        logger = logging.getLogger("test")

        filter_tsv_column_by_value.stream_and_filter_tsv(
            input_file,
            output_file,
            "col2",
            "match",
            False,
            logger
        )

        result = output_file.getvalue()
        expected = "col1\tcol2\tcol3\nval4\tnomatch\tval6\n"
        assert result == expected

    def test_filter_with_type_conversion(self):
        """Test filtering with automatic type conversion."""
        input_data = "col1\tcol2\tcol3\nval1\t123\tval3\nval4\t456\tval6\nval7\t123\tval9\n"
        input_file = StringIO(input_data)
        output_file = StringIO()
        logger = logging.getLogger("test")

        filter_tsv_column_by_value.stream_and_filter_tsv(
            input_file,
            output_file,
            "col2",
            123,
            True,
            logger
        )

        result = output_file.getvalue()
        expected = "col1\tcol2\tcol3\nval1\t123\tval3\nval7\t123\tval9\n"
        assert result == expected

    def test_empty_file(self):
        """Test handling of empty file."""
        input_file = StringIO("")
        output_file = StringIO()
        logger = logging.getLogger("test")

        filter_tsv_column_by_value.stream_and_filter_tsv(
            input_file,
            output_file,
            "col1",
            "value",
            True,
            logger
        )

        result = output_file.getvalue()
        assert result == ""

    def test_header_only_file(self):
        """Test file with only header line."""
        input_data = "col1\tcol2\tcol3\n"
        input_file = StringIO(input_data)
        output_file = StringIO()
        logger = logging.getLogger("test")

        filter_tsv_column_by_value.stream_and_filter_tsv(
            input_file,
            output_file,
            "col2",
            "value",
            True,
            logger
        )

        result = output_file.getvalue()
        expected = "col1\tcol2\tcol3\n"
        assert result == expected

    def test_no_matches(self):
        """Test when no rows match the filter."""
        input_data = "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n"
        input_file = StringIO(input_data)
        output_file = StringIO()
        logger = logging.getLogger("test")

        filter_tsv_column_by_value.stream_and_filter_tsv(
            input_file,
            output_file,
            "col2",
            "nomatch",
            True,
            logger
        )

        result = output_file.getvalue()
        expected = "col1\tcol2\tcol3\n"
        assert result == expected

    def test_all_rows_match(self):
        """Test when all rows match the filter."""
        input_data = "col1\tcol2\tcol3\nval1\tmatch\tval3\nval4\tmatch\tval6\n"
        input_file = StringIO(input_data)
        output_file = StringIO()
        logger = logging.getLogger("test")

        filter_tsv_column_by_value.stream_and_filter_tsv(
            input_file,
            output_file,
            "col2",
            "match",
            True,
            logger
        )

        result = output_file.getvalue()
        expected = "col1\tcol2\tcol3\nval1\tmatch\tval3\nval4\tmatch\tval6\n"
        assert result == expected

    def test_invalid_column(self):
        """Test that invalid column raises ValueError."""
        input_data = "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        input_file = StringIO(input_data)
        output_file = StringIO()
        logger = logging.getLogger("test")

        with pytest.raises(ValueError, match="Column 'missing' not found"):
            filter_tsv_column_by_value.stream_and_filter_tsv(
                input_file,
                output_file,
                "missing",
                "value",
                True,
                logger
            )
