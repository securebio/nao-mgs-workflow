#!/usr/bin/env python

import logging
from io import StringIO

import pytest

import filter_tsv_column_by_value


class TestConvertValue:
    """Test the convert_value function."""

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("false", False),
            ("False", False),
            ("FALSE", False),
        ],
    )
    def test_convert_boolean(self, input_value, expected):
        """Test converting boolean strings."""
        assert filter_tsv_column_by_value.convert_value(input_value) is expected

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("123", 123),
            ("-456", -456),
            ("0", 0),
        ],
    )
    def test_convert_integer(self, input_value, expected):
        """Test converting integer strings."""
        assert filter_tsv_column_by_value.convert_value(input_value) == expected

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("123.45", 123.45),
            ("-67.89", -67.89),
            ("0.0", 0.0),
        ],
    )
    def test_convert_float(self, input_value, expected):
        """Test converting float strings."""
        assert filter_tsv_column_by_value.convert_value(input_value) == expected

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("hello", "hello"),
            ("test_value", "test_value"),
            ("", ""),
            ("  ", "  "),
        ],
    )
    def test_convert_string(self, input_value, expected):
        """Test converting plain strings."""
        assert filter_tsv_column_by_value.convert_value(input_value) == expected


class TestValidateInputColumns:
    """Test the validate_input_columns function."""

    def test_valid_column(self):
        """Test finding a valid column."""
        header = ["col1", "col2", "col3"]
        logger = logging.getLogger("test")
        index = filter_tsv_column_by_value.validate_input_columns(
            header, "col2", logger
        )
        assert index == 1

    def test_missing_column(self):
        """Test that missing column raises ValueError."""
        header = ["col1", "col2", "col3"]
        logger = logging.getLogger("test")
        with pytest.raises(ValueError, match="Column 'missing' not found in header"):
            filter_tsv_column_by_value.validate_input_columns(
                header, "missing", logger
            )


class TestStreamAndFilterTsv:
    """Test the stream_and_filter_tsv function."""

    @pytest.fixture
    def logger(self):
        """Provide a logger instance for tests."""
        return logging.getLogger("test")

    @pytest.mark.parametrize(
        "input_data,column,filter_value,keep_matching,expected",
        [
            (
                "col1\tcol2\tcol3\nval1\tmatch\tval3\nval4\tnomatch\tval6\nval7\tmatch\tval9\n",
                "col2",
                "match",
                True,
                "col1\tcol2\tcol3\nval1\tmatch\tval3\nval7\tmatch\tval9\n",
            ),
            (
                "col1\tcol2\tcol3\nval1\tmatch\tval3\nval4\tnomatch\tval6\nval7\tmatch\tval9\n",
                "col2",
                "match",
                False,
                "col1\tcol2\tcol3\nval4\tnomatch\tval6\n",
            ),
            (
                "col1\tcol2\tcol3\nval1\t123\tval3\nval4\t456\tval6\nval7\t123\tval9\n",
                "col2",
                123,
                True,
                "col1\tcol2\tcol3\nval1\t123\tval3\nval7\t123\tval9\n",
            ),
            (
                "",
                "col1",
                "value",
                True,
                "",
            ),
            (
                "col1\tcol2\tcol3\n",
                "col2",
                "value",
                True,
                "col1\tcol2\tcol3\n",
            ),
            (
                "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n",
                "col2",
                "nomatch",
                True,
                "col1\tcol2\tcol3\n",
            ),
            (
                "col1\tcol2\tcol3\nval1\tmatch\tval3\nval4\tmatch\tval6\n",
                "col2",
                "match",
                True,
                "col1\tcol2\tcol3\nval1\tmatch\tval3\nval4\tmatch\tval6\n",
            ),
        ],
        ids=[
            "keep_matching",
            "keep_non_matching",
            "type_conversion",
            "empty_file",
            "header_only",
            "no_matches",
            "all_rows_match",
        ],
    )
    def test_stream_and_filter_tsv(
        self, logger, input_data, column, filter_value, keep_matching, expected
    ):
        """Test filtering TSV data with various scenarios."""
        input_file = StringIO(input_data)
        output_file = StringIO()

        filter_tsv_column_by_value.stream_and_filter_tsv(
            input_file, output_file, column, filter_value, keep_matching, logger
        )

        result = output_file.getvalue()
        assert result == expected

    def test_invalid_column(self, logger):
        """Test that invalid column raises ValueError."""
        input_data = "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        input_file = StringIO(input_data)
        output_file = StringIO()

        with pytest.raises(ValueError, match="Column 'missing' not found"):
            filter_tsv_column_by_value.stream_and_filter_tsv(
                input_file, output_file, "missing", "value", True, logger
            )
