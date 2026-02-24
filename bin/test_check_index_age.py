#!/usr/bin/env python3
"""Unit tests for check_index_age.py"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from check_index_age import (
    check_index_age,
    fetch_time_txt_from_s3,
    get_max_index_age_days,
    main,
    parse_index_date,
)


class TestGetMaxIndexAgeDays:
    @pytest.mark.parametrize("age_days", [90, 120])
    def test_reads_value(self, tmp_path, age_days):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            f"[tool.mgs-workflow]\nmax-stable-index-age-days = {age_days}\n"
        )
        assert get_max_index_age_days(str(pyproject)) == age_days

    def test_missing_key(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("[tool.mgs-workflow]\n")
        with pytest.raises(KeyError):
            get_max_index_age_days(str(pyproject))


class TestParseIndexDate:
    @pytest.mark.parametrize(
        "time_text, expected",
        [
            ("2025-01-15 14:30:00 UTC (+0000)\n", date(2025, 1, 15)),
            ("2024-12-31 23:59:59 UTC (+0000)\n", date(2024, 12, 31)),
            ("  2025-06-15 10:00:00 UTC (+0000)  \n", date(2025, 6, 15)),
        ],
        ids=["typical", "year-boundary", "extra-whitespace"],
    )
    def test_parses_date(self, time_text, expected):
        assert parse_index_date(time_text) == expected

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            parse_index_date("not a timestamp")


class TestCheckIndexAge:
    @pytest.mark.parametrize(
        "index_date, max_age, today, expected_ok, expected_days",
        [
            (date(2025, 2, 1), 90, date(2025, 3, 1), True, 28),
            (date(2025, 1, 1), 90, date(2025, 4, 1), True, 90),
            (date(2025, 1, 1), 90, date(2025, 4, 2), False, 91),
            (date(2025, 1, 1), 90, date(2025, 6, 1), False, 151),
        ],
        ids=["within-limit", "exactly-at-limit", "one-over", "well-over"],
    )
    def test_age_check(self, index_date, max_age, today, expected_ok, expected_days):
        is_ok, age_days = check_index_age(index_date, max_age, today=today)
        assert is_ok is expected_ok
        assert age_days == expected_days

    def test_defaults_to_today(self):
        is_ok, age_days = check_index_age(date.today(), 90)
        assert is_ok is True
        assert age_days == 0


class TestFetchTimeTxtFromS3:
    @patch("check_index_age.boto3.client")
    def test_parses_s3_uri_and_returns_content(self, mock_boto_client):
        mock_body = MagicMock()
        mock_body.read.return_value = b"2025-01-15 14:30:00 UTC (+0000)\n"
        mock_client = mock_boto_client.return_value
        mock_client.get_object.return_value = {"Body": mock_body}
        result = fetch_time_txt_from_s3("s3://my-bucket/path/to/time.txt")
        mock_client.get_object.assert_called_once_with(
            Bucket="my-bucket", Key="path/to/time.txt"
        )
        assert result == "2025-01-15 14:30:00 UTC (+0000)\n"


@patch("check_index_age.date")
@patch("check_index_age.fetch_time_txt_from_s3")
@patch("check_index_age.parse_arguments")
class TestMain:
    def _make_args(self, pyproject_path):
        return MagicMock(
            s3_time_txt="s3://bucket/time.txt",
            pyproject=pyproject_path,
        )

    def test_passes_when_index_is_fresh(
        self, mock_parse_args, mock_fetch, mock_date, tmp_path
    ):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("[tool.mgs-workflow]\nmax-stable-index-age-days = 90\n")
        mock_parse_args.return_value = self._make_args(str(pyproject))
        mock_fetch.return_value = "2025-06-01 12:00:00 UTC (+0000)\n"
        mock_date.today.return_value = date(2025, 6, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        main()  # should not raise

    def test_raises_when_index_is_stale(
        self, mock_parse_args, mock_fetch, mock_date, tmp_path
    ):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("[tool.mgs-workflow]\nmax-stable-index-age-days = 90\n")
        mock_parse_args.return_value = self._make_args(str(pyproject))
        mock_fetch.return_value = "2025-01-01 12:00:00 UTC (+0000)\n"
        mock_date.today.return_value = date(2025, 6, 1)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        with pytest.raises(RuntimeError, match="151 days old"):
            main()
