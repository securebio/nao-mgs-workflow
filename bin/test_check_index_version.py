#!/usr/bin/env python3
"""Unit tests for check_index_version.py"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from check_index_version import (
    fetch_pyproject_from_s3,
    get_index_version,
    is_dev_version,
    main,
)

INDEX_PYPROJECT = '[project]\nversion = "3.2.2.0"\n'
INDEX_PYPROJECT_DEV = '[project]\nversion = "3.2.2.1-dev"\n'


class TestGetIndexVersion:
    @pytest.mark.parametrize("version", ["3.2.2.0", "3.2.2.1-dev", "3.2.1.5"])
    def test_reads_version(self, version: str) -> None:
        assert get_index_version(f'[project]\nversion = "{version}"\n') == version


class TestIsDevVersion:
    @pytest.mark.parametrize(
        "version, expected",
        [("3.2.2.0", False), ("3.2.1.5", False), ("3.2.2.1-dev", True)],
    )
    def test_detects_dev(self, version: str, expected: bool) -> None:
        assert is_dev_version(version) is expected


class TestFetchPyprojectFromS3:
    @patch("check_index_version.boto3.client")
    def test_parses_s3_uri_and_returns_content(
        self, mock_boto_client: MagicMock
    ) -> None:
        mock_body = MagicMock()
        mock_body.read.return_value = INDEX_PYPROJECT.encode("utf-8")
        mock_client = mock_boto_client.return_value
        mock_client.get_object.return_value = {"Body": mock_body}
        result = fetch_pyproject_from_s3("s3://my-bucket/path/to/pyproject.toml")
        mock_client.get_object.assert_called_once_with(
            Bucket="my-bucket", Key="path/to/pyproject.toml"
        )
        assert result == INDEX_PYPROJECT


@patch("check_index_version.parse_arguments")
@patch("check_index_version.fetch_pyproject_from_s3")
class TestMain:
    def _args(self) -> MagicMock:
        return MagicMock(s3_pyproject="s3://bucket/pyproject.toml")

    def test_passes_for_release_version(
        self, mock_fetch: MagicMock, mock_parse_args: MagicMock
    ) -> None:
        mock_parse_args.return_value = self._args()
        mock_fetch.return_value = INDEX_PYPROJECT
        main()  # should not raise

    def test_raises_for_dev_version(
        self, mock_fetch: MagicMock, mock_parse_args: MagicMock
    ) -> None:
        mock_parse_args.return_value = self._args()
        mock_fetch.return_value = INDEX_PYPROJECT_DEV
        with pytest.raises(RuntimeError, match="-dev"):
            main()
