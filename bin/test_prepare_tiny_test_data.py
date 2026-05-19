#!/usr/bin/env python3
"""Unit tests for prepare_tiny_test_data.py"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from prepare_tiny_test_data import parse_s3_uri


class TestParseS3Uri:
    @pytest.mark.parametrize(
        ("uri", "default_key", "expected_bucket", "expected_key"),
        [
            # Standard cases
            ("s3://bucket/key", "default.txt", "bucket", "key"),
            ("s3://bucket/path/to/key", "default.txt", "bucket", "path/to/key"),
            # No key in URI: use default
            ("s3://bucket", "default.txt", "bucket", "default.txt"),
            ("s3://bucket/", "default.txt", "bucket", "default.txt"),
            # Trailing slash on key stripped
            ("s3://bucket/key/", "default.txt", "bucket", "key"),
            # Regression for B005: lstrip("s3://") would treat "s3://" as a
            # character set and consume any leading combination of {s,3,:,/}.
            # removeprefix("s3://") must strip the literal scheme exactly once.
            ("s3://sssbucket/key", "default.txt", "sssbucket", "key"),
            ("s3://3bucket/key", "default.txt", "3bucket", "key"),
        ],
    )
    def test_valid(
        self, uri: str, default_key: str, expected_bucket: str, expected_key: str,
    ) -> None:
        bucket, key = parse_s3_uri(uri, default_key)
        assert bucket == expected_bucket
        assert key == expected_key

    @pytest.mark.parametrize(
        "uri",
        [
            "",
            "bucket/key",
            "http://bucket/key",
            "S3://bucket/key",  # uppercase scheme rejected (case-sensitive)
            "s3:/bucket/key",  # missing second slash
        ],
    )
    def test_invalid(self, uri: str) -> None:
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            parse_s3_uri(uri, "default.txt")
