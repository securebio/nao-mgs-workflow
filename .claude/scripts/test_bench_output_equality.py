"""Tests for bench_output_equality.py."""

import gzip
import hashlib
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock

import pytest

from bench_output_equality import (
    S3Uri,
    classify_diff,
    compare_prefixes,
    fetch_and_hash,
    is_known_noise,
    list_keys,
    render_markdown,
)


@pytest.mark.parametrize(
    ("uri", "bucket", "prefix"),
    [
        ("s3://bucket/", "bucket", ""),
        ("s3://bucket", "bucket", ""),
        ("s3://bucket/foo", "bucket", "foo/"),
        ("s3://bucket/foo/", "bucket", "foo/"),
        ("s3://bucket/foo/bar/baz/", "bucket", "foo/bar/baz/"),
    ],
)
def test_s3uri_parse(uri: str, bucket: str, prefix: str) -> None:
    parsed = S3Uri.parse(uri)
    assert parsed.bucket == bucket
    assert parsed.prefix == prefix


@pytest.mark.parametrize("bad", ["not-an-s3-uri", "http://bucket/", "s3:///key"])
def test_s3uri_parse_rejects_invalid(bad: str) -> None:
    with pytest.raises(ValueError):
        S3Uri.parse(bad)


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("kraken.tsv.gz", True),
        ("sub/dir/kraken.tsv.gz", True),
        ("qc_basic_stats_cleaned.tsv.gz", True),
        ("results/qc_basic_stats_cleaned.tsv.gz", True),
        ("virus_hits.tsv.gz", False),
        ("read_counts.tsv", False),
        ("kraken.tsv", False),  # not gzipped → different file
    ],
)
def test_is_known_noise(key: str, expected: bool) -> None:
    assert is_known_noise(key) is expected


def test_classify_diff() -> None:
    assert classify_diff("kraken.tsv.gz") == "diff_known_noise"
    assert classify_diff("virus_hits.tsv.gz") == "diff_unexpected"


def _mock_get_object_body(content: bytes) -> dict[str, Any]:
    """Build a fake `get_object` response dict with `Body` that reads bytes."""
    return {"Body": BytesIO(content)}


def test_fetch_and_hash_plain() -> None:
    s3 = MagicMock()
    payload = b"line2\nline1\nline3\n"
    s3.get_object.return_value = _mock_get_object_body(payload)

    result = fetch_and_hash(s3, "bucket", "key.tsv")
    expected = hashlib.md5(b"\n".join(sorted(payload.split(b"\n"))), usedforsecurity=False).hexdigest()
    assert result == expected


def test_fetch_and_hash_gzipped() -> None:
    s3 = MagicMock()
    inner = b"line2\nline1\nline3\n"
    gz = gzip.compress(inner)
    s3.get_object.return_value = _mock_get_object_body(gz)

    result = fetch_and_hash(s3, "bucket", "key.tsv.gz")
    expected = hashlib.md5(b"\n".join(sorted(inner.split(b"\n"))), usedforsecurity=False).hexdigest()
    assert result == expected


def test_fetch_and_hash_order_invariant() -> None:
    """Same content in different line orders should hash identically."""
    s3 = MagicMock()
    a = b"x\ny\nz\n"
    b = b"z\nx\ny\n"
    s3.get_object.side_effect = [_mock_get_object_body(a), _mock_get_object_body(b)]
    assert fetch_and_hash(s3, "bucket", "k") == fetch_and_hash(s3, "bucket", "k")


def _mock_paginator(pages: list[list[str]]) -> Any:
    """Build a mock S3 client whose list_objects_v2 paginator yields the given pages of keys."""
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": k} for k in page]} for page in pages
    ]
    s3.get_paginator.return_value = paginator
    return s3


def test_list_keys_strips_prefix() -> None:
    s3 = _mock_paginator([["foo/a.tsv", "foo/b.tsv", "foo/sub/c.tsv"]])
    keys = list(list_keys(s3, S3Uri(bucket="bucket", prefix="foo/")))
    assert keys == ["a.tsv", "b.tsv", "sub/c.tsv"]


def test_list_keys_skips_prefix_only_object() -> None:
    # Sometimes S3 returns the prefix itself as a 0-byte "directory" marker.
    s3 = _mock_paginator([["foo/", "foo/a.tsv"]])
    keys = list(list_keys(s3, S3Uri(bucket="bucket", prefix="foo/")))
    assert keys == ["a.tsv"]


def _make_full_mock(dev_objs: dict[str, bytes], pr_objs: dict[str, bytes]) -> Any:
    """Build a mock S3 client backed by two in-memory key→bytes maps.

    Paginator returns keys; get_object returns the bytes. Keys in `dev_objs`
    appear under the "dev/" prefix; keys in `pr_objs` appear under "pr/".
    """
    s3 = MagicMock()
    paginator = MagicMock()

    def paginate(Bucket: str, Prefix: str) -> list[dict[str, Any]]:  # noqa: N803 (boto3 arg name)
        if Prefix == "dev/":
            keys = [f"dev/{k}" for k in dev_objs]
        elif Prefix == "pr/":
            keys = [f"pr/{k}" for k in pr_objs]
        else:
            keys = []
        return [{"Contents": [{"Key": k} for k in keys]}]

    paginator.paginate.side_effect = paginate
    s3.get_paginator.return_value = paginator

    def get_object(Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        if Key.startswith("dev/"):
            return _mock_get_object_body(dev_objs[Key[len("dev/") :]])
        if Key.startswith("pr/"):
            return _mock_get_object_body(pr_objs[Key[len("pr/") :]])
        raise KeyError(Key)

    s3.get_object.side_effect = get_object
    return s3


def test_compare_prefixes_all_ok() -> None:
    body = b"a\nb\nc\n"
    s3 = _make_full_mock({"f.tsv": body, "g.tsv": body}, {"f.tsv": body, "g.tsv": body})
    report = compare_prefixes(s3, S3Uri("bucket", "dev/"), S3Uri("bucket", "pr/"))
    assert report["summary"]["total"] == 2
    assert report["summary"]["ok"] == 2
    assert report["summary"]["diff_unexpected"] == 0


def test_compare_prefixes_unexpected_diff() -> None:
    s3 = _make_full_mock({"virus_hits.tsv.gz": gzip.compress(b"a\nb\n")}, {"virus_hits.tsv.gz": gzip.compress(b"a\nc\n")})
    report = compare_prefixes(s3, S3Uri("bucket", "dev/"), S3Uri("bucket", "pr/"))
    assert report["summary"]["diff_unexpected"] == 1
    assert report["files"][0]["status"] == "diff_unexpected"


def test_compare_prefixes_known_noise_diff() -> None:
    """A DIFF on a known-noise file is classified diff_known_noise, not diff_unexpected."""
    s3 = _make_full_mock({"kraken.tsv.gz": gzip.compress(b"a\nb\n")}, {"kraken.tsv.gz": gzip.compress(b"a\nc\n")})
    report = compare_prefixes(s3, S3Uri("bucket", "dev/"), S3Uri("bucket", "pr/"))
    assert report["summary"]["diff_known_noise"] == 1
    assert report["summary"]["diff_unexpected"] == 0
    assert report["files"][0]["status"] == "diff_known_noise"
    assert report["files"][0]["note"] is not None


def test_compare_prefixes_dev_only_and_pr_only() -> None:
    s3 = _make_full_mock({"a.tsv": b"x\n"}, {"b.tsv": b"x\n"})
    report = compare_prefixes(s3, S3Uri("bucket", "dev/"), S3Uri("bucket", "pr/"))
    assert report["summary"]["dev_only"] == 1
    assert report["summary"]["pr_only"] == 1
    by_key = {f["key"]: f for f in report["files"]}
    assert by_key["a.tsv"]["status"] == "dev_only"
    assert by_key["b.tsv"]["status"] == "pr_only"


def test_compare_prefixes_order_difference_treated_as_ok() -> None:
    """Same lines in a different order should hash identically and report OK."""
    a = b"line1\nline2\nline3\n"
    b = b"line3\nline1\nline2\n"
    s3 = _make_full_mock({"f.tsv": a}, {"f.tsv": b})
    report = compare_prefixes(s3, S3Uri("bucket", "dev/"), S3Uri("bucket", "pr/"))
    assert report["summary"]["ok"] == 1
    assert report["summary"]["diff_unexpected"] == 0


def test_render_markdown_all_ok() -> None:
    report = {
        "dev_prefix": "s3://b/dev/",
        "pr_prefix": "s3://b/pr/",
        "summary": {"total": 5, "ok": 5, "diff_unexpected": 0, "diff_known_noise": 0, "dev_only": 0, "pr_only": 0},
        "files": [],
    }
    md = render_markdown(report)  # type: ignore[arg-type]
    assert "All files byte-identical" in md
    assert "OK (byte-identical) | 5" in md


def test_render_markdown_with_diffs() -> None:
    report = {
        "dev_prefix": "s3://b/dev/",
        "pr_prefix": "s3://b/pr/",
        "summary": {"total": 2, "ok": 0, "diff_unexpected": 1, "diff_known_noise": 1, "dev_only": 0, "pr_only": 0},
        "files": [
            {"key": "virus_hits.tsv.gz", "status": "diff_unexpected", "dev_hash": "a", "pr_hash": "b", "note": None},
            {"key": "kraken.tsv.gz", "status": "diff_known_noise", "dev_hash": "c", "pr_hash": "d", "note": "order-sensitive estimator drift"},
        ],
    }
    md = render_markdown(report)  # type: ignore[arg-type]
    assert "Files needing attention" in md
    assert "virus_hits.tsv.gz" in md
    assert "kraken.tsv.gz" in md
    assert "order-sensitive estimator drift" in md
