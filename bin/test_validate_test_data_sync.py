#!/usr/bin/env python3
"""Tests for validate_test_data_sync.py."""

import hashlib
import json
from pathlib import Path
import pytest
from validate_test_data_sync import (
    compute_md5,
    parse_snapshot,
    validate_snapshot,
)

def make_snapshot(content_map: dict[str, list[str]]) -> dict:
    """Helper to create snapshot structure from {name: [entries]} mapping."""
    return {
        name: {"content": entries, "meta": {}, "timestamp": "2026-01-01T00:00:00"}
        for name, entries in content_map.items()
    }

##################
# parse_snapshot #
##################

class TestParseSnapshot:
    @pytest.mark.parametrize(
        "content_entries,expected_md5_map",
        [
            # Single file
            (["file1.tsv.gz:md5,abc123"], {"file1": "abc123"}),
            # Multiple files
            (
                ["file1.tsv.gz:md5,abc123", "file2.tsv.gz:md5,def456"],
                {"file1": "abc123", "file2": "def456"},
            ),
            # Various extensions stripped
            (
                [
                    "file.tsv.gz:md5,abc123",
                    "another.csv.gz:md5,def456",
                    "simple.txt:md5,ghi789",
                ],
                {"file": "abc123", "another": "def456", "simple": "ghi789"},
            ),
            # Entries without :md5, are ignored
            (
                ["file1.tsv.gz:md5,abc123", "not_an_md5_entry", "another:entry:without:md5"],
                {"file1": "abc123"},
            ),
            # Empty content
            ([], {}),
        ],
        ids=["single", "multiple", "extensions", "ignore_non_md5", "empty"],
    )
    def test_parses_content_entries(
        self, tmp_path: Path, content_entries: list[str], expected_md5_map: dict[str, str]
    ) -> None:
        snapshot = make_snapshot({"test_output": content_entries})
        snapshot_path = tmp_path / "test.snap"
        snapshot_path.write_text(json.dumps(snapshot))
        result = parse_snapshot(snapshot_path)
        assert result == {"test_output": expected_md5_map}

    def test_parses_multiple_snapshots(self, tmp_path: Path) -> None:
        snapshot = make_snapshot({
            "snapshot_one": ["file1.tsv.gz:md5,aaa111"],
            "snapshot_two": ["file2.tsv.gz:md5,bbb222"],
        })
        snapshot_path = tmp_path / "test.snap"
        snapshot_path.write_text(json.dumps(snapshot))
        result = parse_snapshot(snapshot_path)
        assert result == {
            "snapshot_one": {"file1": "aaa111"},
            "snapshot_two": {"file2": "bbb222"},
        }

    def test_handles_missing_content_key(self, tmp_path: Path) -> None:
        snapshot = {"test_output": {"meta": {}, "timestamp": "2026-01-01T00:00:00"}}
        snapshot_path = tmp_path / "test.snap"
        snapshot_path.write_text(json.dumps(snapshot))
        result = parse_snapshot(snapshot_path)
        assert result == {"test_output": {}}

###############
# compute_md5 #
###############

class TestComputeMd5:
    @pytest.mark.parametrize(
        "content",
        [
            b"Hello, World!",
            b"",
            bytes(range(256)),
        ],
        ids=["text", "empty", "binary"],
    )
    def test_computes_correct_md5(self, tmp_path: Path, content: bytes) -> None:
        test_file = tmp_path / "test.bin"
        test_file.write_bytes(content)
        expected_md5 = hashlib.md5(content).hexdigest()
        assert compute_md5(test_file) == expected_md5

#####################
# validate_snapshot #
#####################


def setup_snapshot(
    tmp_path: Path, file_contents: dict[str, dict[str, bytes]]
) -> tuple[Path, Path]:
    """Create directories, files, and snapshot for testing."""
    results_dir = tmp_path / "results"
    snapshot_content = {}
    for snapshot_name, files in file_contents.items():
        snapshot_dir = results_dir / snapshot_name
        snapshot_dir.mkdir(parents=True)
        entries = []
        for stem, content in files.items():
            (snapshot_dir / f"{stem}.tsv").write_bytes(content)
            md5 = hashlib.md5(content).hexdigest()
            entries.append(f"{stem}.tsv.gz:md5,{md5}")
        snapshot_content[snapshot_name] = entries
    snapshot = make_snapshot(snapshot_content)
    snapshot_path = tmp_path / "test.snap"
    snapshot_path.write_text(json.dumps(snapshot))
    return snapshot_path, results_dir


class TestValidateSnapshot:
    @pytest.mark.parametrize(
        "file_contents",
        [
            {"test_output": {"file1": b"test content"}},
            {"test_output": {"file1": b"content one", "file2": b"content two"}},
            {"snap_one": {"file1": b"one"}, "snap_two": {"file2": b"two"}},
            {"test_output": {}},
        ],
        ids=["single_file", "multiple_files", "multiple_snapshots", "empty_content"],
    )
    def test_passes_valid_snapshots(
        self, tmp_path: Path, file_contents: dict[str, dict[str, bytes]]
    ) -> None:
        snapshot_path, results_dir = setup_snapshot(tmp_path, file_contents)
        validate_snapshot(snapshot_path, results_dir)

    @pytest.mark.parametrize(
        "snapshot_entries,create_subdir,create_file,error_type,error_match",
        [
            # Directory missing
            (["file1.tsv.gz:md5,abc123"], False, False, FileNotFoundError, "not found"),
            # File missing
            (["missing.tsv.gz:md5,abc123"], True, False, ValueError, "Errors found"),
            # MD5 mismatch
            (["file1.tsv.gz:md5,wrong"], True, True, ValueError, "Errors found"),
            # Multiple files match stem
            (["file1.tsv.gz:md5,abc123"], True, "both", ValueError, "Errors found"),
        ],
        ids=["dir_missing", "file_missing", "md5_mismatch", "multiple_matches"],
    )
    def test_raises_on_errors(
        self,
        tmp_path: Path,
        snapshot_entries: list[str],
        create_subdir: bool,
        create_file: bool | str,
        error_type: type,
        error_match: str,
    ) -> None:
        results_dir = tmp_path / "results"
        results_dir.mkdir()
        if create_subdir:
            snapshot_dir = results_dir / "test_output"
            snapshot_dir.mkdir()
            if create_file == True:
                (snapshot_dir / "file1.tsv").write_bytes(b"actual content")
            elif create_file == "both":
                (snapshot_dir / "file1.tsv").write_text("content1")
                (snapshot_dir / "file1.csv").write_text("content2")
        snapshot = make_snapshot({"test_output": snapshot_entries})
        snapshot_path = tmp_path / "test.snap"
        snapshot_path.write_text(json.dumps(snapshot))
        with pytest.raises(error_type, match=error_match):
            validate_snapshot(snapshot_path, results_dir)
