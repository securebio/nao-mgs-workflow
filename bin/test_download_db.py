#!/usr/bin/env python3
"""
Unit tests for download-db.py

Run with: pytest bin/test_download_db.py
"""

import fcntl
import subprocess

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from download_db import (
    parse_source_path,
    get_cache_name,
    file_lock,
    configure_aws_s3_transfer,
    sync_from_s3,
    sync_from_local,
    download_database,
    main,
)

class TestParseSourcePath:
    """Test source path parsing and normalization."""

    @pytest.mark.parametrize("input_path,expected_path,expected_is_s3", [
        # S3 paths - various malformed inputs
        ("s3://bucket/path/db", "s3://bucket/path/db", True),
        ("s3:///bucket/path/db", "s3://bucket/path/db", True),
        ("s3:/bucket/path/db", "s3://bucket/path/db", True),
        ("s3:bucket/path/db", "s3://bucket/path/db", True),
        ("s3://bucket//path///db", "s3://bucket/path/db", True),
        # Local paths
        ("/path/to/db", "/path/to/db", False),
        ("/path//to///db", "/path/to/db", False),
        ("relative/path/db", "relative/path/db", False),
    ])
    def test_parse_source_path(self, input_path, expected_path, expected_is_s3):
        """Test path normalization with various inputs."""
        normalized, is_s3 = parse_source_path(input_path)
        assert normalized == expected_path
        assert is_s3 == expected_is_s3

class TestGetCacheName:
    """Test cache name generation."""

    @pytest.mark.parametrize("source_path,expected_name", [
        ("s3://bucket/path/kraken_db", "kraken_db_35f571f1"),
        ("s3://bucket/path/db", "db_56898535"),
        ("/local/path/bowtie2_idx", "bowtie2_idx_46a0d77f"),
    ])
    def test_cache_name(self, source_path, expected_name):
        """Test that cache name is basename + first 8 chars of sha256 hash."""
        assert get_cache_name(source_path) == expected_name

class TestFileLock:
    """Test file locking context manager."""

    @pytest.mark.parametrize("lock_path", ["test.lock", "subdir/nested/test.lock"])
    def test_lock_creates_file_and_parents(self, tmp_path, lock_path):
        """Test that lock creates the lock file and any parent directories."""
        lock_file = tmp_path / lock_path
        with file_lock(lock_file):
            assert lock_file.exists()

    def test_lock_timeout(self, tmp_path):
        """Test that timeout raises TimeoutError when lock is held by another."""
        lock_file = tmp_path / "test.lock"
        with open(lock_file, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            with pytest.raises(TimeoutError, match="Timed out waiting for lock"):
                with file_lock(lock_file, timeout_seconds=0.2):
                    pass
            # Lock is released when file is closed

class TestConfigureAwsS3Transfer:
    """Test AWS S3 transfer configuration."""

    @patch('download_db.subprocess.run')
    def test_configure_aws_s3_transfer(self, mock_run):
        """Test that AWS CLI settings are configured correctly."""
        mock_run.return_value = MagicMock(returncode=0)
        configure_aws_s3_transfer()
        assert mock_run.call_count == 3
        calls = [call[0][0] for call in mock_run.call_args_list]
        assert ["aws", "configure", "set", "default.s3.max_concurrent_requests", "20"] in calls
        assert ["aws", "configure", "set", "default.s3.multipart_threshold", "64MB"] in calls
        assert ["aws", "configure", "set", "default.s3.multipart_chunksize", "16MB"] in calls

    @patch('download_db.subprocess.run')
    def test_configure_aws_s3_transfer_failure(self, mock_run):
        """Test that configuration failure raises exception."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "aws")
        with pytest.raises(subprocess.CalledProcessError):
            configure_aws_s3_transfer()

class TestSyncFromS3:
    """Test S3 sync function."""

    @patch('download_db.configure_aws_s3_transfer')
    @patch('download_db.subprocess.run')
    def test_sync_from_s3_success(self, mock_run, mock_configure):
        """Test successful S3 sync."""
        sync_from_s3("s3://bucket/db", Path("/scratch/db"))
        mock_configure.assert_called_once()
        mock_run.assert_called_once_with(
            ["aws", "s3", "sync", "s3://bucket/db", "/scratch/db", "--delete"],
            check=True,
            capture_output=True,
            text=True,
        )

    @patch('download_db.configure_aws_s3_transfer')
    @patch('download_db.subprocess.run')
    def test_sync_from_s3_failure(self, mock_run, mock_configure):
        """Test S3 sync failure raises CalledProcessError."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "aws s3 sync")
        with pytest.raises(subprocess.CalledProcessError):
            sync_from_s3("s3://bucket/db", Path("/scratch/db"))

class TestSyncFromLocal:
    """Test local rsync function."""

    @patch('download_db.subprocess.run')
    def test_sync_from_local_success(self, mock_run):
        """Test successful local sync."""
        sync_from_local("/source/db", Path("/scratch/db"))
        mock_run.assert_called_once_with(
            ["rsync", "-a", "--delete", "/source/db/", "/scratch/db/"],
            check=True,
            capture_output=True,
            text=True,
        )

    @patch('download_db.subprocess.run')
    def test_sync_from_local_failure(self, mock_run):
        """Test local sync failure raises CalledProcessError."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "rsync")
        with pytest.raises(subprocess.CalledProcessError):
            sync_from_local("/source/db", Path("/scratch/db"))

class TestDownloadDatabase:
    """Test main download_database function."""

    @pytest.fixture
    def mock_file_lock(self):
        with patch('download_db.file_lock') as mock:
            mock.return_value.__enter__ = MagicMock()
            mock.return_value.__exit__ = MagicMock(return_value=False)
            yield mock

    @pytest.mark.parametrize("source_path,expected_basename,sync_func", [
        ("s3://bucket/kraken_db", "kraken_db_", "download_db.sync_from_s3"),
        ("/source/bowtie2_db", "bowtie2_db_", "download_db.sync_from_local"),
        ("s3:///bucket//path///db", "db_", "download_db.sync_from_s3"),  # tests normalization
    ])
    def test_download_database(self, mock_file_lock, tmp_path, source_path, expected_basename, sync_func):
        """Test downloading from S3 and local paths."""
        with patch(sync_func) as mock_sync:
            result = download_database(source_path, scratch_dir=tmp_path)
            assert result.parent == tmp_path
            assert result.name.startswith(expected_basename)
            mock_sync.assert_called_once()

    def test_download_database_creates_scratch_dir(self, mock_file_lock, tmp_path):
        """Test that scratch directory is created if it doesn't exist."""
        _ = mock_file_lock  # fixture needed for side effect
        with patch('download_db.sync_from_s3'):
            scratch = tmp_path / "new_scratch"
            assert not scratch.exists()
            download_database("s3://bucket/db", scratch_dir=scratch)
            assert scratch.exists()

    def test_download_database_passes_timeout(self, tmp_path):
        """Test that timeout is passed to file_lock."""
        with patch('download_db.file_lock') as mock_lock, patch('download_db.sync_from_s3'):
            mock_lock.return_value.__enter__ = MagicMock()
            mock_lock.return_value.__exit__ = MagicMock(return_value=False)
            download_database("s3://bucket/db", timeout_seconds=600, scratch_dir=tmp_path)
            assert mock_lock.call_args[0][1] == 600

class TestMain:
    """Test main entry point."""

    @pytest.mark.parametrize("argv,expected_args", [
        (['download_db.py', 's3://bucket/db', '300'], ('s3://bucket/db', 300)),
        (['download_db.py', '/local/path/db'], ('/local/path/db', None)),
    ])
    @patch('download_db.download_database')
    def test_main(self, mock_download, argv, expected_args):
        """Test that main parses args and calls download_database."""
        mock_download.return_value = Path("/scratch/db_abc123")
        with patch('sys.argv', argv):
            main()
            mock_download.assert_called_once_with(*expected_args)

    @patch('download_db.download_database')
    def test_main_prints_path(self, mock_download, capsys):
        """Test that main prints the local path to stdout."""
        mock_download.return_value = Path("/scratch/kraken_db_abc12345")
        with patch('sys.argv', ['download_db.py', 's3://bucket/db']):
            main()
        captured = capsys.readouterr()
        assert captured.out.strip() == "/scratch/kraken_db_abc12345"
