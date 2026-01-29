#!/usr/bin/env python3
DESC = """
Safely download or sync databases to a local scratch directory with file locking.

This script handles downloading reference databases (e.g., Bowtie2 indices, Kraken
databases) from S3 or local paths to a shared /scratch directory. It uses file
locking to prevent race conditions when multiple processes try to download the
same database simultaneously.

Key features:
- File locking prevents concurrent downloads of the same database
- Source path tracking ensures cached data matches the requested source
- Incremental sync (via aws s3 sync --delete or rsync --delete) ensures local
  copy matches remote, removing stale files from previous runs
- Automatic cache invalidation when source path changes

The scratch directory (/scratch by default) is typically a shared volume mounted
across AWS Batch workers, allowing databases to be reused across jobs on the
same instance.
"""

###########
# IMPORTS #
###########

import argparse
import fcntl
import logging
import os
import re
import shutil
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

###########
# LOGGING #
###########

class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

####################
# HELPER FUNCTIONS #
####################

def parse_source_path(path: str) -> tuple[str, bool]:
    """
    Normalize and parse a source path, determining if it's an S3 path.
    Handles malformed paths with multiple slashes and restores proper S3 URI format.
    Args:
        path: Input path string (S3 URI or local path)
    Returns:
        Tuple of (normalized_path, is_s3)
    """
    # Collapse consecutive slashes
    normalized = re.sub(r"//+", "/", path)
    # Check if S3 path and restore proper s3:// prefix
    if normalized.startswith("s3:/") or normalized.startswith("s3:"):
        path_without_prefix = re.sub(r"^s3:/*", "", normalized)
        return f"s3://{path_without_prefix}", True
    return normalized, False

@contextmanager
def file_lock(lock_file: Path, timeout_seconds: int | None = None):
    """
    Context manager for exclusive file locking.
    Args:
        lock_file: Path to the lock file (will be created if it doesn't exist)
        timeout_seconds: Maximum time to wait for lock, or None for no timeout
    Raises:
        TimeoutError: If the lock cannot be acquired within the timeout period
    """
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_file), os.O_RDWR | os.O_CREAT)
    try:
        if timeout_seconds is not None:
            start_time = time.time()
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.time() - start_time >= timeout_seconds:
                        raise TimeoutError(
                            f"Timed out waiting for lock after {timeout_seconds} seconds"
                        )
                    time.sleep(0.1)
        else:
            fcntl.flock(fd, fcntl.LOCK_EX)
        logger.info(f"Acquired lock: {lock_file}")
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        logger.info("Released lock")

##################
# DATABASE CACHE #
##################

class DatabaseCache:
    """
    Manages a cached database in the scratch directory.
    Tracks the source path used to populate the cache and handles
    invalidation when the source changes.
    """
    def __init__(self, db_name: str, scratch_dir: Path = Path("/scratch")):
        self.db_name = db_name
        self.local_path = scratch_dir / db_name
        self.lock_file = scratch_dir / f"{db_name}.lock"
        self._source_marker = self.local_path / ".source_path"

    def get_cached_source(self) -> str | None:
        """Return the source path used to populate this cache, or None if not cached."""
        if not self._source_marker.exists():
            return None
        return self._source_marker.read_text().strip()

    def set_cached_source(self, source_path: str) -> None:
        """Record the source path used to populate this cache."""
        self._source_marker.write_text(source_path)

    def clear(self) -> None:
        """Remove the cached database directory."""
        if self.local_path.exists():
            logger.info(f"Removing stale cache: {self.local_path}")
            shutil.rmtree(self.local_path)

    def invalidate_if_source_changed(self, source_path: str) -> None:
        """Clear cache if it was populated from a different source."""
        cached_source = self.get_cached_source()
        if cached_source is not None and cached_source != source_path:
            logger.warning(f"Cache was populated from different source: {cached_source}")
            self.clear()

#####################
# DATABASE TRANSFER #
#####################

def configure_aws_s3_transfer() -> None:
    """Configure AWS CLI settings for optimal S3 transfer performance."""
    settings = [
        ("default.s3.max_concurrent_requests", "20"),
        ("default.s3.multipart_threshold", "64MB"),
        ("default.s3.multipart_chunksize", "16MB"),
    ]
    for key, value in settings:
        subprocess.run(
            ["aws", "configure", "set", key, value],
            check=True,
            capture_output=True,
        )

def sync_from_s3(source_path: str, local_path: Path) -> None:
    """
    Sync a database from S3 using 'aws s3 sync --delete'.
    The --delete flag removes local files not present in the source,
    preventing stale files from previous runs.
    """
    logger.info(f"Syncing from S3: {source_path} -> {local_path}")
    configure_aws_s3_transfer()
    result = subprocess.run(
        ["aws", "s3", "sync", source_path, str(local_path), "--delete"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"aws s3 sync failed: {result.stderr}")
    logger.info("S3 sync completed")

def sync_from_local(source_path: str, local_path: Path) -> None:
    """
    Sync a database from a local path using 'rsync -a --delete'.
    The --delete flag removes destination files not present in the source.
    """
    logger.info(f"Syncing from local path: {source_path} -> {local_path}")
    result = subprocess.run(
        ["rsync", "-a", "--delete", f"{source_path}/", f"{local_path}/"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"rsync failed: {result.stderr}")
    logger.info("Local sync completed")

##############
# MAIN LOGIC #
##############

def download_database(
    source_path: str,
    timeout_seconds: int | None = None,
    scratch_dir: Path = Path("/scratch"),
) -> Path:
    """
    Download or sync a database to the scratch directory with locking.
    Args:
        source_path: S3 URI or local path to the database
        timeout_seconds: Maximum time to wait for lock, or None for no timeout
        scratch_dir: Base directory for local database storage
    Returns:
        Path to the local database directory
    """
    logger.info(f"Starting database download: {source_path}")
    # Parse source path
    source_path, is_s3 = parse_source_path(source_path)
    logger.info(f"Normalized source path: {source_path} (S3: {is_s3})")
    # Set up cache
    db_name = os.path.basename(source_path.rstrip("/"))
    cache = DatabaseCache(db_name, scratch_dir)
    # Ensure scratch directory exists
    scratch_dir.mkdir(parents=True, exist_ok=True)
    with file_lock(cache.lock_file, timeout_seconds):
        cache.invalidate_if_source_changed(source_path)
        cache.local_path.mkdir(parents=True, exist_ok=True)
        if is_s3:
            sync_from_s3(source_path, cache.local_path)
        else:
            sync_from_local(source_path, cache.local_path)
        cache.set_cached_source(source_path)
        logger.info(f"Database available at: {cache.local_path}")
        return cache.local_path

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "source_path",
        help="S3 URI or local path to the database directory",
    )
    parser.add_argument(
        "timeout_seconds",
        nargs="?",
        type=int,
        default=None,
        help="Maximum time in seconds to wait for lock acquisition (default: no timeout)",
    )
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    logger.info("Initializing script.")
    start_time = time.time()
    try:
        args = parse_arguments()
        logger.info(f"Arguments: {args}")
        download_database(args.source_path, args.timeout_seconds)
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Total time elapsed: {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()
