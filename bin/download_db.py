#!/usr/bin/env python3
DESC = """
Safely download or sync databases to a local scratch directory with file locking.

This script handles downloading reference databases (e.g., Bowtie2 indices, Kraken
databases) from S3 or local paths to a shared /scratch directory. It uses file
locking to prevent race conditions when multiple processes try to download the
same database simultaneously.

Key features:
- File locking prevents concurrent downloads of the same database
- Unique cache directories per source path (using hash suffix) prevent collisions
- Incremental sync (via aws s3 sync --delete or rsync --delete) ensures local
  copy matches remote, removing stale files from previous runs

The scratch directory (/scratch by default) is typically a shared volume mounted
across AWS Batch workers, allowing databases to be reused across jobs on the
same instance.
"""

###########
# IMPORTS #
###########

import argparse
import fcntl
import hashlib
import logging
import os
import re
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

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

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

def get_cache_name(source_path: str) -> str:
    """
    Generate a unique cache directory name from source path.
    Uses basename for readability plus a hash suffix to avoid collisions
    when different sources have the same basename.
    """
    basename = os.path.basename(source_path.rstrip("/"))
    path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
    return f"{basename}_{path_hash}"

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
    # Set up cache with unique name based on source path
    cache_name = get_cache_name(source_path)
    local_path = scratch_dir / cache_name
    lock_file = scratch_dir / f"{cache_name}.lock"
    logger.info(f"Cache directory: {local_path}")
    # Ensure scratch directory exists
    scratch_dir.mkdir(parents=True, exist_ok=True)
    with file_lock(lock_file, timeout_seconds):
        local_path.mkdir(parents=True, exist_ok=True)
        if is_s3:
            sync_from_s3(source_path, local_path)
        else:
            sync_from_local(source_path, local_path)
        logger.info(f"Database available at: {local_path}")
        return local_path

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
        local_path = download_database(args.source_path, args.timeout_seconds)
        print(local_path)  # Output path for shell capture
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Total time elapsed: {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()
