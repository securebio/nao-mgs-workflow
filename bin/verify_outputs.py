#!/usr/bin/env python3
"""
Verify pipeline outputs match expected outputs from pyproject.toml.

This script checks that:
1. All expected output files are present
2. No unexpected files are present (except explicitly excluded patterns)
"""

#=============================================================================
# Imports
#=============================================================================

# Standard library imports
import argparse
import csv
import fnmatch
import logging
import tempfile
import tomllib
import time
from datetime import UTC, datetime
from pathlib import Path

# Third-party imports
import boto3

#=============================================================================
# Logging
#=============================================================================

class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#=============================================================================
# Constants
#=============================================================================

DEFAULT_PYPROJECT_PATH = Path(__file__).parent.parent / "pyproject.toml"

# Patterns to exclude from verification (matched against relative paths)
EXCLUDED_PATTERNS = [
    "logging/trace*",
    "logging_downstream/trace*",
]

#=============================================================================
# File listing helpers
#=============================================================================

def list_local_files(local_path: str) -> set[str]:
    """
    List all files under a local directory path.
    Args:
        local_path (str): Local directory path
    Returns:
        set[str]: Set of relative paths (relative to local_path)
    """
    base = Path(local_path)
    if not base.is_dir():
        raise ValueError(f"Directory does not exist: {local_path}")
    files = set()
    for file_path in base.rglob("*"):
        if file_path.is_file():
            files.add(str(file_path.relative_to(base)))
    return files

def list_s3_files(s3_path: str) -> set[str]:
    """
    List all files under an S3 path.
    Args:
        s3_path (str): S3 URI (s3://bucket/prefix)
    Returns:
        set[str]: Set of relative paths (relative to s3_path)
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    path_without_scheme = s3_path.removeprefix("s3://")
    parts = path_without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") + "/" if len(parts) > 1 else ""
    s3_client = boto3.client("s3")
    files = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.startswith(prefix):
                relative_path = key.removeprefix(prefix)
                if relative_path:
                    files.add(relative_path)
    return files

def list_files(path: str) -> set[str]:
    """
    List all files under a path (S3 or local).
    Args:
        path (str): S3 URI or local directory path
    Returns:
        set[str]: Set of relative paths
    """
    if path.startswith("s3://"):
        return list_s3_files(path)
    return list_local_files(path)

def download_s3_file(s3_path: str, local_path: Path) -> None:
    """
    Download a file from S3.
    Args:
        s3_path (str): S3 URI to download
        local_path (Path): Local path to save file
    """
    path_without_scheme = s3_path.removeprefix("s3://")
    parts = path_without_scheme.split("/", 1)
    bucket = parts[0]
    key = parts[1]
    s3_client = boto3.client("s3")
    s3_client.download_file(bucket, key, str(local_path))

#=============================================================================
# Config helpers
#=============================================================================

def get_expected_outputs(pyproject_path: Path, workflow: str) -> list[str]:
    """
    Read expected outputs from pyproject.toml for a specific workflow.
    Args:
        pyproject_path (Path): Path to pyproject.toml
        workflow (str): Workflow name ('run', 'downstream', or 'downstream-ont')
    Returns:
        list[str]: List of expected output paths (may contain {GROUP} placeholder)
    """
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    key = f"expected-outputs-{workflow}"
    mgs_config = data.get("tool", {}).get("mgs-workflow", {})
    if key not in mgs_config:
        raise ValueError(
            f"Missing '{key}' in [tool.mgs-workflow] section of {pyproject_path}"
        )
    return mgs_config[key]

def parse_groups_from_file(groups_file: str) -> list[str]:
    """
    Extract unique groups from a groups TSV (uses 'group' column) or samplesheet (uses 'sample' column).
    Args:
        groups_file (str): Local path to groups TSV or samplesheet CSV
    Returns:
        list[str]: List of unique group names
    """
    # Detect delimiter from file extension
    delimiter = "\t" if groups_file.endswith(".tsv") else ","
    with open(groups_file) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if "group" in reader.fieldnames:
            column = "group"
        elif "sample" in reader.fieldnames:
            column = "sample"
        else:
            raise ValueError(
                f"File must have 'group' or 'sample' column: {groups_file}"
            )
        groups = {row[column] for row in reader}
    return sorted(groups)

def resolve_groups(groups_file: str) -> list[str]:
    """
    Resolve groups file path and extract groups.
    Args:
        groups_file (str): Path to groups TSV or samplesheet (local or S3)
    Returns:
        list[str]: List of group names
    """
    if groups_file.startswith("s3://"):
        suffix = ".tsv" if groups_file.endswith(".tsv") else ".csv"
        with tempfile.NamedTemporaryFile(suffix=suffix) as tmp:
            download_s3_file(groups_file, Path(tmp.name))
            return parse_groups_from_file(tmp.name)
    return parse_groups_from_file(groups_file)

#=============================================================================
# Verification helpers
#=============================================================================

def expand_group_placeholder(patterns: list[str], groups: list[str]) -> set[str]:
    """
    Expand {GROUP} placeholders in patterns.
    Args:
        patterns (list[str]): List of patterns that may contain {GROUP}
        groups (list[str]): List of group names to expand
    Returns:
        set[str]: Set of expanded patterns with {GROUP} replaced
    """
    expanded = set()
    for pattern in patterns:
        if "{GROUP}" in pattern:
            for group in groups:
                expanded.add(pattern.replace("{GROUP}", group))
        else:
            expanded.add(pattern)
    return expanded

def is_excluded(path: str, excluded_patterns: list[str]) -> bool:
    """
    Check if a path matches any exclusion pattern.
    Args:
        path (str): Relative file path
        excluded_patterns (list[str]): List of glob patterns to exclude
    Returns:
        bool: True if path should be excluded
    """
    return any(fnmatch.fnmatch(path, pattern) for pattern in excluded_patterns)

def compare_outputs(
    expected: set[str],
    actual: set[str],
    excluded_patterns: list[str],
) -> tuple[set[str], set[str]]:
    """
    Compare expected vs actual outputs.
    Args:
        expected (set[str]): Set of expected file paths
        actual (set[str]): Set of actual file paths
        excluded_patterns (list[str]): Patterns to exclude from unexpected file detection
    Returns:
        tuple[set[str], set[str]]: Tuple of (missing files, unexpected files)
    """
    missing = expected - actual
    unexpected_candidates = actual - expected
    unexpected = {
        path
        for path in unexpected_candidates
        if not is_excluded(path, excluded_patterns)
    }
    return missing, unexpected

def report_verification(
    workflow_name: str,
    missing: set[str],
    unexpected: set[str],
) -> None:
    """
    Report verification results and raise if failed.
    Args:
        workflow_name (str): Name for logging (e.g., "RUN", "DOWNSTREAM")
        missing (set[str]): Set of missing file paths
        unexpected (set[str]): Set of unexpected file paths
    """
    if not missing and not unexpected:
        logger.info(f"{workflow_name} outputs: OK")
        return
    msg = (
        f"Expected output verification failed for {workflow_name}. "
        f"{len(missing)} missing files, {len(unexpected)} unexpected files"
    )
    logger.error(msg)
    if missing:
        logger.error("Missing files:")
        for path in sorted(missing):
            logger.error(f"  - {path}")
    if unexpected:
        logger.error("Unexpected files:")
        for path in sorted(unexpected):
            logger.error(f"  - {path}")
    raise ValueError(msg)

def verify_outputs(
    workflow_name: str,
    output_dir: str,
    expected_patterns: list[str],
    excluded_patterns: list[str],
    groups: list[str] | None,
) -> None:
    """
    Verify outputs for a single workflow.
    Args:
        workflow_name (str): Name for logging (e.g., "RUN", "DOWNSTREAM")
        output_dir (str): Path to output directory (local or S3)
        expected_patterns (list[str]): Expected output patterns (may contain {GROUP})
        excluded_patterns (list[str]): Patterns to exclude from unexpected file detection
        groups (list[str] | None): Group names for placeholder expansion (None to skip)
    Raises:
        ValueError: If verification fails (missing or unexpected files)
    """
    logger.info(f"Verifying {workflow_name} outputs: {output_dir}")
    if groups is not None:
        expected = expand_group_placeholder(expected_patterns, groups)
    else:
        expected = set(expected_patterns)
    actual = list_files(output_dir)
    logger.info(f"Expected: {len(expected)} files, Actual: {len(actual)} files")
    missing, unexpected = compare_outputs(expected, actual, excluded_patterns)
    report_verification(workflow_name, missing, unexpected)

#=============================================================================
# Argument parsing
#=============================================================================

def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Path to output directory to verify (local or S3)",
    )
    parser.add_argument(
        "--expected-outputs-key",
        type=str,
        required=True,
        choices=["run", "downstream", "downstream-ont"],
        help="Key in pyproject.toml specifying expected outputs",
    )
    parser.add_argument(
        "--groups",
        type=str,
        default=None,
        help="Optional: path to groups TSV or samplesheet (local or S3) for {GROUP} expansion",
    )
    parser.add_argument(
        "--pyproject",
        type=Path,
        default=DEFAULT_PYPROJECT_PATH,
        help=f"Path to pyproject.toml (default: {DEFAULT_PYPROJECT_PATH})",
    )
    parser.add_argument(
        "--exclude",
        type=str,
        action="append",
        default=[],
        help=f"Additional exclusion patterns (can be repeated). "
             f"Default exclusions: {EXCLUDED_PATTERNS}",
    )
    return parser.parse_args()

#=============================================================================
# Main function
#=============================================================================

def main() -> None:
    """
    Main function.
    """
    logger.info("Starting output verification")
    start_time = time.time()
    args = parse_args()
    logger.info(f"Parsed arguments: {args}")
    excluded_patterns = EXCLUDED_PATTERNS + args.exclude
    logger.info(f"Excluded patterns: {excluded_patterns}")
    if args.groups:
        groups = resolve_groups(args.groups)
        logger.info(f"Groups: {groups}")
    else:
        groups = None
    expected_patterns = get_expected_outputs(args.pyproject, args.expected_outputs_key)
    verify_outputs(
        args.expected_outputs_key,
        args.output_dir,
        expected_patterns,
        excluded_patterns,
        groups,
    )
    end_time = time.time()
    logger.info(f"Output verification completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
