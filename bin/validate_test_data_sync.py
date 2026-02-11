#!/usr/bin/env python3
DESC = """
Validate that md5 sums are in sync between test data files and workflow snapshots. Each
entry in a snapshot is matched to a subdirectory with the same name, and all files in
that subdirectory are validated against the expected MD5 sums in the snapshot. This helps
catch drift between workflow test outputs and committed test data.
"""

###########
# IMPORTS #
###########

import argparse
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import time

###########
# LOGGING #
###########

class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
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

def parse_snapshot(snapshot_path: Path) -> dict[str, dict[str, str]]:
    """
    Parse nf-test snapshot file and extract MD5 sums.
    Args:
        snapshot_path (Path): Path to the snapshot JSON file
    Returns:
        dict[str, dict[str, str]]: Dictionary mapping snapshot names to {filename: md5} dictionaries
    """
    results: dict[str, dict[str, str]] = {}
    with open(snapshot_path, "r") as f:
        snapshot = json.load(f)
    for snapshot_name, snapshot_data in snapshot.items():
        content = snapshot_data.get("content", [])
        md5_map: dict[str, str] = {}
        for entry in content:
            # Format: "filename.ext:md5,hash_value"
            # nf-test snapshots record gzipped filenames (e.g. foo.tsv.gz) but
            # committed test-data files are uncompressed (foo.tsv), so strip .gz
            if ":md5," in entry:
                filename, md5_part = entry.rsplit(":md5,", 1)
                if filename.endswith(".gz"):
                    filename = filename[:-3]
                md5_map[filename] = md5_part
        results[snapshot_name] = md5_map
    return results

def compute_md5(file_path: Path) -> str:
    """
    Compute MD5 hash of a file.
    Args:
        file_path (Path): Path to the file
    Returns:
        str: Hexadecimal MD5 hash string
    """
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def validate_snapshot(
    snapshot_path: Path,
    results_dir: Path,
) -> None:
    """
    Validate results files against a snapshot file. For each snapshot in the file,
    looks for a subdirectory in results_dir with the same name and validates that
    all files match their expected MD5 sums.
    Args:
        snapshot_path (Path): Path to the workflow snapshot file
        results_dir (Path): Path to test-data/results directory
    """
    logger.info(f"Parsing snapshot: {snapshot_path}")
    snapshot_data = parse_snapshot(snapshot_path)
    for snapshot_name, md5_map in snapshot_data.items():
        logger.info(f"Validating {snapshot_name}")
        snapshot_results_dir = results_dir / snapshot_name
        if not snapshot_results_dir.exists():
            msg = f"Snapshot directory not found: {snapshot_results_dir}"
            logger.error(msg)
            raise FileNotFoundError(msg)
        errors: list[str] = []
        for filename, expected_md5 in md5_map.items():
            file_path = snapshot_results_dir / filename
            if not file_path.exists():
                errors.append(f"File not found: {file_path}")
            else:
                actual_md5 = compute_md5(file_path)
                if actual_md5 != expected_md5:
                    errors.append(f"MD5 mismatch for {file_path}: {actual_md5} (actual) != {expected_md5} (expected)")
        if errors:
            logger.error(f"Errors found for {snapshot_name}:")
            for error in errors:
                logger.error(f"  {error}")
            raise ValueError(f"Errors found for {snapshot_name}")

def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        required=True,
        help="Path to workflow snapshot file (e.g., tests/workflows/run.nf.test.snap)",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("test-data/results"),
        help="Path to test data directory (default: test-data/results)",
    )
    return parser.parse_args()

#################
# MAIN FUNCTION #
#################

def main() -> None:
    """Main entry point for the script."""
    start_time = time.time()
    logger.info("Initializing script.")
    args = parse_arguments()
    logger.info(f"Arguments: {args}")
    logger.info(f"Validating {args.results_dir} against {args.snapshot}...")
    validate_snapshot(args.snapshot, args.results_dir)
    end_time = time.time()
    logger.info(f"Script completed successfully in {end_time - start_time} seconds.")

if __name__ == "__main__":
    main()
