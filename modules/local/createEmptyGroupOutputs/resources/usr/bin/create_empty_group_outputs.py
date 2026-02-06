#!/usr/bin/env python3
"""
Create empty output files for groups with no virus hits.

Reads empty group names from a TSV file and creates empty gzipped files
for each expected per-group output defined in pyproject.toml. When a
table-schema exists for an output, the file includes a header row.
"""

#=============================================================================
# Imports
#=============================================================================

# Standard library imports
import argparse
import csv
import gzip
import io
import json
import logging
import time
import tomllib
from datetime import UTC, datetime
from pathlib import Path

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
# File I/O helpers
#=============================================================================

def open_by_suffix(filename: str | Path, mode: str = "r") -> io.TextIOWrapper:
    """
    Open a file using the appropriate method based on its suffix.
    Args:
        filename (str | Path): Path to file to open.
        mode (str): File open mode (default "r").
    Returns:
        io.TextIOWrapper: File handle appropriate for the file compression type.
    """
    filename_str = str(filename)
    if filename_str.endswith(".gz"):
        return gzip.open(filename_str, mode + "t")
    else:
        return open(filename_str, mode)

#=============================================================================
# Core functions
#=============================================================================

def get_unique_groups(tsv_path: str) -> set[str]:
    """
    Extract unique group names from a TSV file.
    Args:
        tsv_path (str): Path to gzipped TSV file with 'group' column.
    Returns:
        set[str]: Set of unique group names found in the file.
    """
    groups: set[str] = set()
    with open_by_suffix(tsv_path, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if "group" not in (reader.fieldnames or []):
            logger.warning(f"'group' column not found in {tsv_path}")
            return groups
        for row in reader:
            groups.add(row["group"])
    return groups

def get_group_output_patterns(pyproject_path: str, platform: str) -> list[str]:
    """
    Extract per-group output patterns from pyproject.toml.
    Args:
        pyproject_path (str): Path to pyproject.toml file.
        platform (str): Platform name ("illumina" or "ont").
    Returns:
        list[str]: List of filename patterns containing {GROUP} placeholder.
    """
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    key = "expected-outputs-downstream-ont" if platform == "ont" else "expected-outputs-downstream"
    outputs: list[str] = data.get("tool", {}).get("mgs-workflow", {}).get(key, [])
    patterns: list[str] = []
    for output in outputs:
        if "{GROUP}" in output:
            filename = output.split("/")[-1]
            patterns.append(filename)
    return patterns


def get_schema_name_from_pattern(pattern: str) -> str:
    """
    Extract the schema name from an output filename pattern.

    For a pattern like '{GROUP}_duplicate_stats.tsv.gz', returns 'duplicate_stats'.
    Args:
        pattern (str): Filename pattern with {GROUP} placeholder.
    Returns:
        str: The base name to use for schema lookup.
    """
    # Remove {GROUP}_ prefix
    name = pattern.replace("{GROUP}_", "")
    # Use Path to strip all extensions (e.g., .tsv.gz -> base name)
    p = Path(name)
    while p.suffix:
        p = p.with_suffix("")
    return p.name


def load_schema_headers(schema_dir: Path, schema_name: str) -> list[str] | None:
    """
    Load column headers from a table-schema file if it exists.
    Args:
        schema_dir (Path): Directory containing schema files.
        schema_name (str): Base name of the schema (e.g., 'duplicate_stats').
    Returns:
        list[str] | None: List of column names, or None if no schema found.
    """
    schema_path = schema_dir / f"{schema_name}.schema.json"
    if not schema_path.exists():
        return None
    with open(schema_path) as f:
        schema = json.load(f)
    fields = schema.get("fields", [])
    if not fields:
        return None
    return [field["name"] for field in fields]

def create_empty_outputs(
    groups: set[str],
    patterns: list[str],
    output_dir: str,
    schema_dir: Path | None = None,
) -> list[str]:
    """
    Create empty gzipped files for each group and pattern combination.

    When a table-schema exists for an output pattern, the file includes
    a header row with column names from the schema.
    Args:
        groups (set[str]): Set of group names to create outputs for.
        patterns (list[str]): List of filename patterns with {GROUP} placeholder.
        output_dir (str): Directory to write output files.
        schema_dir (Path | None): Directory containing table-schema files.
    Returns:
        list[str]: List of paths to created files.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    # Pre-load headers for each pattern
    pattern_headers: dict[str, list[str] | None] = {}
    for pattern in patterns:
        if schema_dir:
            schema_name = get_schema_name_from_pattern(pattern)
            headers = load_schema_headers(schema_dir, schema_name)
            if headers:
                logger.info(f"Found schema for {pattern}: {schema_name}.schema.json")
            pattern_headers[pattern] = headers
        else:
            pattern_headers[pattern] = None
    created_files: list[str] = []
    for group in sorted(groups):
        if ".." in group or "/" in group:
            raise ValueError(f"Invalid group name: '{group}' contains path traversal characters.")
        for pattern in patterns:
            filename = pattern.replace("{GROUP}", group)
            filepath = output_path / filename
            headers = pattern_headers[pattern]
            with open_by_suffix(filepath, "w") as f:
                if headers:
                    f.write("\t".join(headers) + "\n")
            created_files.append(str(filepath))
            logger.info(f"Created: {filepath}" + (" (with headers)" if headers else ""))
    return created_files

#=============================================================================
# Argument parsing
#=============================================================================

def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    Returns:
        argparse.Namespace: Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "empty_groups_tsv",
        help="Path to TSV file listing empty groups (must have 'group' column)",
    )
    parser.add_argument(
        "pyproject_toml",
        help="Path to pyproject.toml containing expected outputs",
    )
    parser.add_argument(
        "output_dir",
        help="Directory to write empty output files",
    )
    parser.add_argument(
        "--platform",
        choices=["illumina", "ont"],
        default="illumina",
        help="Platform to determine which expected outputs to use (default: illumina)",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=None,
        help="Directory containing table-schema files for generating headers",
    )
    return parser.parse_args()

#=============================================================================
# Main
#=============================================================================

def main() -> None:
    """Main entry point for the script."""
    start_time = time.time()
    logger.info("Initializing script.")
    args = parse_args()
    logger.info(f"Arguments: {args}")
    logger.info("Getting unique groups from empty groups TSV.")
    groups = get_unique_groups(args.empty_groups_tsv)
    if not groups:
        logger.info("No empty groups found, nothing to create")
        return
    logger.info(f"Found {len(groups)} empty groups: {sorted(groups)}")
    patterns = get_group_output_patterns(args.pyproject_toml, args.platform)
    if not patterns:
        logger.warning("No per-group output patterns found in pyproject.toml")
        return
    logger.info(f"Found {len(patterns)} per-group output patterns: {patterns}")
    created = create_empty_outputs(groups, patterns, args.output_dir, args.schema_dir)
    logger.info(f"Created {len(created)} empty output files")
    end_time = time.time()
    logger.info(f"Total time elapsed: {end_time - start_time} seconds")
    logger.info("Script completed successfully.")

if __name__ == "__main__":
    main()
