#!/usr/bin/env python3
DESC = """
Validate workflow output files against table-schema definitions.

This script finds output files in results*/ subdirectories that have
corresponding table-schemas in the schemas/ directory and validates them
using the frictionless library. Files without schemas are skipped.

Exit codes:
  0 - All validations passed (or no files to validate)
  1 - One or more validations failed
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import logging
import shutil
import sys
import tempfile
import time
import tomllib
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from frictionless import Dialect, Resource, formats, system, validate

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

def get_output_schema_names(pyproject_path: Path) -> set[str]:
    """
    Extract schema names from expected output patterns in pyproject.toml.
    Looks for patterns like '{GROUP}_duplicate_stats.tsv.gz' and extracts
    'duplicate_stats' as the schema name.
    Args:
        pyproject_path: Path to pyproject.toml file.
    Returns:
        Set of schema names found in expected output patterns.
    """
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    mgs_config = data.get("tool", {}).get("mgs-workflow", {})
    schema_names: set[str] = set()
    for key in mgs_config:
        if not key.startswith("expected-outputs"):
            continue
        for pattern in mgs_config[key]:
            if "{GROUP}" not in pattern:
                continue
            # Extract filename from path pattern
            filename = pattern.split("/")[-1]
            # Remove {GROUP}_ prefix and extensions to get schema name
            name = filename.replace("{GROUP}_", "")
            p = Path(name)
            while p.suffix:
                p = p.with_suffix("")
            schema_names.add(p.name)
    return schema_names

def find_schema_for_file(
    data_file: Path,
    schema_dir: Path,
    known_schema_names: set[str],
) -> Path | None:
    """
    Find the matching schema file for a data file, if one exists.
    Matches by checking if the filename ends with a known schema name pattern.
    Args:
        data_file: Path to the data file.
        schema_dir: Directory containing schema files.
        known_schema_names: Set of schema names from pyproject.toml.
    Returns:
        Path to the schema file, or None if not found.
    """
    # Strip extensions from filename
    p = Path(data_file.name)
    while p.suffix:
        p = p.with_suffix("")
    name = p.name
    # Check if filename ends with any known schema name
    for schema_name in known_schema_names:
        if name.endswith(f"_{schema_name}"):
            schema_path = schema_dir / f"{schema_name}.schema.json"
            if schema_path.exists():
                return schema_path
    return None

def find_data_files(output_dir: Path) -> list[Path]:
    """
    Find all files in results*/ subdirectories of the output directory.
    Args:
        output_dir: Base output directory to search (e.g., output/).
    Returns:
        List of paths to data files.
    """
    files = []
    for results_dir in output_dir.glob("results*"):
        if results_dir.is_dir():
            files.extend(f for f in results_dir.iterdir() if f.is_file())
    return sorted(files)

@contextmanager
def decompressed_path(data_file: Path) -> Generator[Path, None, None]:
    """
    Context manager that yields a path to an uncompressed version of the file.
    For gzipped files, decompresses to a temporary file that is cleaned up
    on exit. For uncompressed files, yields the original path.
    Args:
        data_file: Path to the data file.
    Yields:
        Path to the uncompressed file.
    """
    if not data_file.name.endswith(".gz"):
        yield data_file
        return
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=True) as tmp:
        with gzip.open(data_file, "rt") as f_in:
            shutil.copyfileobj(f_in, tmp)
        tmp.flush()
        yield Path(tmp.name)

def validate_file(data_file: Path, schema_path: Path) -> tuple[bool, list[str]]:
    """
    Validate a data file against a schema.
    Args:
        data_file: Path to the data file.
        schema_path: Path to the schema file.
    Returns:
        Tuple of (is_valid, list of error messages).
    """
    with decompressed_path(data_file) as file_to_validate:
        dialect = Dialect(controls=[formats.CsvControl(delimiter="\t")])
        resource = Resource(
            path=str(file_to_validate),
            schema=str(schema_path),
            dialect=dialect,
        )
        with system.use_context(trusted=True):
            report = validate(resource)
    if report.valid:
        return True, []
    errors = []
    for task in report.tasks:
        for error in task.errors:
            errors.append(error.message)
    return False, errors

##############
# MAIN LOGIC #
##############

def validate_outputs(
    output_dir: Path,
    schema_dir: Path,
    pyproject_path: Path,
) -> int:
    """
    Validate all output files that have matching schemas.
    Args:
        output_dir: Base output directory (searches results*/ subdirectories).
        schema_dir: Directory containing schema files.
        pyproject_path: Path to pyproject.toml for schema name lookup.
    Returns:
        Exit code (0 for success, 1 for failure).
    """
    # Validate required inputs
    if not output_dir.exists():
        logger.error(f"Output directory does not exist: {output_dir}")
        return 1
    if not schema_dir.exists():
        logger.error(f"Schema directory does not exist: {schema_dir}")
        return 1
    if not pyproject_path.exists():
        logger.error(f"pyproject.toml does not exist: {pyproject_path}")
        return 1
    known_schema_names = get_output_schema_names(pyproject_path)
    logger.info(f"Known schema names: {sorted(known_schema_names)}")
    data_files = find_data_files(output_dir)
    if not data_files:
        logger.error(f"No data files found in {output_dir}/results*/")
        return 1
    logger.info(f"Found {len(data_files)} data file(s) in {output_dir}/results*/")
    # Find files with matching schemas
    files_to_validate: list[tuple[Path, Path]] = []
    for data_file in data_files:
        schema_path = find_schema_for_file(data_file, schema_dir, known_schema_names)
        if schema_path:
            files_to_validate.append((data_file, schema_path))
            logger.info(f"  {data_file.name} -> {schema_path.name}")
        else:
            logger.debug(f"  {data_file.name} -> no schema found, skipping")
    if not files_to_validate:
        logger.info("No files with matching schemas found.")
        return 0
    # Validate files with matching schemas
    logger.info(f"Validating {len(files_to_validate)} file(s) with schemas...")
    all_passed = True
    for data_file, schema_path in files_to_validate:
        is_valid, errors = validate_file(data_file, schema_path)
        if is_valid:
            logger.info(f"  PASS: {data_file.name}")
        else:
            logger.error(f"  FAIL: {data_file.name}")
            for error in errors:
                logger.error(f"    - {error}")
            all_passed = False
    if all_passed:
        logger.info("All validations passed.")
        return 0
    else:
        logger.error("Some validations failed.")
        return 1

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        help="Base output directory containing results*/ subdirectories",
        required=True,
    )
    parser.add_argument(
        "-s", "--schema-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "schemas",
        help="Directory containing schema files (default: <repo>/schemas)",
    )
    parser.add_argument(
        "-p", "--pyproject",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "pyproject.toml",
        help="Path to pyproject.toml for schema name lookup (default: <repo>/pyproject.toml)",
    )
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    logger.info("Initializing script.")
    start_time = time.time()
    args = parse_arguments()
    logger.info(f"Arguments: {args}")
    exit_code = validate_outputs(args.output_dir, args.schema_dir, args.pyproject)
    end_time = time.time()
    logger.info(f"Total time elapsed: {end_time - start_time:.2f} seconds")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
