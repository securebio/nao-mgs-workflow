#!/usr/bin/env python3
DESC = """
Combine per-sample JSON files into a single per-group JSON.

Reads per-sample JSON files (named {sample}_{suffix}), injects 'sample' and
'group' fields into each, and writes a combined JSON object keyed by sample
name.
"""

###########
# IMPORTS #
###########

import argparse
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

###########
# LOGGING #
###########


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

######################
# COMBINING FUNCTION #
######################


def extract_sample_name(filepath: Path, suffix: str) -> str:
    """Extract sample name from a filename by stripping the suffix.

    Args:
        filepath: Path to the input file.
        suffix: The file suffix to strip (e.g. "fastp.json").

    Returns:
        The sample name portion of the filename.

    Raises:
        ValueError: If the filename does not end with _{suffix}.
    """
    filename = filepath.name
    expected_ending = f"_{suffix}"
    if not filename.endswith(expected_ending):
        raise ValueError(
            f"Filename '{filename}' does not end with '{expected_ending}'"
        )
    return filename[: -len(expected_ending)]


def combine_sample_jsons(
    input_files: list[Path],
    group: str,
    suffix: str,
) -> dict:
    """Combine per-sample JSON files into a single dict keyed by sample name.

    Each sample entry has 'sample' and 'group' fields injected at the top
    level, alongside the original JSON content.

    Args:
        input_files: List of paths to per-sample JSON files.
        group: Group name to inject into each entry.
        suffix: File suffix used to extract sample names.

    Returns:
        Combined dict mapping sample names to their augmented JSON data.
    """
    combined: dict = {}
    for filepath in sorted(input_files):
        sample = extract_sample_name(filepath, suffix)
        if sample in combined:
            raise ValueError(
                f"Duplicate sample name '{sample}' from {filepath.name}"
            )
        with open(filepath) as f:
            data = json.load(f)
        data["sample"] = sample
        data["group"] = group
        combined[sample] = data
        logger.info(f"Loaded sample '{sample}' from {filepath.name}")
    return combined


#####################
# ARGUMENT PARSING  #
#####################


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--group",
        required=True,
        help="Group name to inject into each sample entry.",
    )
    parser.add_argument(
        "--suffix",
        required=True,
        help="File suffix to strip for sample name extraction (e.g. 'fastp.json').",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output path for the combined JSON file.",
    )
    parser.add_argument(
        "input_files",
        nargs="+",
        type=Path,
        help="Per-sample JSON input files.",
    )
    return parser.parse_args()


########
# MAIN #
########


def main() -> None:
    """Main entry point."""
    start_time = time.time()
    logger.info("Initializing script.")
    args = parse_arguments()
    logger.info(f"Arguments: group={args.group}, suffix={args.suffix}, "
                f"output={args.output}, input_files={len(args.input_files)} file(s)")
    combined = combine_sample_jsons(args.input_files, args.group, args.suffix)
    with open(args.output, "w") as f:
        json.dump(combined, f, indent=2)
    logger.info(f"Wrote {len(combined)} sample(s) to {args.output}")
    end_time = time.time()
    logger.info(f"Total time elapsed: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
