#!/usr/bin/env python3
"""Extract per-sample output file suffixes from pyproject.toml.

Reads expected-outputs-run entries, finds {SAMPLE} placeholders, and prints the
suffix (the part after '{SAMPLE}_') with .gz stripped, one per line.
"""

import argparse
import sys
import tomllib
from pathlib import Path


def get_run_output_suffixes(
    pyproject_path: Path, platform: str = "illumina"
) -> list[str]:
    """Extract per-sample output suffixes from pyproject.toml.

    Looks for patterns like 'results/{SAMPLE}_virus_hits.tsv.gz' and extracts
    'virus_hits.tsv' as the suffix (stripping .gz if present).

    When platform is "illumina", also includes suffixes from the
    expected-outputs-run-shortread-extra key.

    Args:
        pyproject_path: Path to pyproject.toml file.
        platform: Platform name ("illumina" or "ont").

    Returns:
        Sorted list of unique suffixes.
    """
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    mgs_config = data.get("tool", {}).get("mgs-workflow", {})
    suffixes: set[str] = set()
    keys = ["expected-outputs-run"]
    if platform == "illumina":
        keys.append("expected-outputs-run-shortread-extra")
    for key in keys:
        for pattern in mgs_config.get(key, []):
            if "{SAMPLE}" not in pattern:
                continue
            parts = pattern.split("{SAMPLE}_", 1)
            if len(parts) != 2:
                continue
            suffix = parts[1]
            if suffix.endswith(".gz"):
                suffix = suffix[:-3]
            suffixes.add(suffix)
    return sorted(suffixes)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pyproject_path", type=Path, help="Path to pyproject.toml")
    parser.add_argument(
        "--platform",
        choices=["illumina", "ont"],
        default="illumina",
        help="Platform to determine which extra outputs to include (default: illumina)",
    )
    args = parser.parse_args()
    if not args.pyproject_path.exists():
        print(f"Error: {args.pyproject_path} not found", file=sys.stderr)
        sys.exit(1)
    for suffix in get_run_output_suffixes(args.pyproject_path, args.platform):
        print(suffix)


if __name__ == "__main__":
    main()
