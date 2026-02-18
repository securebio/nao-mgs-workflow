#!/usr/bin/env python3
"""Extract per-sample output file suffixes from pyproject.toml.

Reads expected-outputs-run entries, finds {SAMPLE} placeholders, and prints the
suffix (the part after '{SAMPLE}_') with .gz stripped, one per line.
"""

import argparse
import sys
import tomllib
from pathlib import Path


def get_run_output_suffixes(pyproject_path: Path) -> list[str]:
    """Extract per-sample output suffixes from pyproject.toml.

    Looks for patterns like 'results/{SAMPLE}_virus_hits.tsv.gz' and extracts
    'virus_hits.tsv' as the suffix (stripping .gz if present).

    Args:
        pyproject_path: Path to pyproject.toml file.

    Returns:
        Sorted list of unique suffixes.
    """
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    mgs_config = data.get("tool", {}).get("mgs-workflow", {})
    suffixes: set[str] = set()
    for key in mgs_config:
        if not key.startswith("expected-outputs"):
            continue
        for pattern in mgs_config[key]:
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
    args = parser.parse_args()
    if not args.pyproject_path.exists():
        print(f"Error: {args.pyproject_path} not found", file=sys.stderr)
        sys.exit(1)
    for suffix in get_run_output_suffixes(args.pyproject_path):
        print(suffix)


if __name__ == "__main__":
    main()
