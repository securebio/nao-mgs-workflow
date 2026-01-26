#!/usr/bin/env python3
"""Extract changelog section for a specific version from CHANGELOG.md."""

#=============================================================================
# Imports
#=============================================================================

import argparse
import re
import sys
from pathlib import Path

#=============================================================================
# Constants
#=============================================================================

DEFAULT_CHANGELOG_PATH = Path("CHANGELOG.md")

# Pattern to match version headers: "# vX.Y.Z.W" or "# vX.Y.Z.W-dev"
# Also tolerates no space after # (e.g., "#v1.2.3.4")
VERSION_HEADER_PATTERN = re.compile(r"^#\s*v?(\d+\.\d+\.\d+\.\d+(?:-dev)?)$")

#=============================================================================
# Helper functions
#=============================================================================

def parse_version_header(line: str) -> str | None:
    """
    Parse a line to check if it's a version header and extract the version.

    Args:
        line: Line to parse

    Returns:
        Version string if the line is a valid version header, None otherwise
    """
    match = VERSION_HEADER_PATTERN.match(line.strip())
    if match:
        return match.group(1)
    return None


def extract_changelog(version: str, changelog_path: Path = DEFAULT_CHANGELOG_PATH) -> str:
    """
    Extract the changelog content for a specific version by streaming the file.

    Args:
        version: Version string (e.g., "3.0.1.7")
        changelog_path: Path to CHANGELOG.md file

    Returns:
        The changelog content for the specified version (non-empty lines only)

    Raises:
        ValueError: If version is not found in changelog
        FileNotFoundError: If CHANGELOG.md doesn't exist
    """
    if not changelog_path.exists():
        raise FileNotFoundError(f"Changelog file not found: {changelog_path}")

    content_lines = []
    found_version = False
    in_section = False

    with open(changelog_path) as f:
        for line in f:
            header_version = parse_version_header(line)

            if header_version is not None:
                if header_version == version:
                    # Found the target version header
                    found_version = True
                    in_section = True
                    continue  # Skip the header line itself
                elif in_section:
                    # Found the next version header, we're done
                    break

            if in_section:
                # Collect non-empty lines from the target version section
                if line.strip():
                    content_lines.append(line)

    if not found_version:
        raise ValueError(
            f"Version {version} not found in {changelog_path}. "
            f"Expected header format: '# v{version}' or '# {version}'"
        )

    return "".join(content_lines)


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 3.0.1.7
  %(prog)s 3.0.1.7 --changelog-path /path/to/CHANGELOG.md
        """,
    )

    parser.add_argument(
        "version",
        help="Version string to extract (e.g., 3.0.1.7)",
    )

    parser.add_argument(
        "--changelog-path",
        type=Path,
        default=DEFAULT_CHANGELOG_PATH,
        help=f"Path to CHANGELOG.md file (default: {DEFAULT_CHANGELOG_PATH})",
    )

    return parser.parse_args()

#=============================================================================
# Main function
#=============================================================================

def main() -> None:
    """
    Main function.
    """
    args = parse_args()

    try:
        content = extract_changelog(args.version, args.changelog_path)
        print(content, end="")
    except (ValueError, FileNotFoundError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
