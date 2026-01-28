#!/usr/bin/env python3
"""Check version consistency between pyproject.toml and CHANGELOG.md.

Enforces:
1. For PRs to main: CHANGELOG.md must have # Unreleased section with valid bump_type
2. For PRs to dev: version in CHANGELOG.md header must match pyproject.toml (if versioned)
"""

import argparse
import re
import sys
import tomllib

VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+\.\d+$")
VALID_BUMP_TYPES = {"major", "schema", "results", "point"}


def validate_version(version: str, source: str) -> str:
    """Validate version string matches expected format.

    Returns the version if valid, raises ValueError if not.
    """
    if not VERSION_PATTERN.match(version):
        raise ValueError(
            f"Invalid version format in {source}: {version!r}. "
            "Expected format: X.Y.Z.W",
        )
    return version


def get_pyproject_version(path: str = "pyproject.toml") -> str:
    """Extract version from pyproject.toml."""
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return validate_version(data["project"]["version"], path)


def get_changelog_version(path: str = "CHANGELOG.md") -> str | None:
    """Extract version from first version header in CHANGELOG.md.

    Returns the version string, or None if first line is # Unreleased.
    """
    with open(path) as f:
        first_line = f.readline().strip()
    # Check for Unreleased section
    if first_line == "# Unreleased":
        return None
    # Expected format: "# vX.Y.Z.W"
    prefix = "# v"
    if not first_line.startswith(prefix):
        raise ValueError(
            f"{path} first line must start with '{prefix}' or be '# Unreleased', "
            f"got: {first_line!r}",
        )
    version = first_line.removeprefix(prefix)
    return validate_version(version, path)


def validate_unreleased_section(path: str = "CHANGELOG.md") -> tuple[str, list[str]]:
    """Validate the # Unreleased section in CHANGELOG.md.

    Returns (bump_type, content_lines) if valid.
    Raises ValueError if validation fails.
    """
    with open(path) as f:
        lines = f.readlines()
    if not lines or lines[0].strip() != "# Unreleased":
        raise ValueError(f"{path} must start with '# Unreleased'")
    # Find bump_type directive
    bump_type = None
    content_lines = []
    for line in lines[1:]:
        stripped = line.strip()
        # Check if we've hit the next version header
        if stripped.startswith("# v"):
            break
        # Look for bump_type directive
        if stripped.startswith("bump_type:"):
            bump_type = stripped.split(":", 1)[1].strip()
            continue
        # Collect non-empty content lines
        if stripped and not stripped.startswith("#"):
            content_lines.append(stripped)
    # Validate bump_type
    if bump_type is None:
        raise ValueError(
            f"{path}: No 'bump_type:' directive found after '# Unreleased'. "
            "Add 'bump_type: point' (or major/schema/results)",
        )
    if bump_type not in VALID_BUMP_TYPES:
        raise ValueError(
            f"{path}: Invalid bump_type '{bump_type}'. "
            f"Must be one of: {', '.join(sorted(VALID_BUMP_TYPES))}",
        )
    # Validate content exists
    if not content_lines:
        raise ValueError(
            f"{path}: '# Unreleased' section has no content. "
            "Add changelog entries before merging to main",
        )
    return bump_type, content_lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-branch", help="PR base branch (e.g., dev, main)")
    parser.add_argument("--head-branch", help="PR head branch (e.g., feature/foo)")
    args = parser.parse_args()

    pyproject_version = get_pyproject_version()
    print(f"pyproject.toml version: {pyproject_version}")

    if args.base_branch:
        print(f"Base branch: {args.base_branch}")
        if args.head_branch:
            print(f"Head branch: {args.head_branch}")

        # For PRs to main, validate Unreleased section
        if args.base_branch == "main":
            try:
                bump_type, content_lines = validate_unreleased_section()
                print("CHANGELOG.md: Found valid '# Unreleased' section")
                print(f"bump_type: {bump_type}")
                print(f"Content entries: {len(content_lines)}")
            except ValueError as e:
                print(f"ERROR: {e}", file=sys.stderr)
                return 1
        else:
            # For other PRs, check version in CHANGELOG header matches pyproject.toml
            changelog_version = get_changelog_version()
            if changelog_version is None:
                print("CHANGELOG.md: Found '# Unreleased' section (OK for dev)")
            else:
                print(f"CHANGELOG.md version: {changelog_version}")
                if pyproject_version != changelog_version:
                    print(
                        "ERROR: Version mismatch between pyproject.toml and CHANGELOG.md",
                        file=sys.stderr,
                    )
                    return 1
                print("OK: Versions match")

    return 0


if __name__ == "__main__":
    sys.exit(main())
