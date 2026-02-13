#!/usr/bin/env python3
"""Check version consistency between pyproject.toml and CHANGELOG.md.

Enforces:
1. Versions in pyproject.toml and CHANGELOG.md must match
2. PRs to main must have -dev suffix (workflow strips it)
3. PRs to stable must not have -dev suffix
4. All PRs to dev must have -dev suffix
"""

import argparse
import re
import sys
import tomllib

VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+\.\d+(-dev)?$")


def validate_version(version: str, source: str) -> str:
    """Validate version string matches expected format.

    Returns the version if valid, raises ValueError if not.
    """
    if not VERSION_PATTERN.match(version):
        raise ValueError(
            f"Invalid version format in {source}: {version!r}. "
            "Expected format: X.Y.Z.W or X.Y.Z.W-dev",
        )
    return version


def get_pyproject_version(path: str = "pyproject.toml") -> str:
    """Extract version from pyproject.toml."""
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return validate_version(data["project"]["version"], path)


def get_changelog_version(path: str = "CHANGELOG.md") -> str:
    """Extract version from first line of CHANGELOG.md."""
    with open(path) as f:
        first_line = f.readline().strip()
    # Expected format: "# vX.Y.Z.W" or "# vX.Y.Z.W-dev"
    prefix = "# v"
    if not first_line.startswith(prefix):
        raise ValueError(
            f"{path} first line must start with '{prefix}', got: {first_line!r}",
        )
    version = first_line.removeprefix(prefix)
    return validate_version(version, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-branch", help="PR base branch (e.g., dev, main)")
    parser.add_argument("--head-branch", help="PR head branch (e.g., feature/foo)")
    args = parser.parse_args()

    pyproject_version = get_pyproject_version()
    changelog_version = get_changelog_version()

    print(f"pyproject.toml version: {pyproject_version}")
    print(f"CHANGELOG.md version:   {changelog_version}")

    # Check versions match
    if pyproject_version != changelog_version:
        print(
            "ERROR: Version mismatch between pyproject.toml and CHANGELOG.md",
            file=sys.stderr,
        )
        return 1
    print("OK: Versions match")

    # If branch info provided, check dev suffix rules
    if args.base_branch:
        is_dev_version = pyproject_version.endswith("-dev")

        print(f"Base branch: {args.base_branch}")
        print(f"Head branch: {args.head_branch}")

        # Rule 1: PRs to main MUST have -dev suffix (workflow strips it)
        if args.base_branch == "main":
            if not is_dev_version:
                print(
                    "ERROR: PRs to main must have -dev version suffix "
                    "(the bump-version workflow strips it on merge)",
                    file=sys.stderr,
                )
                return 1
            print("OK: Dev version suffix correct for PR to main")

        # Rule 2: PRs to stable must NOT have -dev suffix
        if args.base_branch == "stable":
            if is_dev_version:
                print(
                    "ERROR: PRs to stable must not have -dev version suffix",
                    file=sys.stderr,
                )
                return 1
            print("OK: Non-dev version correct for PR to stable")

        # Rule 3: All PRs to dev MUST have -dev suffix
        if args.base_branch == "dev":
            if not is_dev_version:
                print(
                    "ERROR: PRs to dev must have -dev version suffix",
                    file=sys.stderr,
                )
                return 1
            print("OK: Dev version suffix correct for PR to dev")

    return 0


if __name__ == "__main__":
    sys.exit(main())
