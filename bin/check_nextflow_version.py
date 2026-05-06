#!/usr/bin/env python3
"""
Check the pinned Nextflow version against the highest non-ignored upstream
release.
"""

#=============================================================================
# Imports
#=============================================================================

import argparse
import json
import re
import sys
import urllib.request
from datetime import date
from pathlib import Path

from packaging.version import Version

#=============================================================================
# Constants
#=============================================================================

REPO_ROOT = Path(__file__).parent.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "configs" / "profiles.config"
DEFAULT_IGNORE_PATH = REPO_ROOT / ".nextflowignore"
DEFAULT_RELEASES_URL = (
    "https://api.github.com/repos/nextflow-io/nextflow/releases"
)

# Pattern to extract version from config: nextflowVersion = '!>=25.10.0'
NEXTFLOW_VERSION_PATTERN = re.compile(
    r"nextflowVersion\s*=\s*['\"]!>=(\d+\.\d+\.\d+)['\"]",
)

# Pattern to validate semantic version (X.Y.Z)
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")

# One .nextflowignore entry: `<version>` or `<version> exp:YYYY-MM-DD`.
IGNORE_ENTRY_PATTERN = re.compile(
    r"^(?P<version>\d+\.\d+\.\d+)(?:\s+exp:(?P<exp>\d{4}-\d{2}-\d{2}))?\s*$",
)

#=============================================================================
# Helper functions
#=============================================================================

def validate_semver(version: str, source: str) -> str:
    """
    Validate that a version string matches X.Y.Z semantic versioning.

    Args:
        version (str): The version string to validate.
        source (str): The source of the version string (for error messages).

    Returns:
        str: The validated version string.
    """
    if not SEMVER_PATTERN.match(version):
        raise ValueError(
            f"Invalid version format from {source}: {version!r}. "
            "Expected semantic version: X.Y.Z",
        )
    return version

def get_pinned_version(config_path: Path) -> str:
    """
    Extract the pinned Nextflow version from a Nextflow config file.

    Args:
        config_path (Path): Path to the config file (e.g. profiles.config).

    Returns:
        str: The pinned Nextflow version (X.Y.Z).
    """
    content = config_path.read_text()
    match = NEXTFLOW_VERSION_PATTERN.search(content)
    if not match:
        raise ValueError(
            f"Could not find nextflowVersion in {config_path}. "
            "Expected format: nextflowVersion = '!>=X.Y.Z'",
        )
    return validate_semver(match.group(1), str(config_path))

def parse_nextflowignore(
    ignore_path: Path,
    today: date | None = None,
) -> set[str]:
    """
    Parse a .nextflowignore file and return the active ignore set.

    Each non-comment, non-blank line must match `<X.Y.Z>` optionally followed
    by ` exp:YYYY-MM-DD`. Entries with an expiration in the past are dropped
    (with a warning to stderr) so stale ignores cannot accumulate silently.

    Args:
        ignore_path (Path): Path to the .nextflowignore file. A missing file
            is treated as an empty ignore set.
        today (date | None): Reference date for expiration checks; defaults to
            the current date. Injected for testability.

    Returns:
        set[str]: Currently active ignored version strings (X.Y.Z).
    """
    if not ignore_path.exists():
        return set()
    today = today if today is not None else date.today()
    active: set[str] = set()
    for line_number, raw in enumerate(ignore_path.read_text().splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        match = IGNORE_ENTRY_PATTERN.match(line)
        if not match:
            raise ValueError(
                f"Malformed entry in {ignore_path}:{line_number}: {line!r}. "
                "Expected '<X.Y.Z>' or '<X.Y.Z> exp:YYYY-MM-DD'.",
            )
        version = match.group("version")
        exp_str = match.group("exp")
        if exp_str is None:
            active.add(version)
            continue
        try:
            exp_date = date.fromisoformat(exp_str)
        except ValueError as err:
            raise ValueError(
                f"Invalid expiration date in {ignore_path}:{line_number}: "
                f"{exp_str!r}.",
            ) from err
        if exp_date < today:
            print(
                f"WARNING: ignore for Nextflow {version} expired on "
                f"{exp_date}; treating as unignored. "
                f"Remove or extend {ignore_path}:{line_number}.",
                file=sys.stderr,
            )
            continue
        active.add(version)
    return active

def fetch_releases(api_url: str) -> list[str]:
    """
    Fetch published, non-draft, non-prerelease Nextflow releases from GitHub.

    Args:
        api_url (str): GitHub `/releases` endpoint to query.

    Returns:
        list[str]: Release version strings (X.Y.Z), in API order.
    """
    request = urllib.request.Request(
        api_url,
        headers={"Accept": "application/vnd.github.v3+json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        releases = json.loads(response.read().decode())
    versions: list[str] = []
    for release in releases:
        if release.get("prerelease") or release.get("draft"):
            continue
        tag = release["tag_name"].removeprefix("v")
        if SEMVER_PATTERN.match(tag):
            versions.append(tag)
    return versions

def select_target_version(releases: list[str], ignored: set[str]) -> str:
    """
    Pick the highest-semver release that is not in the ignore set.

    Args:
        releases (list[str]): Candidate release version strings.
        ignored (set[str]): Currently active ignored version strings.

    Returns:
        str: The chosen target version.
    """
    eligible = [v for v in releases if v not in ignored]
    if not eligible:
        if not releases:
            raise ValueError("No Nextflow release candidates supplied.")
        raise ValueError(
            f"No eligible Nextflow release: all {len(releases)} candidates "
            f"are in the ignore set ({sorted(ignored)}).",
        )
    return max(eligible, key=Version)

def check_pinned_against_target(pinned: str, target: str) -> None:
    """
    Verify that the pinned version equals the target.

    Strict equality is intentional: if `pinned` differs from the highest
    non-ignored release we either need to bump the pin or add a justification
    to .nextflowignore. A mismatch where `pinned` is *higher* than `target`
    typically indicates a stale ignore entry for the version we are pinned to.

    Args:
        pinned (str): The pinned Nextflow version from profiles.config.
        target (str): The selected target version (highest non-ignored).
    """
    if pinned != target:
        raise ValueError(
            f"Version mismatch: pinned {pinned} != target {target}. "
            "Bump configs/profiles.config to match, or add an entry to "
            ".nextflowignore with a justification.",
        )

def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--ignore-file", type=Path, default=DEFAULT_IGNORE_PATH)
    parser.add_argument(
        "--releases-url",
        default=DEFAULT_RELEASES_URL,
        help="Nextflow releases endpoint (override is intended for tests; "
        "production runs should use the default).",
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
    pinned = get_pinned_version(args.config)
    print(f"Pinned Nextflow version: {pinned}")

    ignored = parse_nextflowignore(args.ignore_file)
    if ignored:
        print(f"Ignored versions (active): {sorted(ignored)}")

    releases = fetch_releases(args.releases_url)
    target = select_target_version(releases, ignored)
    print(f"Target Nextflow version: {target}")

    check_pinned_against_target(pinned, target)
    print("OK: Pinned version is current")

if __name__ == "__main__":
    main()
