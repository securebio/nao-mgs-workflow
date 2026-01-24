#!/usr/bin/env python3
"""
Check that the pinned Nextflow version is the latest release by
comparing against the latest release from GitHub.
"""

#=============================================================================
# Imports
#=============================================================================

import argparse
import json
import re
import urllib.request
from pathlib import Path

#=============================================================================
# Constants
#=============================================================================

DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "configs" / "profiles.config"
DEFAULT_GITHUB_API_URL = (
    "https://api.github.com/repos/nextflow-io/nextflow/releases/latest"
)
GITHUB_RELEASES_URL = (
    "https://api.github.com/repos/nextflow-io/nextflow/releases"
)

# Known broken Nextflow versions that should be excluded from version checks.
# Add versions here when they have critical bugs that affect the pipeline.
EXCLUDED_VERSIONS = {
    "25.10.3",  # AWS Batch job submission issues
}

# Pattern to extract version from config: nextflowVersion = '!>=25.10.0'
NEXTFLOW_VERSION_PATTERN = re.compile(
    r"nextflowVersion\s*=\s*['\"]!>=(\d+\.\d+\.\d+)['\"]"
)

# Pattern to validate semantic version (X.Y.Z)
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")

#=============================================================================
# Helper functions
#=============================================================================

def validate_semver(version: str, source: str) -> str:
    """
    Validate that a version string matches semantic versioning format.

    Args:
        version (str): The version string to validate.
        source (str): The source of the version string.

    Returns:
        str: The validated version string.
    """
    if not SEMVER_PATTERN.match(version):
        raise ValueError(
            f"Invalid version format from {source}: {version!r}. "
            "Expected semantic version: X.Y.Z"
        )
    return version

def get_pinned_version(config_path: Path) -> str:
    """
    Extract the pinned Nextflow version from a config file.

    Args:
        config_path (Path): The path to the config file.

    Returns:
        str: The pinned Nextflow version.
    """
    content = config_path.read_text()
    match = NEXTFLOW_VERSION_PATTERN.search(content)
    if not match:
        raise ValueError(
            f"Could not find nextflowVersion in {config_path}. "
            "Expected format: nextflowVersion = '!>=X.Y.Z'"
        )
    return validate_semver(match.group(1), str(config_path))

def get_latest_version(api_url: str) -> str:
    """
    Fetch the latest Nextflow release version from GitHub API.

    Args:
        api_url (str): The GitHub API URL to fetch the latest release.

    Returns:
        str: The latest Nextflow release version.
    """
    request = urllib.request.Request(
        api_url,
        headers={"Accept": "application/vnd.github.v3+json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode())
    tag = data["tag_name"].removeprefix("v")
    return validate_semver(tag, api_url)


def get_latest_non_excluded_version(excluded: set[str]) -> str:
    """
    Fetch the latest Nextflow release version that is not in the excluded set.

    Args:
        excluded (set[str]): Set of version strings to exclude.

    Returns:
        str: The latest non-excluded Nextflow release version.
    """
    request = urllib.request.Request(
        GITHUB_RELEASES_URL,
        headers={"Accept": "application/vnd.github.v3+json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        releases = json.loads(response.read().decode())

    for release in releases:
        if release.get("prerelease") or release.get("draft"):
            continue
        tag = release["tag_name"].removeprefix("v")
        try:
            version = validate_semver(tag, GITHUB_RELEASES_URL)
        except ValueError:
            continue
        if version not in excluded:
            return version

    raise ValueError(
        f"Could not find any non-excluded release. Excluded: {excluded}"
    )

def compare_versions(version1: str, version2: str) -> None:
    """
    Compare two version strings, raising an error if they
    do not match.

    Args:
        version1 (str): The first version string to compare.
        version2 (str): The second version string to compare.
    """
    if version1 != version2:
        raise ValueError(
            f"Version mismatch: {version1} != {version2}"
        )

def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--api-url", default=DEFAULT_GITHUB_API_URL)
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
    latest = get_latest_version(args.api_url)
    print(f"Latest Nextflow version: {latest}")

    if latest in EXCLUDED_VERSIONS:
        print(f"Latest version {latest} is excluded (known issues)")
        target = get_latest_non_excluded_version(EXCLUDED_VERSIONS)
        print(f"Latest non-excluded version: {target}")
    else:
        target = latest

    compare_versions(pinned, target)
    print("OK: Pinned version matches target release")

if __name__ == "__main__":
    main()
