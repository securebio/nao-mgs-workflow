#!/usr/bin/env python3

"""
Extract version information from pyproject.toml files and output as environment variables.
"""

import argparse
from dataclasses import dataclass
from typing import Any

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]


@dataclass
class VersionInfo:
    """Version information extracted from a pyproject.toml file."""

    version: str
    min_index_version: str | None = None
    min_pipeline_version: str | None = None


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract version information from pyproject.toml files"
    )
    parser.add_argument(
        "pipeline_pyproject",
        help="Path to pipeline pyproject.toml file",
    )
    parser.add_argument(
        "index_pyproject",
        help="Path to index pyproject.toml file",
    )
    parser.add_argument(
        "--tool-name",
        default="mgs-workflow",
        help=(
            "Name of the [tool.<name>] table holding the compatibility fields "
            "(default: mgs-workflow). Set this for pipelines that vendor this "
            "subworkflow but store their config under a different tool table."
        ),
    )
    return parser.parse_args()


def read_toml(path: str) -> dict[str, Any]:
    """Read and parse a TOML file."""
    with open(path, "rb") as f:
        return tomllib.load(f)


def get_nested_value(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Safely get a nested value from a dictionary.

    Args:
        data: Dictionary to traverse
        *keys: Keys to traverse in order
        default: Default value if key path doesn't exist

    Returns:
        The value at the key path, or default if not found
    """
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def extract_version_info(
    toml_data: dict[str, Any], tool_name: str = "mgs-workflow"
) -> VersionInfo:
    """
    Extract version information from parsed TOML data.

    Args:
        toml_data: Parsed TOML data as a dictionary
        tool_name: Name of the [tool.<tool_name>] table holding the
            compatibility fields

    Returns:
        VersionInfo with extracted version data
    """
    version = toml_data["project"]["version"]
    min_index_version = get_nested_value(
        toml_data, "tool", tool_name, "pipeline-min-index-version"
    )
    min_pipeline_version = get_nested_value(
        toml_data, "tool", tool_name, "index-min-pipeline-version"
    )
    return VersionInfo(
        version=version,
        min_index_version=min_index_version,
        min_pipeline_version=min_pipeline_version,
    )


def extract_versions(
    pipeline_path: str, index_path: str, tool_name: str = "mgs-workflow"
) -> None:
    """
    Extract version information from pyproject.toml files and print as
    shell variable assignments.

    Args:
        pipeline_path: Path to pipeline pyproject.toml
        index_path: Path to index pyproject.toml
        tool_name: Name of the [tool.<tool_name>] table holding the
            compatibility fields
    """
    pipeline_info = extract_version_info(read_toml(pipeline_path), tool_name)
    index_info = extract_version_info(read_toml(index_path), tool_name)

    # Output as shell variable assignments
    print(f"PIPELINE_VERSION={pipeline_info.version}")
    print(f"INDEX_VERSION={index_info.version}")
    print(f"PIPELINE_MIN_INDEX={pipeline_info.min_index_version or ''}")
    print(f"INDEX_MIN_PIPELINE={index_info.min_pipeline_version or ''}")


def main() -> None:
    """Main entry point."""
    args = parse_args()
    extract_versions(args.pipeline_pyproject, args.index_pyproject, args.tool_name)


if __name__ == "__main__":
    main()
