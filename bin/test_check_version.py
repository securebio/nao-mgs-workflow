#!/usr/bin/env python3
"""Unit tests for check_version.py"""

import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))
from check_version import (
    validate_version,
    get_pyproject_version,
    get_changelog_version,
)


class TestValidateVersion:
    @pytest.mark.parametrize("version", [
        "1.2.3.4",
        "0.0.0.0",
        "10.20.30.40",
        "1.2.3.4-dev",
    ])
    def test_valid(self, version):
        assert validate_version(version, "test") == version

    @pytest.mark.parametrize("version", [
        "",
        "1.2.3",
        "1.2.3.4.5",
        "v1.2.3.4",
        "1.2.3.4-beta",
        "a.b.c.d",
    ])
    def test_invalid(self, version):
        with pytest.raises(ValueError, match="Invalid version format"):
            validate_version(version, "test")

    def test_error_includes_source(self):
        with pytest.raises(ValueError, match="my_file.txt"):
            validate_version("bad", "my_file.txt")


class TestGetPyprojectVersion:
    @pytest.mark.parametrize("version", ["1.2.3.4", "1.2.3.4-dev"])
    def test_valid(self, tmp_path, version):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(f'[project]\nversion = "{version}"\n')
        assert get_pyproject_version(str(pyproject)) == version

    @pytest.mark.parametrize("content,error", [
        ('[project]\nname = "test"\n', KeyError),
        ('[tool]\nname = "test"\n', KeyError),
        ('[project]\nversion = "1.2.3"\n', ValueError),
    ])
    def test_invalid(self, tmp_path, content, error):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(content)
        with pytest.raises(error):
            get_pyproject_version(str(pyproject))


class TestGetChangelogVersion:
    @pytest.mark.parametrize("version", ["1.2.3.4", "1.2.3.4-dev"])
    def test_valid(self, tmp_path, version):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(f"# v{version}\n\n## Changes\n")
        assert get_changelog_version(str(changelog)) == version

    @pytest.mark.parametrize("content,match", [
        ("# 1.2.3.4\n", "must start with '# v'"),
        ("v1.2.3.4\n", "must start with '# v'"),
        ("", "must start with '# v'"),
        ("# v1.2.3\n", "Invalid version format"),
    ])
    def test_invalid(self, tmp_path, content, match):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(content)
        with pytest.raises(ValueError, match=match):
            get_changelog_version(str(changelog))
