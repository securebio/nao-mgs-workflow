#!/usr/bin/env python3
"""Unit tests for check_version.py"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from check_version import (
    get_changelog_version,
    get_pyproject_version,
    validate_unreleased_section,
    validate_version,
)


class TestValidateVersion:
    @pytest.mark.parametrize("version", ["1.2.3.4", "0.0.0.0", "10.20.30.40"])
    def test_valid(self, version):
        assert validate_version(version, "test") == version

    @pytest.mark.parametrize("version", [
        "", "1.2.3", "1.2.3.4.5", "v1.2.3.4", "1.2.3.4-dev", "1.2.3.4-beta", "a.b.c.d",
    ])
    def test_invalid(self, version):
        with pytest.raises(ValueError, match="Invalid version format"):
            validate_version(version, "test")

    def test_error_includes_source(self):
        with pytest.raises(ValueError, match="my_file.txt"):
            validate_version("bad", "my_file.txt")


class TestGetPyprojectVersion:
    def test_valid(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text('[project]\nversion = "1.2.3.4"\n')
        assert get_pyproject_version(str(pyproject)) == "1.2.3.4"

    @pytest.mark.parametrize("content,error", [
        ('[project]\nname = "test"\n', KeyError),
        ('[tool]\nname = "test"\n', KeyError),
        ('[project]\nversion = "1.2.3"\n', ValueError),
        ('[project]\nversion = "1.2.3.4-dev"\n', ValueError),
    ])
    def test_invalid(self, tmp_path, content, error):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(content)
        with pytest.raises(error):
            get_pyproject_version(str(pyproject))


class TestGetChangelogVersion:
    def test_valid_version(self, tmp_path):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text("# v1.2.3.4\n\n## Changes\n")
        assert get_changelog_version(str(changelog)) == "1.2.3.4"

    def test_unreleased_returns_none(self, tmp_path):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text("# Unreleased\nbump_type: point\n\n- Change\n")
        assert get_changelog_version(str(changelog)) is None

    @pytest.mark.parametrize("content,match", [
        ("# 1.2.3.4\n", "must start with '# v'"),
        ("v1.2.3.4\n", "must start with '# v'"),
        ("", "must start with '# v'"),
        ("# v1.2.3\n", "Invalid version format"),
        ("# v1.2.3.4-dev\n", "Invalid version format"),
    ])
    def test_invalid(self, tmp_path, content, match):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(content)
        with pytest.raises(ValueError, match=match):
            get_changelog_version(str(changelog))


class TestValidateUnreleasedSection:
    @pytest.mark.parametrize("bump_type", ["major", "schema", "results", "point"])
    def test_valid_bump_types(self, tmp_path, bump_type):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(f"# Unreleased\nbump_type: {bump_type}\n\n- Change\n")
        result_type, content = validate_unreleased_section(str(changelog))
        assert result_type == bump_type
        assert len(content) == 1

    @pytest.mark.parametrize("content,match", [
        ("# v1.2.3.4\n- Change\n", "must start with '# Unreleased'"),
        ("# Unreleased\n\n- Change\n", "No 'bump_type:' directive"),
        ("# Unreleased\nbump_type: invalid\n\n- Change\n", "Invalid bump_type"),
        ("# Unreleased\nbump_type: point\n\n# v1.0.0.0\n- Old\n", "has no content"),
    ])
    def test_invalid(self, tmp_path, content, match):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(content)
        with pytest.raises(ValueError, match=match):
            validate_unreleased_section(str(changelog))

    def test_stops_at_next_version(self, tmp_path):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(
            "# Unreleased\nbump_type: point\n\n- New change\n\n# v1.0.0.0\n- Old\n",
        )
        _, content = validate_unreleased_section(str(changelog))
        assert content == ["- New change"]
