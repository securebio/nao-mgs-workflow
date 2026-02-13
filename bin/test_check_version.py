#!/usr/bin/env python3
"""Unit tests for check_version.py"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from check_version import (
    get_changelog_version,
    get_pyproject_version,
    main,
    validate_version,
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


class TestMain:
    """Test the main function with branch-based suffix rules."""

    def _setup_files(self, tmp_path, version):
        """Create pyproject.toml and CHANGELOG.md with the given version."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(f'[project]\nversion = "{version}"\n')
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(f"# v{version}\n- Changes\n")
        return str(pyproject), str(changelog)

    def test_no_branch_info_dev_version(self, tmp_path):
        """Without branch info, just checks version consistency."""
        pyproject, changelog = self._setup_files(tmp_path, "1.2.3.4-dev")
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4-dev"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4-dev"), \
             patch("sys.argv", ["check_version.py"]):
            assert main() == 0

    def test_no_branch_info_release_version(self, tmp_path):
        """Without branch info, release version also passes."""
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4"), \
             patch("sys.argv", ["check_version.py"]):
            assert main() == 0

    def test_version_mismatch(self):
        """Mismatched versions should fail."""
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4-dev"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.5-dev"), \
             patch("sys.argv", ["check_version.py"]):
            assert main() == 1

    # PRs to main: MUST have -dev
    def test_pr_to_main_with_dev_passes(self):
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4-dev"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4-dev"), \
             patch("sys.argv", ["check_version.py", "--base-branch", "main", "--head-branch", "release/jo/123-release"]):
            assert main() == 0

    def test_pr_to_main_without_dev_fails(self):
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4"), \
             patch("sys.argv", ["check_version.py", "--base-branch", "main", "--head-branch", "release/jo/123-release"]):
            assert main() == 1

    # PRs to stable: must NOT have -dev
    def test_pr_to_stable_without_dev_passes(self):
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4"), \
             patch("sys.argv", ["check_version.py", "--base-branch", "stable", "--head-branch", "hotfix/jo/456-fix"]):
            assert main() == 0

    def test_pr_to_stable_with_dev_fails(self):
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4-dev"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4-dev"), \
             patch("sys.argv", ["check_version.py", "--base-branch", "stable", "--head-branch", "hotfix/jo/456-fix"]):
            assert main() == 1

    # PRs to dev: MUST have -dev (all PRs, regardless of branch type)
    def test_pr_to_dev_feature_with_dev_passes(self):
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4-dev"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4-dev"), \
             patch("sys.argv", ["check_version.py", "--base-branch", "dev", "--head-branch", "feature/jo/789-add-thing"]):
            assert main() == 0

    def test_pr_to_dev_feature_without_dev_fails(self):
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4"), \
             patch("sys.argv", ["check_version.py", "--base-branch", "dev", "--head-branch", "feature/jo/789-add-thing"]):
            assert main() == 1

    def test_pr_to_dev_release_with_dev_passes(self):
        """Release PRs to dev must also have -dev suffix now."""
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4-dev"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4-dev"), \
             patch("sys.argv", ["check_version.py", "--base-branch", "dev", "--head-branch", "release/jo/123-release"]):
            assert main() == 0

    def test_pr_to_dev_release_without_dev_fails(self):
        """Release PRs to dev must also have -dev suffix now."""
        with patch("check_version.get_pyproject_version", return_value="1.2.3.4"), \
             patch("check_version.get_changelog_version", return_value="1.2.3.4"), \
             patch("sys.argv", ["check_version.py", "--base-branch", "dev", "--head-branch", "release/jo/123-release"]):
            assert main() == 1
