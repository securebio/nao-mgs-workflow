#!/usr/bin/env python3
"""Unit tests for check_nextflow_version.py"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from check_nextflow_version import (
    compare_versions,
    get_latest_version,
    get_pinned_version,
    main,
    validate_semver,
)


class TestValidateSemver:
    @pytest.mark.parametrize("version", ["1.2.3", "0.0.0", "10.20.30", "25.10.0"])
    def test_valid(self, version):
        assert validate_semver(version, "test") == version

    @pytest.mark.parametrize(
        "version", ["", "1.2", "1.2.3.4", "v1.2.3", "1.2.3-dev", "a.b.c"]
    )
    def test_invalid(self, version):
        with pytest.raises(ValueError, match="Invalid version format"):
            validate_semver(version, "test")

    def test_error_includes_source(self):
        with pytest.raises(ValueError, match="my_file.txt"):
            validate_semver("bad", "my_file.txt")


class TestGetPinnedVersion:
    @pytest.mark.parametrize(
        "content,expected",
        [
            ("manifest {\n    nextflowVersion = '!>=25.10.0'\n}", "25.10.0"),
            ('manifest {\n    nextflowVersion = "!>=1.0.0"\n}', "1.0.0"),
            ("// Comment\nmanifest {\n    nextflowVersion = '!>=99.99.99'\n}\ndocker.enabled = true", "99.99.99"),
        ],
    )
    def test_valid(self, tmp_path, content, expected):
        config = tmp_path / "profiles.config"
        config.write_text(content)
        assert get_pinned_version(config) == expected

    @pytest.mark.parametrize(
        "content",
        [
            "",
            "manifest {}",
            "nextflowVersion = '>=25.10.0'",  # missing !
            "nextflowVersion = '!>=1.2'",  # only 2 parts
            "nextflowVersion = '!>=1.2.3.4'",  # 4 parts
        ],
    )
    def test_invalid(self, tmp_path, content):
        config = tmp_path / "profiles.config"
        config.write_text(content)
        with pytest.raises(ValueError, match="Could not find nextflowVersion"):
            get_pinned_version(config)


class TestGetLatestVersion:
    @pytest.mark.parametrize("tag_name,expected", [("v25.10.0", "25.10.0"), ("25.10.0", "25.10.0")])
    def test_valid(self, tag_name, expected):
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"tag_name": tag_name}).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            assert get_latest_version("https://api.example.com") == expected

    def test_invalid_version_format(self):
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"tag_name": "v1.2"}).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            with pytest.raises(ValueError, match="Invalid version format"):
                get_latest_version("https://api.example.com")


class TestCompareVersions:
    def test_matching_versions(self):
        compare_versions("25.10.0", "25.10.0")

    def test_mismatched_versions(self):
        with pytest.raises(ValueError, match="Version mismatch: 25.10.0 != 25.10.1"):
            compare_versions("25.10.0", "25.10.1")


class TestMain:
    def _mock_api_response(self, version):
        """Create a mock response returning the given version."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"tag_name": f"v{version}"}).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        return mock_response

    def test_versions_match(self, tmp_path, capsys):
        config = tmp_path / "profiles.config"
        config.write_text("manifest {\n    nextflowVersion = '!>=25.10.0'\n}")

        with patch("urllib.request.urlopen", return_value=self._mock_api_response("25.10.0")):
            with patch("sys.argv", ["check_nextflow_version.py", "--config", str(config)]):
                main()

        captured = capsys.readouterr()
        assert "Pinned Nextflow version: 25.10.0" in captured.out
        assert "Latest Nextflow version: 25.10.0" in captured.out
        assert "OK: Pinned version matches target release" in captured.out

    def test_versions_mismatch(self, tmp_path):
        config = tmp_path / "profiles.config"
        config.write_text("manifest {\n    nextflowVersion = '!>=25.10.0'\n}")

        with patch("urllib.request.urlopen", return_value=self._mock_api_response("25.10.1")):
            with patch("sys.argv", ["check_nextflow_version.py", "--config", str(config)]):
                with pytest.raises(ValueError, match="Version mismatch: 25.10.0 != 25.10.1"):
                    main()

    def test_invalid_config(self, tmp_path):
        config = tmp_path / "profiles.config"
        config.write_text("manifest {}")

        with patch("sys.argv", ["check_nextflow_version.py", "--config", str(config)]):
            with pytest.raises(ValueError, match="Could not find nextflowVersion"):
                main()
