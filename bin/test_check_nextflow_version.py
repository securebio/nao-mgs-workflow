#!/usr/bin/env python3
"""Unit tests for check_nextflow_version.py"""

import json
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from check_nextflow_version import (
    check_pinned_against_target,
    fetch_releases,
    get_pinned_version,
    main,
    parse_nextflowignore,
    select_target_version,
    validate_semver,
)


class TestValidateSemver:
    @pytest.mark.parametrize("version", ["1.2.3", "0.0.0", "10.20.30", "25.10.0"])
    def test_valid(self, version: str) -> None:
        assert validate_semver(version, "test") == version

    @pytest.mark.parametrize(
        "version", ["", "1.2", "1.2.3.4", "v1.2.3", "1.2.3-dev", "a.b.c"],
    )
    def test_invalid(self, version: str) -> None:
        with pytest.raises(ValueError, match="Invalid version format"):
            validate_semver(version, "test")

    def test_error_includes_source(self) -> None:
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
    def test_valid(self, tmp_path: Path, content: str, expected: str) -> None:
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
    def test_invalid(self, tmp_path: Path, content: str) -> None:
        config = tmp_path / "profiles.config"
        config.write_text(content)
        with pytest.raises(ValueError, match="Could not find nextflowVersion"):
            get_pinned_version(config)


class TestParseNextflowignore:
    """parse_nextflowignore covers permanent entries, expirable entries with
    active and past dates, comments, and various malformed inputs."""

    # Symbolic reference date for date-relative tests; entry exp values are
    # chosen so "active" vs "expired" reads from the constant alone, without
    # having to remember the actual calendar date.
    TODAY = date(2030, 6, 15)

    def _write(self, tmp_path: Path, body: str) -> Path:
        path = tmp_path / ".nextflowignore"
        path.write_text(body)
        return path

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        assert parse_nextflowignore(tmp_path / "nope") == set()

    def test_empty_file_returns_empty(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "")
        assert parse_nextflowignore(path) == set()

    def test_only_comments_and_blanks(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "# header\n\n   # indented comment\n\n")
        assert parse_nextflowignore(path) == set()

    def test_permanent_entry(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "25.10.3\n")
        assert parse_nextflowignore(path) == {"25.10.3"}

    def test_active_expirable_entry(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "26.04.0 exp:2031-01-01\n")
        assert parse_nextflowignore(path, today=self.TODAY) == {"26.04.0"}

    def test_expired_entry_dropped_with_warning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        path = self._write(tmp_path, "26.04.0 exp:2025-01-01\n")
        assert parse_nextflowignore(path, today=self.TODAY) == set()
        stderr = capsys.readouterr().err
        assert "expired on 2025-01-01" in stderr
        assert "26.04.0" in stderr

    def test_expiration_today_still_active(self, tmp_path: Path) -> None:
        # Entry expiring today is still active; only past dates lapse.
        path = self._write(tmp_path, f"26.04.0 exp:{self.TODAY.isoformat()}\n")
        assert parse_nextflowignore(path, today=self.TODAY) == {"26.04.0"}

    def test_inline_trailing_comment(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "25.10.3  # broken on AWS Batch\n")
        assert parse_nextflowignore(path) == {"25.10.3"}

    def test_mixed_entries(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        body = (
            "# Block comment\n"
            "\n"
            "25.10.3\n"
            "26.04.0 exp:2031-01-01\n"
            "24.99.99 exp:2020-01-01  # long expired\n"
        )
        path = self._write(tmp_path, body)
        active = parse_nextflowignore(path, today=self.TODAY)
        assert active == {"25.10.3", "26.04.0"}
        assert "24.99.99" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "line",
        [
            "not_a_version",
            "25.10",
            "25.10.3 expires:2030-01-01",  # wrong keyword
            "25.10.3 exp:30-01-01",  # not YYYY-MM-DD
            "25.10.3 exp:2030/01/01",  # wrong separator
            "25.10.3 trailing junk",
        ],
    )
    def test_malformed_entries_raise(self, tmp_path: Path, line: str) -> None:
        path = self._write(tmp_path, line + "\n")
        with pytest.raises(ValueError, match="Malformed entry"):
            parse_nextflowignore(path)

    def test_invalid_calendar_date_raises(self, tmp_path: Path) -> None:
        # Regex accepts YYYY-MM-DD; date.fromisoformat catches impossible dates.
        path = self._write(tmp_path, "25.10.3 exp:2026-02-30\n")
        with pytest.raises(ValueError, match="Invalid expiration date"):
            parse_nextflowignore(path)

    def test_error_message_includes_line_number(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "# c1\n# c2\nnonsense\n")
        with pytest.raises(ValueError, match=":3:"):
            parse_nextflowignore(path)


class TestFetchReleases:
    def _mock(self, releases_data: list[dict[str, object]]) -> MagicMock:
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(releases_data).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        return mock_response

    def test_returns_versions_in_api_order(self) -> None:
        releases_data = [
            {"tag_name": "v25.10.5", "prerelease": False, "draft": False},
            {"tag_name": "26.04.0", "prerelease": False, "draft": False},
            {"tag_name": "v25.10.4", "prerelease": False, "draft": False},
        ]
        with patch("urllib.request.urlopen", return_value=self._mock(releases_data)):
            assert fetch_releases("https://example.com") == ["25.10.5", "26.04.0", "25.10.4"]

    @pytest.mark.parametrize("prerelease,draft", [(True, False), (False, True), (True, True)])
    def test_skips_prerelease_and_draft(self, prerelease: bool, draft: bool) -> None:
        releases_data = [
            {"tag_name": "v25.10.5", "prerelease": prerelease, "draft": draft},
            {"tag_name": "v25.10.4", "prerelease": False, "draft": False},
        ]
        with patch("urllib.request.urlopen", return_value=self._mock(releases_data)):
            assert fetch_releases("https://example.com") == ["25.10.4"]

    def test_skips_invalid_semver_tags(self) -> None:
        releases_data = [
            {"tag_name": "v25.10", "prerelease": False, "draft": False},
            {"tag_name": "edge", "prerelease": False, "draft": False},
            {"tag_name": "v25.10.4", "prerelease": False, "draft": False},
        ]
        with patch("urllib.request.urlopen", return_value=self._mock(releases_data)):
            assert fetch_releases("https://example.com") == ["25.10.4"]


class TestSelectTargetVersion:
    def test_picks_highest_semver_not_chronological(self) -> None:
        # 25.10.5 ships chronologically after 26.04.0 (LTS backport patch);
        # semver-max must still pick 26.04.0.
        releases = ["25.10.5", "26.04.0", "25.10.4"]
        assert select_target_version(releases, set()) == "26.04.0"

    def test_excludes_ignored(self) -> None:
        releases = ["25.10.5", "26.04.0", "25.10.4"]
        assert select_target_version(releases, {"26.04.0"}) == "25.10.5"

    def test_excludes_multiple_ignored(self) -> None:
        releases = ["25.10.5", "26.04.0", "25.10.4"]
        assert select_target_version(releases, {"26.04.0", "25.10.5"}) == "25.10.4"

    def test_raises_when_all_ignored(self) -> None:
        with pytest.raises(ValueError, match="No eligible Nextflow release"):
            select_target_version(["25.10.5"], {"25.10.5"})

    def test_raises_when_no_releases(self) -> None:
        # Empty input should produce a distinct error from the all-ignored case.
        with pytest.raises(ValueError, match="No Nextflow release candidates supplied"):
            select_target_version([], set())


class TestCheckPinnedAgainstTarget:
    """Strict equality: pinned must equal target. Pinning above target is
    treated as a mismatch (typically a stale-ignore signal); below as needing
    a bump."""

    def test_pinned_equals_target(self) -> None:
        check_pinned_against_target("26.04.0", "26.04.0")

    @pytest.mark.parametrize(
        "pinned,target",
        [
            ("25.10.4", "25.10.5"),  # below target — needs a bump
            ("26.04.0", "25.10.5"),  # above target — likely stale ignore on 26.04.0
            ("99.99.99", "25.10.5"),  # not a real release — typo
        ],
    )
    def test_mismatch_fails(self, pinned: str, target: str) -> None:
        with pytest.raises(
            ValueError,
            match=f"Version mismatch: pinned {pinned} != target {target}",
        ):
            check_pinned_against_target(pinned, target)


class TestMain:
    """End-to-end main() tests with mocked urlopen and tmp config + ignore file."""

    def _mock_releases(self, tags: list[str]) -> MagicMock:
        releases_data = [
            {"tag_name": f"v{tag}", "prerelease": False, "draft": False}
            for tag in tags
        ]
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(releases_data).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        return mock_response

    def _write_config(self, tmp_path: Path, pinned: str) -> Path:
        config = tmp_path / "profiles.config"
        config.write_text(f"manifest {{\n    nextflowVersion = '!>={pinned}'\n}}")
        return config

    def _argv(self, config: Path, ignore_file: Path) -> list[str]:
        return [
            "check_nextflow_version.py",
            "--config", str(config),
            "--ignore-file", str(ignore_file),
        ]

    def test_versions_match(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        config = self._write_config(tmp_path, "25.10.5")
        ignore_file = tmp_path / ".nextflowignore"  # missing -> empty ignore set
        with patch(
            "urllib.request.urlopen",
            return_value=self._mock_releases(["25.10.5", "25.10.4"]),
        ), patch("sys.argv", self._argv(config, ignore_file)):
            main()
        captured = capsys.readouterr()
        assert "Pinned Nextflow version: 25.10.5" in captured.out
        assert "Target Nextflow version: 25.10.5" in captured.out
        assert "OK: Pinned version is current" in captured.out

    def test_versions_mismatch(self, tmp_path: Path) -> None:
        config = self._write_config(tmp_path, "25.10.4")
        ignore_file = tmp_path / ".nextflowignore"
        with patch(
            "urllib.request.urlopen",
            return_value=self._mock_releases(["25.10.5", "25.10.4"]),
        ), patch("sys.argv", self._argv(config, ignore_file)), pytest.raises(
            ValueError, match="Version mismatch: pinned 25.10.4 != target 25.10.5",
        ):
            main()

    def test_invalid_config(self, tmp_path: Path) -> None:
        config = tmp_path / "profiles.config"
        config.write_text("manifest {}")
        ignore_file = tmp_path / ".nextflowignore"
        with patch("sys.argv", self._argv(config, ignore_file)), pytest.raises(
            ValueError, match="Could not find nextflowVersion",
        ):
            main()

    def test_ignored_target_falls_back_to_next(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        # 26.04.0 is ignored; target should fall back to 25.10.5.
        config = self._write_config(tmp_path, "25.10.5")
        ignore_file = tmp_path / ".nextflowignore"
        ignore_file.write_text("26.04.0 exp:2030-01-01\n")
        with patch(
            "urllib.request.urlopen",
            return_value=self._mock_releases(["26.04.0", "25.10.5", "25.10.4"]),
        ), patch("sys.argv", self._argv(config, ignore_file)):
            main()
        captured = capsys.readouterr()
        assert "Ignored versions (active): ['26.04.0']" in captured.out
        assert "Target Nextflow version: 25.10.5" in captured.out
        assert "OK: Pinned version is current" in captured.out

    def test_pinned_above_chronologically_later_lts_patch(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        # Pinned 26.04.0; 25.10.5 is the chronologically-latest release but a
        # lower semver. With no ignore file, target = max-semver = 26.04.0,
        # so the pin matches and we do not fail.
        config = self._write_config(tmp_path, "26.04.0")
        ignore_file = tmp_path / ".nextflowignore"
        with patch(
            "urllib.request.urlopen",
            # API order matches "most-recently-released first": 25.10.5 ships
            # after 26.04.0 chronologically.
            return_value=self._mock_releases(["25.10.5", "26.04.0", "25.10.4"]),
        ), patch("sys.argv", self._argv(config, ignore_file)):
            main()
        captured = capsys.readouterr()
        assert "Target Nextflow version: 26.04.0" in captured.out
        assert "OK: Pinned version is current" in captured.out

    def test_permanent_ignore_persists(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        # 25.10.3 is permanently ignored. Even without an exp: date, it stays
        # filtered out so we will never accidentally select it as target.
        config = self._write_config(tmp_path, "25.10.4")
        ignore_file = tmp_path / ".nextflowignore"
        ignore_file.write_text("25.10.3\n")
        with patch(
            "urllib.request.urlopen",
            return_value=self._mock_releases(["25.10.4", "25.10.3"]),
        ), patch("sys.argv", self._argv(config, ignore_file)):
            main()
        assert "Target Nextflow version: 25.10.4" in capsys.readouterr().out

    def test_expired_ignore_warns_and_lapses(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        # An expired ignore lapses, so the version becomes target-eligible
        # again. We expect a stderr warning AND a now-failing version check.
        config = self._write_config(tmp_path, "25.10.4")
        ignore_file = tmp_path / ".nextflowignore"
        ignore_file.write_text("26.04.0 exp:2020-01-01\n")
        with patch(
            "urllib.request.urlopen",
            return_value=self._mock_releases(["26.04.0", "25.10.4"]),
        ), patch("sys.argv", self._argv(config, ignore_file)), pytest.raises(
            ValueError, match="Version mismatch: pinned 25.10.4 != target 26.04.0",
        ):
            main()
        captured = capsys.readouterr()
        assert "expired on 2020-01-01" in captured.err

    def test_pinned_typo_detected(self, tmp_path: Path) -> None:
        # A pin that is not a real release surfaces as a generic mismatch
        # against the actual target.
        config = self._write_config(tmp_path, "25.10.99")
        ignore_file = tmp_path / ".nextflowignore"
        with patch(
            "urllib.request.urlopen",
            return_value=self._mock_releases(["25.10.5", "25.10.4"]),
        ), patch("sys.argv", self._argv(config, ignore_file)), pytest.raises(
            ValueError, match="Version mismatch: pinned 25.10.99 != target 25.10.5",
        ):
            main()
