#!/usr/bin/env python3
"""Unit tests for extract_changelog.py"""

import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_changelog import (
    parse_version_header,
    extract_changelog,
)


class TestParseVersionHeader:
    @pytest.mark.parametrize("line,expected", [
        ("# v1.2.3.4", "1.2.3.4"),
        ("# 1.2.3.4", "1.2.3.4"),
        ("# v1.2.3.4-dev", "1.2.3.4-dev"),
        ("# 1.2.3.4-dev", "1.2.3.4-dev"),
        ("# v0.0.0.0", "0.0.0.0"),
        ("# v10.20.30.40", "10.20.30.40"),
        ("#v1.2.3.4", "1.2.3.4"),  # No space after #
        ("#  v1.2.3.4", "1.2.3.4"),  # Multiple spaces
    ])
    def test_valid_headers(self, line, expected):
        """Test parsing of valid version headers."""
        assert parse_version_header(line) == expected

    @pytest.mark.parametrize("line", [
        "# v1.2.3",  # Wrong format (X.Y.Z instead of X.Y.Z.W)
        "# v1.2.3.4.5",  # Too many components
        "# 1.2.3",  # Wrong format without v prefix
        "## v1.2.3.4",  # Wrong markdown level
        "v1.2.3.4",  # No #
        "# Version 1.2.3.4",  # Wrong prefix
        "# v1.2.3.4-beta",  # Wrong suffix (not -dev)
        "# va.b.c.d",  # Non-numeric
        "",  # Empty
        "   ",  # Whitespace only
        "# Some other header",  # Not a version
    ])
    def test_invalid_headers(self, line):
        """Test that invalid headers return None."""
        assert parse_version_header(line) is None


class TestExtractChangelog:
    @pytest.mark.parametrize("content,version,expected", [
        # Basic extraction
        (
            "# v1.0.0.0\n- First feature\n- Second feature\n\n# v0.9.0.0\n- Old feature\n",
            "1.0.0.0",
            "- First feature\n- Second feature\n",
        ),
        # Without v prefix
        (
            "# 1.0.0.0\n- Feature\n\n# 0.9.0.0\n- Old\n",
            "1.0.0.0",
            "- Feature\n",
        ),
        # Dev version
        (
            "# v1.0.0.1-dev\n- Dev feature\n- Another dev feature\n\n# v1.0.0.0\n- Released\n",
            "1.0.0.1-dev",
            "- Dev feature\n- Another dev feature\n",
        ),
        # First version in changelog
        (
            "# v2.0.0.0\n- Latest feature\n\n# v1.0.0.0\n- Old feature\n",
            "2.0.0.0",
            "- Latest feature\n",
        ),
        # Last version in changelog (no next header)
        (
            "# v2.0.0.0\n- New\n\n# v1.0.0.0\n- Old feature\n- Another old feature\n",
            "1.0.0.0",
            "- Old feature\n- Another old feature\n",
        ),
        # Middle version
        (
            "# v3.0.0.0\n- Newest\n\n# v2.0.0.0\n- Middle feature\n- Another middle\n\n# v1.0.0.0\n- Oldest\n",
            "2.0.0.0",
            "- Middle feature\n- Another middle\n",
        ),
        # Various version number formats
        (
            "# v10.20.30.40\n- Feature\n\n# v0.0.0.0\n- Old\n",
            "10.20.30.40",
            "- Feature\n",
        ),
        (
            "# v0.0.0.0\n- Feature\n\n# v1.0.0.0\n- Newer\n",
            "0.0.0.0",
            "- Feature\n",
        ),
    ])
    def test_extraction_scenarios(self, tmp_path, content, version, expected):
        """Test various extraction scenarios."""
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(content)
        result = extract_changelog(version, changelog)
        assert result == expected

    @pytest.mark.parametrize("content,version,expected", [
        # Empty lines filtered out
        (
            "# v1.0.0.0\n\n- Feature one\n\n\n- Feature two\n    - Sub-feature\n\n# v0.9.0.0\n- Old\n",
            "1.0.0.0",
            "- Feature one\n- Feature two\n    - Sub-feature\n",
        ),
        # Indentation preserved
        (
            "# v1.0.0.0\n- Top level\n    - Indented\n        - More indented\n- Another top level\n\n# v0.9.0.0\n- Old\n",
            "1.0.0.0",
            "- Top level\n    - Indented\n        - More indented\n- Another top level\n",
        ),
        # Multiline entries
        (
            "# v1.0.0.0\n- Feature with\n  continuation line\n- Another feature\n\n# v0.9.0.0\n- Old\n",
            "1.0.0.0",
            "- Feature with\n  continuation line\n- Another feature\n",
        ),
        # Subsections
        (
            "# v3.0.0.0\n## Breaking Changes\n- Breaking feature\n## Bug Fixes\n- Fixed bug\n\n# v2.0.0.0\n- Old\n",
            "3.0.0.0",
            "## Breaking Changes\n- Breaking feature\n## Bug Fixes\n- Fixed bug\n",
        ),
    ])
    def test_formatting_preservation(self, tmp_path, content, version, expected):
        """Test that formatting is preserved correctly."""
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(content)
        result = extract_changelog(version, changelog)
        assert result == expected

    @pytest.mark.parametrize("content,version,expected", [
        # Empty section (only blank lines)
        (
            "# v1.0.0.0\n\n\n# v0.9.0.0\n- Old\n",
            "1.0.0.0",
            "",
        ),
    ])
    def test_edge_cases(self, tmp_path, content, version, expected):
        """Test edge cases."""
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(content)
        result = extract_changelog(version, changelog)
        assert result == expected

    @pytest.mark.parametrize("content,version,match", [
        # Version not found
        (
            "# v1.0.0.0\n- Feature\n\n# v0.9.0.0\n- Old\n",
            "2.0.0.0",
            "Version 2.0.0.0 not found",
        ),
    ])
    def test_errors(self, tmp_path, content, version, match):
        """Test error conditions."""
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(content)
        with pytest.raises(ValueError, match=match):
            extract_changelog(version, changelog)

    def test_file_not_found(self, tmp_path):
        """Test error when changelog file doesn't exist."""
        nonexistent = tmp_path / "nonexistent.md"
        with pytest.raises(FileNotFoundError, match="Changelog file not found"):
            extract_changelog("1.0.0.0", nonexistent)

    def test_realistic_changelog(self, tmp_path):
        """Test with a realistic changelog format similar to the actual project."""
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(
            "# v3.0.1.8-dev\n"
            "- Added similarity-based duplicate marking tool in `post-processing/`:\n"
            "    - New Rust tool (`rust_dedup/`) for similarity-based duplicate detection\n"
            "    - Uses nao-dedup library (added as git submodule)\n"
            "- Pruning and streamlining testing for easier releases:\n"
            "    - Separated downloading part of JOIN_RIBO_REF\n"
            "    - Moved ADD_CONDITIONAL_TSV_COLUMN to Python\n"
            "\n"
            "# v3.0.1.7\n"
            "- Clarified testing documentation in `docs/developer.md`.\n"
            "- Added bin/clean-nf-test.sh for test cleanup.\n"
            "\n"
            "# v3.0.1.6\n"
            "- Modified filterTsvColumnByValue to handle quotation characters.\n"
        )
        result = extract_changelog("3.0.1.7", changelog)
        assert result == (
            "- Clarified testing documentation in `docs/developer.md`.\n"
            "- Added bin/clean-nf-test.sh for test cleanup.\n"
        )
