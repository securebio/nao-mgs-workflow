#!/usr/bin/env python3

from unittest.mock import patch
import pytest
import get_run_output_suffixes
from pathlib import Path


class TestGetRunOutputSuffixes:
    """Test the get_run_output_suffixes function."""

    TOML_WITH_SHORTREAD_EXTRA = (
        '[tool.mgs-workflow]\n'
        'expected-outputs-run = [\n'
        '    "results/{SAMPLE}_virus_hits.tsv.gz",\n'
        ']\n'
        'expected-outputs-run-shortread-extra = [\n'
        '    "results/{SAMPLE}_fastp.json",\n'
        ']\n'
    )

    @pytest.mark.parametrize(
        "toml_content,expected",
        [
            pytest.param(
                '[tool.mgs-workflow]\n'
                'expected-outputs-run = [\n'
                '    "results/{SAMPLE}_virus_hits.tsv.gz",\n'
                '    "results/{SAMPLE}_read_counts.tsv",\n'
                '    "input/samplesheet.csv",\n'
                ']\n',
                ["read_counts.tsv", "virus_hits.tsv"],
                id="extracts_sample_suffixes_and_strips_gz",
            ),
            pytest.param(
                '[tool.mgs-workflow]\n'
                'expected-outputs-run = [\n'
                '    "results/{SAMPLE}_read_counts.tsv",\n'
                '    "input/samplesheet.csv",\n'
                ']\n'
                'expected-outputs-downstream = [\n'
                '    "results_downstream/{GROUP}_validation_hits.tsv.gz",\n'
                ']\n',
                ["read_counts.tsv"],
                id="ignores_group_patterns_and_non_templated",
            ),
            pytest.param(
                '[tool.other]\nfoo = "bar"\n',
                [],
                id="empty_when_no_mgs_workflow_section",
            ),
        ],
    )
    def test_get_run_output_suffixes(self, tmp_path, toml_content, expected):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(toml_content)
        assert get_run_output_suffixes.get_run_output_suffixes(pyproject) == expected

    @pytest.mark.parametrize(
        "platform,expected",
        [
            pytest.param(
                "illumina",
                ["fastp.json", "virus_hits.tsv"],
                id="illumina_includes_shortread_extra",
            ),
            pytest.param(
                "ont",
                ["virus_hits.tsv"],
                id="ont_excludes_shortread_extra",
            ),
        ],
    )
    def test_platform_filtering(self, tmp_path, platform, expected):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(self.TOML_WITH_SHORTREAD_EXTRA)
        result = get_run_output_suffixes.get_run_output_suffixes(pyproject, platform)
        assert result == expected

    def test_against_real_pyproject(self):
        """Smoke test against the actual repo pyproject.toml."""
        pyproject = Path(__file__).resolve().parents[6] / "pyproject.toml"
        if not pyproject.exists():
            pytest.skip("pyproject.toml not found at repo root")
        result = get_run_output_suffixes.get_run_output_suffixes(pyproject, "illumina")
        assert len(result) > 0
        assert "virus_hits.tsv" in result
        assert "read_counts.tsv" in result
        assert "fastp.json" in result
        for s in result:
            assert not s.endswith(".gz")

class TestMain:
    """Test the main() CLI entrypoint."""

    def test_prints_suffixes_to_stdout(self, tmp_path, capsys):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            '[tool.mgs-workflow]\n'
            'expected-outputs-run = [\n'
            '    "results/{SAMPLE}_bracken.tsv.gz",\n'
            '    "results/{SAMPLE}_read_counts.tsv",\n'
            ']\n'
        )
        with patch("sys.argv", ["get_run_output_suffixes.py", "--platform", "illumina", str(pyproject)]):
            get_run_output_suffixes.main()
        captured = capsys.readouterr()
        assert captured.out == "bracken.tsv\nread_counts.tsv\n"

    def test_exits_on_missing_file(self, tmp_path):
        with patch(
            "sys.argv",
            ["get_run_output_suffixes.py", "--platform", "illumina", str(tmp_path / "nonexistent.toml")],
        ):
            with pytest.raises(SystemExit):
                get_run_output_suffixes.main()
