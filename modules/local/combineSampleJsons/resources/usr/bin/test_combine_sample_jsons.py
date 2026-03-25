#!/usr/bin/env python3

import json
from pathlib import Path
from unittest.mock import patch

import pytest

import combine_sample_jsons


class TestExtractSampleName:
    """Test the extract_sample_name function."""

    @pytest.mark.parametrize(
        "filename,suffix,expected",
        [
            pytest.param("sample_1_fastp.json", "fastp.json", "sample_1", id="simple"),
            pytest.param("a_b_c_fastp.json", "fastp.json", "a_b_c", id="underscores_in_name"),
            pytest.param("s1_metrics.json", "metrics.json", "s1", id="different_suffix"),
        ],
    )
    def test_extracts_sample_name(self, filename: str, suffix: str, expected: str) -> None:
        result = combine_sample_jsons.extract_sample_name(Path(filename), suffix)
        assert result == expected

    def test_raises_on_wrong_suffix(self) -> None:
        with pytest.raises(ValueError, match="does not end with"):
            combine_sample_jsons.extract_sample_name(
                Path("sample_1_other.json"), "fastp.json"
            )


class TestCombineSampleJsons:
    """Test the combine_sample_jsons function."""

    @pytest.mark.parametrize(
        "sample_names",
        [
            pytest.param(["s1"], id="single_sample"),
            pytest.param(["s1", "s2", "s3"], id="multiple_samples"),
        ],
    )
    def test_combines_samples_with_injected_fields(self, tmp_path: Path, sample_names: list[str]) -> None:
        input_data = {name: {"reads": name} for name in sample_names}
        for name, data in input_data.items():
            (tmp_path / f"{name}_fastp.json").write_text(json.dumps(data))

        files = sorted(tmp_path.glob("*_fastp.json"))
        result = combine_sample_jsons.combine_sample_jsons(files, "grp", "fastp.json")

        assert len(result) == len(sample_names)
        for name in sample_names:
            assert result[name]["sample"] == name
            assert result[name]["group"] == "grp"
            assert result[name]["reads"] == name

    def test_empty_input_list(self) -> None:
        result = combine_sample_jsons.combine_sample_jsons([], "g", "fastp.json")
        assert result == {}

    def test_raises_on_duplicate_sample_name(self, tmp_path: Path) -> None:
        for subdir in ["a", "b"]:
            d = tmp_path / subdir
            d.mkdir()
            (d / "s1_fastp.json").write_text(json.dumps({"x": subdir}))
        files = sorted(tmp_path.rglob("*_fastp.json"))
        with pytest.raises(ValueError, match="Duplicate sample name"):
            combine_sample_jsons.combine_sample_jsons(files, "g", "fastp.json")


class TestMain:
    """Test the main() CLI entrypoint."""

    @patch("sys.argv", new_callable=list)
    def test_writes_combined_json(self, mock_argv: list[str], tmp_path: Path) -> None:
        input_file = tmp_path / "s1_fastp.json"
        input_file.write_text(json.dumps({"total": 42}))
        output_file = tmp_path / "output.json"

        mock_argv.extend([
            "combine_sample_jsons.py",
            "--group", "g1",
            "--suffix", "fastp.json",
            "--output", str(output_file),
            str(input_file),
        ])
        combine_sample_jsons.main()

        result = json.loads(output_file.read_text())
        assert "s1" in result
        assert result["s1"]["sample"] == "s1"
        assert result["s1"]["group"] == "g1"
        assert result["s1"]["total"] == 42
