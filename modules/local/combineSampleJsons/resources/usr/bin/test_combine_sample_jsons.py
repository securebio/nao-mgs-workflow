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
            pytest.param("my_sample_fastp.json", "fastp.json", "my_sample", id="underscore_in_name"),
            pytest.param("s1_metrics.json", "metrics.json", "s1", id="short_name"),
            pytest.param("a_b_c_data.json", "data.json", "a_b_c", id="multiple_underscores"),
        ],
    )
    def test_extracts_sample_name(self, filename, suffix, expected):
        result = combine_sample_jsons.extract_sample_name(Path(filename), suffix)
        assert result == expected

    def test_raises_on_wrong_suffix(self):
        with pytest.raises(ValueError, match="does not end with"):
            combine_sample_jsons.extract_sample_name(
                Path("sample_1_other.json"), "fastp.json"
            )


class TestCombineSampleJsons:
    """Test the combine_sample_jsons function."""

    def test_single_sample(self, tmp_path):
        data = {"summary": {"total_reads": 100}, "command": "fastp --stdin"}
        input_file = tmp_path / "sample_1_fastp.json"
        input_file.write_text(json.dumps(data))

        result = combine_sample_jsons.combine_sample_jsons(
            [input_file], "group_1", "fastp.json"
        )

        assert "sample_1" in result
        assert result["sample_1"]["sample"] == "sample_1"
        assert result["sample_1"]["group"] == "group_1"
        assert result["sample_1"]["summary"] == {"total_reads": 100}
        assert result["sample_1"]["command"] == "fastp --stdin"

    def test_multiple_samples(self, tmp_path):
        for name in ["s1", "s2", "s3"]:
            f = tmp_path / f"{name}_fastp.json"
            f.write_text(json.dumps({"reads": name}))

        files = sorted(tmp_path.glob("*_fastp.json"))
        result = combine_sample_jsons.combine_sample_jsons(
            files, "grp", "fastp.json"
        )

        assert len(result) == 3
        for name in ["s1", "s2", "s3"]:
            assert result[name]["sample"] == name
            assert result[name]["group"] == "grp"
            assert result[name]["reads"] == name

    def test_empty_input_list(self):
        result = combine_sample_jsons.combine_sample_jsons(
            [], "group_1", "fastp.json"
        )
        assert result == {}

    def test_sample_and_group_fields_added_at_top_level(self, tmp_path):
        data = {"nested": {"key": "value"}}
        input_file = tmp_path / "test_sample_metrics.json"
        input_file.write_text(json.dumps(data))

        result = combine_sample_jsons.combine_sample_jsons(
            [input_file], "my_group", "metrics.json"
        )

        entry = result["test_sample"]
        assert "sample" in entry
        assert "group" in entry
        assert entry["nested"] == {"key": "value"}


class TestMain:
    """Test the main() CLI entrypoint."""

    def test_writes_combined_json(self, tmp_path):
        input_file = tmp_path / "s1_fastp.json"
        input_file.write_text(json.dumps({"total": 42}))
        output_file = tmp_path / "output.json"

        with patch(
            "sys.argv",
            [
                "combine_sample_jsons.py",
                "--group", "g1",
                "--suffix", "fastp.json",
                "--output", str(output_file),
                str(input_file),
            ],
        ):
            combine_sample_jsons.main()

        result = json.loads(output_file.read_text())
        assert "s1" in result
        assert result["s1"]["sample"] == "s1"
        assert result["s1"]["group"] == "g1"
        assert result["s1"]["total"] == 42
