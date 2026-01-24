#!/usr/bin/env python

import pytest

import extract_versions
from extract_versions import VersionInfo, get_nested_value, extract_version_info


class TestGetNestedValue:
    """Test the get_nested_value function."""

    @pytest.mark.parametrize(
        "data,keys,default,expected",
        [
            ({"a": {"b": {"c": "value"}}}, ("a", "b", "c"), None, "value"),
            ({"a": {"b": "value"}}, ("a", "c"), None, None),
            ({"a": {"b": "value"}}, ("a", "c"), "custom", "custom"),
            ({"a": "not_a_dict"}, ("a", "b"), None, None),
            ({"a": "value"}, (), None, {"a": "value"}),
            ({"key": "value"}, ("key",), None, "value"),
        ],
        ids=[
            "nested_value",
            "missing_key",
            "custom_default",
            "non_dict_intermediate",
            "no_keys",
            "single_key",
        ],
    )
    def test_get_nested_value(self, data, keys, default, expected):
        """Test retrieving nested values with various inputs."""
        assert get_nested_value(data, *keys, default=default) == expected


class TestExtractVersionInfo:
    """Test the extract_version_info function."""

    @pytest.mark.parametrize(
        "toml_data,expected",
        [
            (
                {"project": {"version": "1.2.3"}},
                VersionInfo("1.2.3", None, None),
            ),
            (
                {
                    "project": {"version": "2.0.0"},
                    "tool": {
                        "mgs-workflow": {
                            "pipeline-min-index-version": "1.0.0",
                            "index-min-pipeline-version": "1.5.0",
                        }
                    },
                },
                VersionInfo("2.0.0", "1.0.0", "1.5.0"),
            ),
            (
                {
                    "project": {"version": "1.0.0"},
                    "tool": {"other-tool": {"key": "value"}},
                },
                VersionInfo("1.0.0", None, None),
            ),
            (
                {
                    "project": {"version": "1.0.0"},
                    "tool": {"mgs-workflow": {"pipeline-min-index-version": "0.5.0"}},
                },
                VersionInfo("1.0.0", "0.5.0", None),
            ),
            (
                {"project": {"version": "1.0.0-alpha"}},
                VersionInfo("1.0.0-alpha", None, None),
            ),
        ],
        ids=[
            "minimal",
            "full",
            "missing_mgs_workflow",
            "partial_min_versions",
            "prerelease_tag",
        ],
    )
    def test_extract_version_info(self, toml_data, expected):
        """Test extracting version info from various TOML structures."""
        assert extract_version_info(toml_data) == expected

    @pytest.mark.parametrize(
        "toml_data",
        [
            {"project": {}},
            {"tool": {"mgs-workflow": {}}},
        ],
        ids=["missing_version", "missing_project"],
    )
    def test_extract_version_info_raises(self, toml_data):
        """Test that missing required fields raise KeyError."""
        with pytest.raises(KeyError):
            extract_version_info(toml_data)


class TestExtractVersionsIntegration:
    """Integration tests for the full extract_versions workflow."""

    @pytest.mark.parametrize(
        "pipeline_toml,index_toml,expected_lines",
        [
            (
                '[project]\nversion = "2.1.0"\n[tool.mgs-workflow]\npipeline-min-index-version = "1.0.0"',
                '[project]\nversion = "1.5.0"\n[tool.mgs-workflow]\nindex-min-pipeline-version = "2.0.0"',
                ["PIPELINE_VERSION=2.1.0", "INDEX_VERSION=1.5.0", "PIPELINE_MIN_INDEX=1.0.0", "INDEX_MIN_PIPELINE=2.0.0"],
            ),
            (
                '[project]\nversion = "1.0.0"',
                '[project]\nversion = "0.9.0"',
                ["PIPELINE_VERSION=1.0.0", "INDEX_VERSION=0.9.0", "PIPELINE_MIN_INDEX=", "INDEX_MIN_PIPELINE="],
            ),
        ],
        ids=["with_optional_fields", "without_optional_fields"],
    )
    def test_extract_versions_output(
        self, temp_file_helper, capsys, pipeline_toml, index_toml, expected_lines
    ):
        """Test extracting versions from TOML files."""
        pipeline_file = temp_file_helper.create_file("pipeline.toml", pipeline_toml)
        index_file = temp_file_helper.create_file("index.toml", index_toml)

        extract_versions.extract_versions(pipeline_file, index_file)

        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        for expected in expected_lines:
            assert expected in lines
