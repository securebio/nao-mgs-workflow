#!/usr/bin/env python3
"""Tests for validate_schemas.py."""

import gzip
import json
from pathlib import Path

import pytest

from validate_schemas import (
    decompressed_path,
    find_data_files,
    find_schema_for_file,
    get_output_schema_names,
    validate_file,
    validate_outputs,
)

############################
# get_output_schema_names  #
############################

class TestGetOutputSchemaNames:
    def test_extracts_schema_names_from_pyproject(self, tmp_path: Path) -> None:
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-downstream = [
    "results_downstream/{GROUP}_clade_counts.tsv.gz",
    "results_downstream/{GROUP}_duplicate_stats.tsv.gz",
]
""")
        result = get_output_schema_names(pyproject)
        assert result == {"clade_counts", "duplicate_stats"}

    def test_ignores_non_group_patterns(self, tmp_path: Path) -> None:
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-run = [
    "results/read_counts.tsv.gz",
    "results/virus_hits_final.tsv.gz",
]
expected-outputs-downstream = [
    "results_downstream/{GROUP}_clade_counts.tsv.gz",
]
""")
        result = get_output_schema_names(pyproject)
        assert result == {"clade_counts"}

    def test_returns_empty_set_when_no_patterns(self, tmp_path: Path) -> None:
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
some-other-key = "value"
""")
        result = get_output_schema_names(pyproject)
        assert result == set()

#########################
# find_schema_for_file  #
#########################

class TestFindSchemaForFile:
    def test_returns_path_when_schema_exists(self, tmp_path: Path) -> None:
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "duplicate_stats.schema.json"
        schema_file.write_text("{}")
        data_file = tmp_path / "tt1_duplicate_stats.tsv.gz"
        known_schema_names = {"duplicate_stats", "clade_counts"}
        result = find_schema_for_file(data_file, schema_dir, known_schema_names)
        assert result == schema_file

    def test_returns_none_when_schema_not_found(self, tmp_path: Path) -> None:
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        data_file = tmp_path / "tt1_nonexistent.tsv.gz"
        known_schema_names = {"duplicate_stats", "clade_counts"}
        result = find_schema_for_file(data_file, schema_dir, known_schema_names)
        assert result is None

    def test_returns_none_when_schema_name_not_known(self, tmp_path: Path) -> None:
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "duplicate_stats.schema.json"
        schema_file.write_text("{}")
        data_file = tmp_path / "tt1_duplicate_stats.tsv.gz"
        known_schema_names = {"clade_counts"}  # duplicate_stats not in set
        result = find_schema_for_file(data_file, schema_dir, known_schema_names)
        assert result is None

    def test_handles_group_id_with_underscores(self, tmp_path: Path) -> None:
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "duplicate_stats.schema.json"
        schema_file.write_text("{}")
        data_file = tmp_path / "group_with_underscores_duplicate_stats.tsv.gz"
        known_schema_names = {"duplicate_stats"}
        result = find_schema_for_file(data_file, schema_dir, known_schema_names)
        assert result == schema_file

####################
# find_data_files  #
####################

class TestFindDataFiles:
    def test_finds_files_in_results_dirs(self, tmp_path: Path) -> None:
        # Create results directories with files
        results = tmp_path / "results"
        results.mkdir()
        (results / "file1.tsv").touch()
        (results / "file2.tsv.gz").touch()
        results_downstream = tmp_path / "results_downstream"
        results_downstream.mkdir()
        (results_downstream / "file3.tsv").touch()
        # Create a non-results directory that should be ignored
        other = tmp_path / "logging"
        other.mkdir()
        (other / "ignored.tsv").touch()
        files = find_data_files(tmp_path)
        assert len(files) == 3
        names = [f.name for f in files]
        assert "file1.tsv" in names
        assert "file2.tsv.gz" in names
        assert "file3.tsv" in names
        assert "ignored.tsv" not in names

    def test_returns_empty_list_when_no_results_dirs(self, tmp_path: Path) -> None:
        files = find_data_files(tmp_path)
        assert files == []

#####################
# decompressed_path #
#####################

class TestDecompressedPath:
    def test_yields_original_path_for_uncompressed(self, tmp_path: Path) -> None:
        data_file = tmp_path / "test.tsv"
        data_file.write_text("col1\tcol2\nval1\tval2\n")
        with decompressed_path(data_file) as path:
            assert path == data_file
            assert path.read_text() == "col1\tcol2\nval1\tval2\n"

    def test_yields_temp_path_for_compressed(self, tmp_path: Path) -> None:
        data_file = tmp_path / "test.tsv.gz"
        content = "col1\tcol2\nval1\tval2\n"
        with gzip.open(data_file, "wt") as f:
            f.write(content)
        with decompressed_path(data_file) as path:
            assert path != data_file
            assert path.read_text() == content

#################
# validate_file #
#################

class TestValidateFile:
    @pytest.fixture
    def simple_schema(self, tmp_path: Path) -> Path:
        schema = {
            "$schema": "https://datapackage.org/profiles/2.0/tableschema.json",
            "fields": [
                {"name": "col1", "type": "string"},
                {"name": "col2", "type": "integer"},
            ],
        }
        schema_path = tmp_path / "test.schema.json"
        schema_path.write_text(json.dumps(schema))
        return schema_path

    def test_valid_uncompressed_file(self, tmp_path: Path, simple_schema: Path) -> None:
        data_file = tmp_path / "test.tsv"
        data_file.write_text("col1\tcol2\nfoo\t42\nbar\t99\n")
        is_valid, errors = validate_file(data_file, simple_schema)
        assert is_valid
        assert errors == []

    def test_valid_compressed_file(self, tmp_path: Path, simple_schema: Path) -> None:
        data_file = tmp_path / "test.tsv.gz"
        with gzip.open(data_file, "wt") as f:
            f.write("col1\tcol2\nfoo\t42\n")
        is_valid, errors = validate_file(data_file, simple_schema)
        assert is_valid
        assert errors == []

    def test_invalid_type_returns_errors(self, tmp_path: Path, simple_schema: Path) -> None:
        data_file = tmp_path / "test.tsv"
        data_file.write_text("col1\tcol2\nfoo\tnot_an_int\n")
        is_valid, errors = validate_file(data_file, simple_schema)
        assert not is_valid
        assert len(errors) > 0

####################
# validate_outputs #
####################

class TestValidateOutputs:
    @pytest.fixture
    def pyproject(self, tmp_path: Path) -> Path:
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-downstream = [
    "results_downstream/{GROUP}_test.tsv.gz",
]
""")
        return pyproject

    def test_success_when_all_valid(self, tmp_path: Path, pyproject: Path) -> None:
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        schema = {
            "$schema": "https://datapackage.org/profiles/2.0/tableschema.json",
            "fields": [{"name": "col1", "type": "string"}],
        }
        (schema_dir / "test.schema.json").write_text(json.dumps(schema))
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        results = output_dir / "results"
        results.mkdir()
        (results / "group1_test.tsv").write_text("col1\nvalue\n")
        exit_code = validate_outputs(output_dir, schema_dir, pyproject)
        assert exit_code == 0

    def test_failure_when_invalid(self, tmp_path: Path, pyproject: Path) -> None:
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        schema = {
            "$schema": "https://datapackage.org/profiles/2.0/tableschema.json",
            "fields": [{"name": "col1", "type": "integer"}],
        }
        (schema_dir / "test.schema.json").write_text(json.dumps(schema))
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        results = output_dir / "results"
        results.mkdir()
        (results / "group1_test.tsv").write_text("col1\nnot_an_int\n")
        exit_code = validate_outputs(output_dir, schema_dir, pyproject)
        assert exit_code == 1

    def test_success_when_no_matching_schemas(self, tmp_path: Path, pyproject: Path) -> None:
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        results = output_dir / "results"
        results.mkdir()
        (results / "group1_unknown.tsv").write_text("col1\nvalue\n")
        exit_code = validate_outputs(output_dir, schema_dir, pyproject)
        assert exit_code == 0

    def test_failure_when_output_dir_missing(self, tmp_path: Path, pyproject: Path) -> None:
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        output_dir = tmp_path / "nonexistent"
        exit_code = validate_outputs(output_dir, schema_dir, pyproject)
        assert exit_code == 1

    def test_failure_when_schema_dir_missing(self, tmp_path: Path, pyproject: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        schema_dir = tmp_path / "nonexistent"
        exit_code = validate_outputs(output_dir, schema_dir, pyproject)
        assert exit_code == 1

    def test_failure_when_pyproject_missing(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        pyproject = tmp_path / "nonexistent.toml"
        exit_code = validate_outputs(output_dir, schema_dir, pyproject)
        assert exit_code == 1
