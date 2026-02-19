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
    reordered_to_schema,
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

########################
# reordered_to_schema  #
########################

class TestReorderedToSchema:
    def _make_schema(
        self, tmp_path: Path, fields: list[str], fields_match: str | None = None
    ) -> Path:
        schema: dict = {
            "$schema": "https://datapackage.org/profiles/2.0/tableschema.json",
            "fields": [{"name": f, "type": "string"} for f in fields],
        }
        if fields_match is not None:
            schema["fieldsMatch"] = fields_match
        schema_path = tmp_path / "test.schema.json"
        schema_path.write_text(json.dumps(schema))
        return schema_path

    @pytest.mark.parametrize(
        "fields,fields_match,data",
        [
            (["a", "b", "c"], "equal", "a\tb\tc\n1\t2\t3\n"),
            (["a", "b", "c"], "equal", "a\tb\td\n1\t2\t3\n"),
            (["a", "b", "c"], None, "c\ta\tb\n3\t1\t2\n"),
            (["a", "b"], "equal", ""),
            ([], "equal", "a\tb\n1\t2\n"),
        ],
        ids=[
            "columns_already_match",
            "column_sets_differ",
            "fields_match_not_set",
            "empty_file",
            "no_schema_fields",
        ],
    )
    def test_no_reorder(
        self, tmp_path: Path, fields: list[str], fields_match: str | None, data: str,
    ) -> None:
        schema_path = self._make_schema(tmp_path, fields, fields_match)
        data_file = tmp_path / "test.tsv"
        data_file.write_text(data)
        with reordered_to_schema(data_file, schema_path) as path:
            assert path == data_file

    @pytest.mark.parametrize(
        "fields,data,expected",
        [
            (
                ["a", "b", "c"],
                "c\ta\tb\n3\t1\t2\n6\t4\t5\n",
                "a\tb\tc\n1\t2\t3\n4\t5\t6\n",
            ),
            (["a", "b"], 'b\ta\nabc"123\tval\n', 'a\tb\nval\tabc"123\n'),
        ],
        ids=["basic", "quote_characters"],
    )
    def test_reorders(
        self, tmp_path: Path, fields: list[str], data: str, expected: str,
    ) -> None:
        schema_path = self._make_schema(tmp_path, fields, "equal")
        data_file = tmp_path / "test.tsv"
        data_file.write_text(data)
        with reordered_to_schema(data_file, schema_path) as path:
            assert path != data_file
            assert path.read_text() == expected

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

    def test_empty_data_file_is_valid(self, tmp_path: Path, simple_schema: Path) -> None:
        """Header-only files should validate successfully."""
        data_file = tmp_path / "test.tsv"
        data_file.write_text("col1\tcol2\n")
        is_valid, errors = validate_file(data_file, simple_schema)
        assert is_valid
        assert errors == []

    @pytest.mark.parametrize(
        "schema_fields,data,should_pass",
        [
            # Required constraint
            (
                [{"name": "col1", "type": "string", "constraints": {"required": True}}],
                "col1\n\n",
                False,
            ),
            # Pattern constraint - invalid
            (
                [{"name": "id", "type": "string", "constraints": {"pattern": "^[^/]+(/[^/]+)?$"}}],
                "id\nfoo/bar/baz\n",
                False,
            ),
            # Pattern constraint - valid
            (
                [{"name": "id", "type": "string", "constraints": {"pattern": "^[^/]+(/[^/]+)?$"}}],
                "id\nNC_001234.1\nAB/CD\n",
                True,
            ),
            # Enum constraint - invalid
            (
                [{"name": "status", "type": "string", "constraints": {"enum": ["pass", "fail"]}}],
                "status\nunknown\n",
                False,
            ),
            # Minimum constraint - invalid
            (
                [{"name": "count", "type": "integer", "constraints": {"minimum": 0}}],
                "count\n-5\n",
                False,
            ),
            # Maximum constraint - invalid
            (
                [{"name": "frac", "type": "number", "constraints": {"maximum": 1.0}}],
                "frac\n1.5\n",
                False,
            ),
        ],
        ids=["required", "pattern_invalid", "pattern_valid", "enum", "minimum", "maximum"],
    )
    def test_constraint_validation(
        self, tmp_path: Path, schema_fields: list, data: str, should_pass: bool
    ) -> None:
        schema = {
            "$schema": "https://datapackage.org/profiles/2.0/tableschema.json",
            "fields": schema_fields,
        }
        schema_path = tmp_path / "test.schema.json"
        schema_path.write_text(json.dumps(schema))
        data_file = tmp_path / "test.tsv"
        data_file.write_text(data)
        is_valid, errors = validate_file(data_file, schema_path)
        assert is_valid == should_pass
        if not should_pass:
            assert len(errors) > 0

    def test_missing_values_handled(self, tmp_path: Path) -> None:
        """Values in missingValues should be treated as null."""
        schema = {
            "$schema": "https://datapackage.org/profiles/2.0/tableschema.json",
            "missingValues": ["", "NA"],
            "fields": [
                {"name": "col1", "type": "integer"},
                {"name": "col2", "type": "integer"},
            ],
        }
        schema_path = tmp_path / "test.schema.json"
        schema_path.write_text(json.dumps(schema))
        data_file = tmp_path / "test.tsv"
        # Both "NA" and "" should be treated as null, not as invalid integers
        data_file.write_text("col1\tcol2\n42\tNA\nNA\t99\n100\t\n")
        is_valid, errors = validate_file(data_file, schema_path)
        assert is_valid

    def test_valid_with_reordered_columns(self, tmp_path: Path) -> None:
        """Columns in different order should validate when fieldsMatch is equal."""
        schema = {
            "$schema": "https://datapackage.org/profiles/2.0/tableschema.json",
            "fieldsMatch": "equal",
            "fields": [
                {"name": "col1", "type": "string"},
                {"name": "col2", "type": "integer"},
            ],
        }
        schema_path = tmp_path / "reorder.schema.json"
        schema_path.write_text(json.dumps(schema))
        data_file = tmp_path / "test.tsv"
        data_file.write_text("col2\tcol1\n42\tfoo\n99\tbar\n")
        is_valid, errors = validate_file(data_file, schema_path)
        assert is_valid
        assert errors == []

    def test_multiple_errors_all_reported(self, tmp_path: Path, simple_schema: Path) -> None:
        """Multiple validation errors should all be reported."""
        data_file = tmp_path / "test.tsv"
        data_file.write_text("col1\tcol2\nfoo\tnot_int_1\nbar\tnot_int_2\nbaz\tnot_int_3\n")
        is_valid, errors = validate_file(data_file, simple_schema)
        assert not is_valid
        assert len(errors) >= 3

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
