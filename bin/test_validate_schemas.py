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
    validate_json_file,
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

    def test_raises_on_duplicate_data_columns(self, tmp_path: Path) -> None:
        schema_path = self._make_schema(tmp_path, ["a", "b", "c"], "equal")
        data_file = tmp_path / "test.tsv"
        data_file.write_text("a\tb\ta\n1\t2\t3\n")
        with pytest.raises(ValueError, match="Duplicate columns"):
            with reordered_to_schema(data_file, schema_path) as path:
                pass

#######################
# validate_json_file  #
#######################

class TestValidateJsonFile:
    SCHEMA_WITH_REQUIRED = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"name": {"type": "string"}, "count": {"type": "integer"}},
        "required": ["name"],
    }
    PERMISSIVE_SCHEMA = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
    }

    @pytest.mark.parametrize("schema,data,should_pass", [
        (SCHEMA_WITH_REQUIRED, {"name": "hello"}, True),
        (SCHEMA_WITH_REQUIRED, {}, False),
        (SCHEMA_WITH_REQUIRED, {"name": "hello", "count": "not_an_int"}, False),
        (PERMISSIVE_SCHEMA, {}, True),
    ], ids=["valid", "missing_required", "wrong_type", "empty_object_permissive"])
    def test_validates_json(self, tmp_path: Path, schema: dict, data: dict, should_pass: bool) -> None:
        data_file = tmp_path / "test.json"
        data_file.write_text(json.dumps(data))
        is_valid, errors = validate_json_file(data_file, schema)
        assert is_valid == should_pass
        if not should_pass:
            assert len(errors) > 0

    def test_malformed_json_returns_error(self, tmp_path: Path) -> None:
        """Malformed JSON should produce a clean FAIL, not an exception."""
        data_file = tmp_path / "bad.json"
        data_file.write_text("{not valid json")
        is_valid, errors = validate_json_file(data_file, self.PERMISSIVE_SCHEMA)
        assert not is_valid
        assert len(errors) == 1
        assert "Invalid JSON" in errors[0]

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

    @pytest.mark.parametrize("data,should_pass", [
        ({"name": "hello"}, True),
        ({}, False),
    ], ids=["valid_json", "invalid_json"])
    def test_json_schema_dispatch(self, tmp_path: Path, data: dict, should_pass: bool) -> None:
        """validate_file dispatches to JSON Schema validation for JSON Schemas."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        }
        schema_path = tmp_path / "test.schema.json"
        schema_path.write_text(json.dumps(schema))
        data_file = tmp_path / "test.json"
        data_file.write_text(json.dumps(data))
        is_valid, errors = validate_file(data_file, schema_path)
        assert is_valid == should_pass
        if not should_pass:
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

    @pytest.mark.parametrize("schema,expected_exit", [
        ({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"}, 0),
        ({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
          "properties": {"name": {"type": "string"}}, "required": ["name"]}, 1),
    ], ids=["valid_json", "invalid_json"])
    def test_json_validation(self, tmp_path: Path, schema: dict, expected_exit: int) -> None:
        """JSON files are validated against JSON Schemas."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-downstream = [
    "results_downstream/{GROUP}_fastp.json",
]
""")
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        (schema_dir / "fastp.schema.json").write_text(json.dumps(schema))
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        results = output_dir / "results"
        results.mkdir()
        (results / "group1_fastp.json").write_text("{}")
        exit_code = validate_outputs(output_dir, schema_dir, pyproject)
        assert exit_code == expected_exit


###############################
# TestFastpSchemaIntegration  #
###############################

class TestFastpSchemaIntegration:
    def test_real_fastp_schema_integration(self) -> None:
        """Integration test: validate real test data against the fastp schema."""
        repo_root = Path(__file__).resolve().parent.parent
        schema_path = repo_root / "schemas" / "fastp.schema.json"
        data_path = repo_root / "test-data" / "results" / "downstream_output_shortread" / "tt1_fastp.json"
        is_valid, errors = validate_file(data_path, schema_path)
        assert is_valid, f"Validation errors: {errors}"

    def test_empty_sample_fixture_matches_sample_entry_schema(self) -> None:
        """Validate empty_sample_fastp.json against the sample_entry sub-schema.

        Catches schema tightenings that would break the empty fixture before
        they surface in the full DOWNSTREAM workflow test.
        """
        from jsonschema.validators import validator_for

        repo_root = Path(__file__).resolve().parent.parent
        schema_path = repo_root / "schemas" / "fastp.schema.json"
        fixture_path = repo_root / "test-data" / "downstream" / "empty" / "empty_sample_fastp.json"
        with open(schema_path) as f:
            full_schema = json.load(f)
        # Extract sample_entry and inject $defs so $ref resolution works
        sample_entry_schema = {**full_schema["$defs"]["sample_entry"], "$defs": full_schema["$defs"]}
        with open(fixture_path) as f:
            fixture_data = json.load(f)
        # The fixture is a per-sample file; add pipeline-injected fields
        fixture_data["sample"] = "empty_sample"
        fixture_data["group"] = "empty_group"
        validator_cls = validator_for(full_schema)
        validator = validator_cls(sample_entry_schema)
        errors = sorted(validator.iter_errors(fixture_data), key=lambda e: list(e.absolute_path))
        assert not errors, f"Fixture fails sample_entry validation: {[e.message for e in errors]}"
