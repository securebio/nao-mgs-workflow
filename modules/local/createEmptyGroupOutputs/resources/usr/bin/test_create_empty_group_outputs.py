import gzip
import json
import pytest
from create_empty_group_outputs import (
    open_by_suffix,
    get_group_output_patterns,
    get_schema_name_from_pattern,
    load_empty_json,
    load_schema_headers,
    create_empty_outputs,
    parse_args,
)


#=============================================================================
# Test helpers
#=============================================================================

def write_pyproject(path, illumina_outputs, ont_outputs):
    """Write a minimal pyproject.toml with expected outputs."""
    content = "[tool.mgs-workflow]\n"
    content += f"expected-outputs-downstream = {illumina_outputs!r}\n"
    content += f"expected-outputs-downstream-ont = {ont_outputs!r}\n"
    path.write_text(content)


#=============================================================================
# Tests for open_by_suffix
#=============================================================================

class TestOpenBySuffix:
    """Tests for open_by_suffix function."""

    @pytest.mark.parametrize("suffix", [".gz", ".tsv"])
    def test_writes_and_reads_file(self, tmp_path, suffix):
        """Test that files can be written and read with correct compression."""
        filepath = tmp_path / f"test{suffix}"
        test_content = "hello\nworld"

        with open_by_suffix(filepath, "w") as f:
            f.write(test_content)

        with open_by_suffix(filepath, "r") as f:
            assert f.read() == test_content


#=============================================================================
# Tests for get_group_output_patterns
#=============================================================================

class TestGetGroupOutputPatterns:
    """Tests for get_group_output_patterns function."""

    @pytest.mark.parametrize("platform,illumina,ont,expected", [
        # Illumina platform extracts illumina patterns
        (
            "illumina",
            ["input/file.csv", "results/{GROUP}_clade.tsv.gz", "results/{GROUP}_dup.tsv.gz"],
            ["results/{GROUP}_val.tsv.gz"],
            ["{GROUP}_clade.tsv.gz", "{GROUP}_dup.tsv.gz"],
        ),
        # ONT platform extracts ont patterns
        (
            "ont",
            ["results/{GROUP}_clade.tsv.gz"],
            ["input/file.csv", "results/{GROUP}_val.tsv.gz"],
            ["{GROUP}_val.tsv.gz"],
        ),
        # No {GROUP} patterns returns empty
        (
            "illumina",
            ["input/file.csv", "results/output.tsv"],
            ["input/file.csv"],
            [],
        ),
        # JSON patterns are also extracted
        (
            "illumina",
            ["results/{GROUP}_clade.tsv.gz", "results/{GROUP}_fastp.json"],
            [],
            ["{GROUP}_clade.tsv.gz", "{GROUP}_fastp.json"],
        ),
    ])
    def test_extracts_patterns(self, tmp_path, platform, illumina, ont, expected):
        """Test extraction of patterns containing {GROUP}."""
        pyproject_path = tmp_path / "pyproject.toml"
        write_pyproject(pyproject_path, illumina, ont)
        assert get_group_output_patterns(str(pyproject_path), platform) == expected


#=============================================================================
# Tests for get_schema_name_from_pattern
#=============================================================================

class TestGetSchemaNameFromPattern:
    """Tests for get_schema_name_from_pattern function."""

    @pytest.mark.parametrize("pattern,expected", [
        ("{GROUP}_duplicate_stats.tsv.gz", "duplicate_stats"),
        ("{GROUP}_clade_counts.tsv.gz", "clade_counts"),
        ("{GROUP}_fastp.json", "fastp"),
        ("{GROUP}_validation_hits.tsv.gz", "validation_hits"),
    ])
    def test_extracts_schema_name(self, pattern, expected):
        """Test schema name extraction from various patterns."""
        assert get_schema_name_from_pattern(pattern) == expected


#=============================================================================
# Tests for load_empty_json
#=============================================================================

class TestLoadEmptyJson:
    """Tests for load_empty_json function."""

    def test_returns_empty_object_for_object_schema(self, tmp_path):
        """Test that '{}' is returned for JSON Schema with type: object."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
        }
        (tmp_path / "fastp.schema.json").write_text(json.dumps(schema))
        assert load_empty_json(tmp_path, "fastp") == "{}"

    def test_returns_empty_array_for_array_schema(self, tmp_path):
        """Test that '[]' is returned for JSON Schema with type: array."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "array",
        }
        (tmp_path / "items.schema.json").write_text(json.dumps(schema))
        assert load_empty_json(tmp_path, "items") == "[]"

    def test_returns_none_when_no_schema(self, tmp_path):
        """Test that None is returned when no schema file exists."""
        assert load_empty_json(tmp_path, "nonexistent") is None

    def test_returns_none_for_table_schema(self, tmp_path):
        """Test that None is returned for table-schema files (not JSON Schema)."""
        schema = {
            "$schema": "https://specs.frictionlessdata.io/schemas/table-schema.json",
            "fields": [{"name": "col1", "type": "string"}],
        }
        (tmp_path / "data.schema.json").write_text(json.dumps(schema))
        assert load_empty_json(tmp_path, "data") is None


#=============================================================================
# Tests for load_schema_headers
#=============================================================================

class TestLoadSchemaHeaders:
    """Tests for load_schema_headers function."""

    def test_returns_headers_when_schema_exists(self, tmp_path):
        """Test that headers are returned from a valid schema file."""
        schema = {
            "fields": [
                {"name": "col1", "type": "string"},
                {"name": "col2", "type": "integer"},
            ]
        }
        schema_path = tmp_path / "test.schema.json"
        schema_path.write_text(json.dumps(schema))
        result = load_schema_headers(tmp_path, "test")
        assert result == ["col1", "col2"]

    def test_returns_none_when_no_schema(self, tmp_path):
        """Test that None is returned when no schema file exists."""
        result = load_schema_headers(tmp_path, "nonexistent")
        assert result is None

    def test_returns_none_when_no_fields(self, tmp_path):
        """Test that None is returned when schema has no fields."""
        schema_path = tmp_path / "empty.schema.json"
        schema_path.write_text(json.dumps({"fields": []}))
        result = load_schema_headers(tmp_path, "empty")
        assert result is None


#=============================================================================
# Tests for create_empty_outputs
#=============================================================================

class TestCreateEmptyOutputs:
    """Tests for create_empty_outputs function."""

    @pytest.mark.parametrize("groups,patterns,expected_count", [
        # Multiple groups and patterns
        ({"g1", "g2"}, ["{GROUP}_a.tsv.gz", "{GROUP}_b.tsv.gz"], 4),
        # Single group, single pattern
        ({"g1"}, ["{GROUP}_a.tsv.gz"], 1),
        # Empty groups
        (set(), ["{GROUP}_a.tsv.gz"], 0),
        # Empty patterns
        ({"g1"}, [], 0),
    ])
    def test_creates_correct_number_of_files(self, tmp_path, groups, patterns, expected_count):
        """Test that correct number of files are created."""
        output_dir = tmp_path / "output"
        created = create_empty_outputs(groups, patterns, str(output_dir))
        assert len(created) == expected_count

    def test_files_are_empty_and_valid_gzip(self, tmp_path):
        """Test that created files are valid empty gzip files."""
        output_dir = tmp_path / "output"
        create_empty_outputs({"g1"}, ["{GROUP}_test.tsv.gz"], str(output_dir))

        filepath = output_dir / "g1_test.tsv.gz"
        assert filepath.exists()
        with gzip.open(filepath, "rt") as f:
            assert f.read() == ""

    def test_creates_nested_output_directory(self, tmp_path):
        """Test that nested output directory is created if needed."""
        output_dir = tmp_path / "nested" / "output"
        assert not output_dir.exists()
        create_empty_outputs({"g1"}, ["{GROUP}_test.tsv.gz"], str(output_dir))
        assert output_dir.exists()

    @pytest.mark.parametrize("use_schema_dir", [False, True], ids=["no_schema", "with_schema"])
    def test_mixed_tsv_and_json_patterns(self, tmp_path, use_schema_dir):
        """Test creating both TSV and JSON outputs together."""
        output_dir = tmp_path / "output"
        patterns = ["{GROUP}_data.tsv.gz", "{GROUP}_fastp.json"]
        schema_dir = None
        if use_schema_dir:
            schema_dir = tmp_path / "schemas"
            schema_dir.mkdir()
            table_schema = {"fields": [{"name": "col1"}, {"name": "col2"}]}
            (schema_dir / "data.schema.json").write_text(json.dumps(table_schema))
            json_schema = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
            }
            (schema_dir / "fastp.schema.json").write_text(json.dumps(json_schema))
        create_empty_outputs({"g1"}, patterns, str(output_dir), schema_dir)

        tsv_path = output_dir / "g1_data.tsv.gz"
        json_path = output_dir / "g1_fastp.json"
        assert tsv_path.exists()
        assert json_path.exists()

        with gzip.open(tsv_path, "rt") as f:
            content = f.read()
        if use_schema_dir:
            assert content == "col1\tcol2\n"
        else:
            assert content == ""
        # JSON should be plain-text (not gzipped) empty object
        assert json_path.read_text() == "{}"


#=============================================================================
# Tests for parse_args
#=============================================================================

class TestParseArgs:
    """Tests for parse_args function."""

    def test_parses_required_args(self, monkeypatch):
        """Test parsing of required arguments."""
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "g1,g2,g3", "pyproject.toml"],
        )
        args = parse_args()
        assert args.missing_groups == "g1,g2,g3"
        assert args.pyproject_toml == "pyproject.toml"
        assert args.output_dir == "./"  # default
        assert args.platform == "illumina"  # default
        assert args.pattern_filter is None  # default

    def test_parses_empty_groups(self, monkeypatch):
        """Test parsing of empty groups string."""
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "", "pyproject.toml"],
        )
        args = parse_args()
        assert args.missing_groups == ""

    @pytest.mark.parametrize("platform", ["illumina", "ont"])
    def test_parses_platform_option(self, monkeypatch, platform):
        """Test parsing of --platform option."""
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "g1,g2", "pyproject.toml", "--platform", platform],
        )
        args = parse_args()
        assert args.platform == platform

    def test_parses_output_dir_option(self, monkeypatch):
        """Test parsing of --output-dir option."""
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "g1,g2", "pyproject.toml", "--output-dir", "output/"],
        )
        args = parse_args()
        assert args.output_dir == "output/"

    def test_parses_pattern_filter_option(self, monkeypatch):
        """Test parsing of --pattern-filter option."""
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "g1,g2", "pyproject.toml", "--pattern-filter", "validation_hits"],
        )
        args = parse_args()
        assert args.pattern_filter == "validation_hits"


#=============================================================================
# Integration tests
#=============================================================================

class TestIntegration:
    """Integration tests for the full workflow."""

    @pytest.mark.parametrize("platform,expected_patterns", [
        ("illumina", ["{GROUP}_clade.tsv.gz", "{GROUP}_dup.tsv.gz"]),
        ("ont", ["{GROUP}_val.tsv.gz"]),
    ])
    def test_full_workflow(self, tmp_path, platform, expected_patterns):
        """Test the complete workflow from comma-separated groups to output files."""
        # Groups as comma-separated string (simulating Nextflow input)
        groups_str = "empty_g1,empty_g2"
        groups = set(g.strip() for g in groups_str.split(",") if g.strip())

        # Create pyproject.toml
        pyproject_path = tmp_path / "pyproject.toml"
        write_pyproject(
            pyproject_path,
            illumina_outputs=["input/f.csv", "results/{GROUP}_clade.tsv.gz", "results/{GROUP}_dup.tsv.gz"],
            ont_outputs=["input/f.csv", "results/{GROUP}_val.tsv.gz"],
        )

        # Run the workflow
        patterns = get_group_output_patterns(str(pyproject_path), platform)
        output_dir = tmp_path / "output"
        created = create_empty_outputs(groups, patterns, str(output_dir))

        # Verify
        assert groups == {"empty_g1", "empty_g2"}
        assert patterns == expected_patterns
        assert len(created) == len(groups) * len(patterns)

    def test_pattern_filter(self, tmp_path):
        """Test that pattern_filter correctly filters output patterns."""
        groups = {"g1"}

        # Create pyproject.toml with multiple patterns
        pyproject_path = tmp_path / "pyproject.toml"
        write_pyproject(
            pyproject_path,
            illumina_outputs=[
                "results/{GROUP}_clade.tsv.gz",
                "results/{GROUP}_dup.tsv.gz",
                "results/{GROUP}_validation_hits.tsv.gz",
            ],
            ont_outputs=[],
        )

        # Get patterns and filter
        patterns = get_group_output_patterns(str(pyproject_path), "illumina")
        filtered_patterns = [p for p in patterns if "validation_hits" in p]

        # Run the workflow with filtered patterns
        output_dir = tmp_path / "output"
        created = create_empty_outputs(groups, filtered_patterns, str(output_dir))

        # Verify only validation_hits file was created
        assert len(created) == 1
        assert "g1_validation_hits.tsv.gz" in created[0]

    def test_mixed_tsv_and_json_integration(self, tmp_path):
        """Test full workflow with both TSV and JSON patterns."""
        groups = {"empty_g1"}

        # Create pyproject.toml with both TSV and JSON patterns
        pyproject_path = tmp_path / "pyproject.toml"
        write_pyproject(
            pyproject_path,
            illumina_outputs=[
                "results/{GROUP}_clade.tsv.gz",
                "results/{GROUP}_fastp.json",
            ],
            ont_outputs=[],
        )

        # Run the workflow
        patterns = get_group_output_patterns(str(pyproject_path), "illumina")
        output_dir = tmp_path / "output"
        created = create_empty_outputs(groups, patterns, str(output_dir))

        # Verify both files created
        assert len(created) == 2

        # TSV should be empty gzip
        tsv_path = output_dir / "empty_g1_clade.tsv.gz"
        assert tsv_path.exists()
        with gzip.open(tsv_path, "rt") as f:
            assert f.read() == ""

        # JSON should have empty object
        json_path = output_dir / "empty_g1_fastp.json"
        assert json_path.exists()
        with open(json_path) as f:
            data = json.load(f)
        assert data == {}
