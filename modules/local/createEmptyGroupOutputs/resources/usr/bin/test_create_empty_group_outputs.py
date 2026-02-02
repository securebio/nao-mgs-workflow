import gzip
import pytest
from create_empty_group_outputs import (
    open_by_suffix,
    get_group_output_patterns,
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
    ])
    def test_extracts_patterns(self, tmp_path, platform, illumina, ont, expected):
        """Test extraction of patterns containing {GROUP}."""
        pyproject_path = tmp_path / "pyproject.toml"
        write_pyproject(pyproject_path, illumina, ont)
        assert get_group_output_patterns(str(pyproject_path), platform) == expected


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


#=============================================================================
# Tests for parse_args
#=============================================================================

class TestParseArgs:
    """Tests for parse_args function."""

    def test_parses_required_args(self, monkeypatch):
        """Test parsing of required arguments."""
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "g1,g2,g3", "pyproject.toml", "output/"],
        )
        args = parse_args()
        assert args.missing_groups == "g1,g2,g3"
        assert args.pyproject_toml == "pyproject.toml"
        assert args.output_dir == "output/"
        assert args.platform == "illumina"  # default

    def test_parses_empty_groups(self, monkeypatch):
        """Test parsing of empty groups string."""
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "", "pyproject.toml", "output/"],
        )
        args = parse_args()
        assert args.missing_groups == ""

    @pytest.mark.parametrize("platform", ["illumina", "ont"])
    def test_parses_platform_option(self, monkeypatch, platform):
        """Test parsing of --platform option."""
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "g1,g2", "pyproject.toml", "output/", "--platform", platform],
        )
        args = parse_args()
        assert args.platform == platform


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
