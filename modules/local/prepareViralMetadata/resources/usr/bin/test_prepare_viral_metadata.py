"""Tests for prepare-viral-metadata.py."""

# Import the module under test — it uses a hyphenated filename,
# so we import via importlib to handle the non-standard name.
import importlib
import importlib.util
from pathlib import Path

import pandas as pd
import pytest

_SCRIPT_DIR = Path(__file__).parent
_SPEC = importlib.util.spec_from_file_location(
    "prepare_viral_metadata", _SCRIPT_DIR / "prepare-viral-metadata.py",
)
pvm = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pvm)


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def genome_dir(tmp_path: Path) -> Path:
    """Create a temporary genome directory with dummy .fna.gz files."""
    gdir = tmp_path / "genomes"
    gdir.mkdir()
    # Create dummy genome files named by accession
    for acc in ["GCA_000001.1", "GCA_000002.1", "GCA_000003.1"]:
        (gdir / f"{acc}_genomic.fna.gz").write_bytes(b"dummy")
    return gdir


@pytest.fixture()
def merged_metadata_path(tmp_path: Path) -> Path:
    """Create a merged metadata TSV file."""
    path = tmp_path / "merged_metadata.tsv"
    df = pd.DataFrame(
        {
            "assembly_accession": ["GCA_000001.1", "GCA_000002.1", "GCA_000003.1"],
            "taxid": ["12345", "67890", "99999"],
            "organism_name": ["Virus A", "Virus B", "Virus C"],
            "source_database": ["GenBank", "GenBank", "RefSeq"],
        },
    )
    df.to_csv(path, sep="\t", index=False)
    return path


@pytest.fixture()
def virus_db_path(tmp_path: Path) -> Path:
    """Create a virus taxonomy DB TSV with taxid_species column."""
    path = tmp_path / "virus_db.tsv"
    df = pd.DataFrame(
        {
            "taxid": ["12345", "67890", "99999", "11111"],
            "taxid_species": ["12345", "67000", "99000", "11000"],
            "name": ["Virus A", "Virus B", "Virus C", "Virus D"],
        },
    )
    df.to_csv(path, sep="\t", index=False)
    return path


# ─── Tests for match_genomes_to_accessions ────────────────────────────────────


class TestMatchGenomesToAccessions:
    """Tests for the match_genomes_to_accessions function."""

    def test_matches_by_prefix(self, genome_dir: Path) -> None:
        accessions = ["GCA_000001.1", "GCA_000002.1"]
        result = pvm.match_genomes_to_accessions(genome_dir, accessions)
        assert result == {
            "GCA_000001.1": "GCA_000001.1_genomic.fna.gz",
            "GCA_000002.1": "GCA_000002.1_genomic.fna.gz",
        }

    def test_missing_accession_excluded(self, genome_dir: Path) -> None:
        accessions = ["GCA_000001.1", "GCA_999999.1"]
        result = pvm.match_genomes_to_accessions(genome_dir, accessions)
        assert "GCA_999999.1" not in result
        assert "GCA_000001.1" in result

    def test_empty_directory(self, tmp_path: Path) -> None:
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        result = pvm.match_genomes_to_accessions(empty_dir, ["GCA_000001.1"])
        assert result == {}

    def test_empty_accessions(self, genome_dir: Path) -> None:
        result = pvm.match_genomes_to_accessions(genome_dir, [])
        assert result == {}


# ─── Tests for prepare_metadata ───────────────────────────────────────────────


class TestPrepareMetadata:
    """Tests for the prepare_metadata function."""

    def test_basic_metadata_preparation(
        self,
        tmp_path: Path,
        merged_metadata_path: Path,
        virus_db_path: Path,
        genome_dir: Path,
    ) -> None:
        output_meta = str(tmp_path / "output_metadata.txt")
        output_genomes = str(tmp_path / "ncbi_genomes")

        pvm.prepare_metadata(
            str(merged_metadata_path),
            str(virus_db_path),
            str(genome_dir),
            output_meta,
            output_genomes,
        )

        # Check output metadata exists and has expected columns
        result = pd.read_csv(output_meta, sep="\t", dtype=str)
        assert "assembly_accession" in result.columns
        assert "taxid" in result.columns
        assert "species_taxid" in result.columns
        assert "local_filename" in result.columns
        assert len(result) == 3

    def test_species_taxid_mapping(
        self,
        tmp_path: Path,
        merged_metadata_path: Path,
        virus_db_path: Path,
        genome_dir: Path,
    ) -> None:
        output_meta = str(tmp_path / "output_metadata.txt")
        output_genomes = str(tmp_path / "ncbi_genomes")

        pvm.prepare_metadata(
            str(merged_metadata_path),
            str(virus_db_path),
            str(genome_dir),
            output_meta,
            output_genomes,
        )

        result = pd.read_csv(output_meta, sep="\t", dtype=str)
        species_map = dict(zip(result["taxid"], result["species_taxid"], strict=False))
        assert species_map["12345"] == "12345"
        assert species_map["67890"] == "67000"
        assert species_map["99999"] == "99000"

    def test_local_filename_points_to_genomes_dir(
        self,
        tmp_path: Path,
        merged_metadata_path: Path,
        virus_db_path: Path,
        genome_dir: Path,
    ) -> None:
        output_meta = str(tmp_path / "output_metadata.txt")
        output_genomes_dir = str(tmp_path / "ncbi_genomes")

        pvm.prepare_metadata(
            str(merged_metadata_path),
            str(virus_db_path),
            str(genome_dir),
            output_meta,
            output_genomes_dir,
        )

        result = pd.read_csv(output_meta, sep="\t", dtype=str)
        for _, row in result.iterrows():
            assert row["local_filename"].startswith(output_genomes_dir)
            assert row["local_filename"].endswith(".fna.gz")

    def test_genome_symlinks_created(
        self,
        tmp_path: Path,
        merged_metadata_path: Path,
        virus_db_path: Path,
        genome_dir: Path,
    ) -> None:
        output_meta = str(tmp_path / "output_metadata.txt")
        output_genomes = str(tmp_path / "ncbi_genomes")

        pvm.prepare_metadata(
            str(merged_metadata_path),
            str(virus_db_path),
            str(genome_dir),
            output_meta,
            output_genomes,
        )

        output_genomes_path = Path(output_genomes)
        assert output_genomes_path.exists()
        genome_files = list(output_genomes_path.glob("*.fna.gz"))
        assert len(genome_files) == 3
        # Verify they are symlinks
        for f in genome_files:
            assert f.is_symlink()

    def test_missing_genome_files_dropped(
        self, tmp_path: Path, virus_db_path: Path,
    ) -> None:
        """Rows for accessions without genome files are dropped."""
        # Create metadata with one accession that has no genome file
        meta_path = tmp_path / "meta.tsv"
        df = pd.DataFrame(
            {
                "assembly_accession": ["GCA_000001.1", "GCA_MISSING.1"],
                "taxid": ["12345", "67890"],
                "organism_name": ["Virus A", "Virus B"],
                "source_database": ["GenBank", "GenBank"],
            },
        )
        df.to_csv(meta_path, sep="\t", index=False)

        # Create genome dir with only one file
        gdir = tmp_path / "genomes"
        gdir.mkdir()
        (gdir / "GCA_000001.1_genomic.fna.gz").write_bytes(b"dummy")

        output_meta = str(tmp_path / "output_metadata.txt")
        output_genomes = str(tmp_path / "ncbi_genomes")

        pvm.prepare_metadata(
            str(meta_path),
            str(virus_db_path),
            str(gdir),
            output_meta,
            output_genomes,
        )

        result = pd.read_csv(output_meta, sep="\t", dtype=str)
        assert len(result) == 1
        assert result.iloc[0]["assembly_accession"] == "GCA_000001.1"

    def test_unmapped_species_taxid_is_nan(
        self, tmp_path: Path, genome_dir: Path,
    ) -> None:
        """Taxids not in virus_db get NaN for species_taxid."""
        meta_path = tmp_path / "meta.tsv"
        df = pd.DataFrame(
            {
                "assembly_accession": ["GCA_000001.1"],
                "taxid": ["00000"],  # not in virus_db
                "organism_name": ["Unknown Virus"],
                "source_database": ["GenBank"],
            },
        )
        df.to_csv(meta_path, sep="\t", index=False)

        virus_db = tmp_path / "virus_db.tsv"
        pd.DataFrame(
            {"taxid": ["12345"], "taxid_species": ["12345"], "name": ["V"]},
        ).to_csv(virus_db, sep="\t", index=False)

        output_meta = str(tmp_path / "output_metadata.txt")
        output_genomes = str(tmp_path / "ncbi_genomes")

        pvm.prepare_metadata(
            str(meta_path),
            str(virus_db),
            str(genome_dir),
            output_meta,
            output_genomes,
        )

        result = pd.read_csv(output_meta, sep="\t", dtype=str)
        assert len(result) == 1
        # species_taxid should be empty/NaN (written as empty string in TSV)
        assert pd.isna(result.iloc[0]["species_taxid"]) or result.iloc[0]["species_taxid"] == ""

    def test_gzipped_metadata_input(
        self,
        tmp_path: Path,
        virus_db_path: Path,
        genome_dir: Path,
    ) -> None:
        """Verify that gzipped metadata input is handled correctly."""
        # pandas handles gzip compression/decompression transparently
        # based on the .gz file suffix, so no special handling is needed
        meta_path = tmp_path / "meta.tsv.gz"
        df = pd.DataFrame(
            {
                "assembly_accession": ["GCA_000001.1"],
                "taxid": ["12345"],
                "organism_name": ["Virus A"],
                "source_database": ["GenBank"],
            },
        )
        df.to_csv(meta_path, sep="\t", index=False)

        output_meta = str(tmp_path / "output_metadata.txt")
        output_genomes = str(tmp_path / "ncbi_genomes")

        pvm.prepare_metadata(
            str(meta_path),
            str(virus_db_path),
            str(genome_dir),
            output_meta,
            output_genomes,
        )

        result = pd.read_csv(output_meta, sep="\t", dtype=str)
        assert len(result) == 1
