"""Tests for prepare_viral_metadata.py."""

from pathlib import Path

import pandas as pd
import pytest

from prepare_viral_metadata import match_genomes_to_accessions, prepare_metadata


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _write_tsv(path: Path, df: pd.DataFrame) -> Path:
    df.to_csv(path, sep="\t", index=False)
    return path


def _make_genome_dir(tmp_path: Path, accessions: list[str]) -> Path:
    gdir = tmp_path / "genomes"
    gdir.mkdir(parents=True)
    for acc in accessions:
        (gdir / f"{acc}_genomic.fna.gz").write_bytes(b"dummy")
    return gdir


# ─── Shared fixtures ─────────────────────────────────────────────────────────


ACCESSIONS = ["GCA_000001.1", "GCA_000002.1", "GCA_000003.1"]

METADATA_DF = pd.DataFrame({
    "assembly_accession": ACCESSIONS,
    "taxid": ["12345", "67890", "99999"],
    "organism_name": ["Virus A", "Virus B", "Virus C"],
    "source_database": ["GenBank", "GenBank", "RefSeq"],
})

VIRUS_DB_DF = pd.DataFrame({
    "taxid": ["12345", "67890", "99999", "11111"],
    "taxid_species": ["12345", "67000", "99000", "11000"],
    "name": ["Virus A", "Virus B", "Virus C", "Virus D"],
})


@pytest.fixture()
def standard_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Return (metadata_path, virus_db_path, genome_dir) with 3 accessions."""
    meta = _write_tsv(tmp_path / "merged_metadata.tsv", METADATA_DF)
    db = _write_tsv(tmp_path / "virus_db.tsv", VIRUS_DB_DF)
    gdir = _make_genome_dir(tmp_path, ACCESSIONS)
    return meta, db, gdir


# ─── Tests for match_genomes_to_accessions ────────────────────────────────────


class TestMatchGenomesToAccessions:
    """Tests for the match_genomes_to_accessions function."""

    @pytest.mark.parametrize(
        ("accessions", "dir_contents", "expected"),
        [
            (
                ["GCA_000001.1", "GCA_000002.1"],
                ["GCA_000001.1", "GCA_000002.1"],
                {"GCA_000001.1": "GCA_000001.1_genomic.fna.gz",
                 "GCA_000002.1": "GCA_000002.1_genomic.fna.gz"},
            ),
            (
                ["GCA_000001.1", "GCA_MISSING.1"],
                ["GCA_000001.1"],
                {"GCA_000001.1": "GCA_000001.1_genomic.fna.gz"},
            ),
            (["GCA_000001.1"], [], {}),
            ([], ["GCA_000001.1"], {}),
        ],
        ids=["all_match", "partial_match", "empty_dir", "empty_accessions"],
    )
    def test_match_genomes(
        self, tmp_path: Path, accessions: list[str],
        dir_contents: list[str], expected: dict[str, str],
    ) -> None:
        gdir = _make_genome_dir(tmp_path, dir_contents)
        assert match_genomes_to_accessions(gdir, accessions) == expected


# ─── Tests for prepare_metadata ───────────────────────────────────────────────


class TestPrepareMetadata:
    """Tests for the prepare_metadata function."""

    def test_standard_output(
        self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path],
    ) -> None:
        """Full pipeline: columns, species_taxid mapping, filenames, symlinks."""
        meta_path, db_path, gdir = standard_inputs
        out_meta = str(tmp_path / "output.txt")
        out_genomes = str(tmp_path / "ncbi_genomes")

        prepare_metadata(str(meta_path), str(db_path), str(gdir), out_meta, out_genomes)

        result = pd.read_csv(out_meta, sep="\t", dtype=str)
        # Correct columns and row count
        assert set(result.columns) >= {"assembly_accession", "taxid", "species_taxid", "local_filename"}
        assert len(result) == 3
        # species_taxid mapped correctly
        species_map = dict(zip(result["taxid"], result["species_taxid"], strict=False))
        assert species_map == {"12345": "12345", "67890": "67000", "99999": "99000"}
        # local_filename points into output genomes dir
        for fname in result["local_filename"]:
            assert fname.startswith(out_genomes) and fname.endswith(".fna.gz")
        # Symlinks created
        symlinks = list(Path(out_genomes).glob("*.fna.gz"))
        assert len(symlinks) == 3
        assert all(s.is_symlink() for s in symlinks)

    def test_missing_genome_drops_row(
        self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path],
    ) -> None:
        """Rows for accessions without genome files are dropped."""
        _, db_path, _ = standard_inputs
        meta = _write_tsv(tmp_path / "meta.tsv", pd.DataFrame({
            "assembly_accession": ["GCA_000001.1", "GCA_MISSING.1"],
            "taxid": ["12345", "67890"],
            "organism_name": ["A", "B"],
            "source_database": ["GenBank", "GenBank"],
        }))
        gdir = _make_genome_dir(tmp_path / "sub", ["GCA_000001.1"])

        out_meta = str(tmp_path / "out.txt")
        prepare_metadata(str(meta), str(db_path), str(gdir), out_meta, str(tmp_path / "g"))

        result = pd.read_csv(out_meta, sep="\t", dtype=str)
        assert len(result) == 1
        assert result.iloc[0]["assembly_accession"] == "GCA_000001.1"

    def test_unmapped_taxid_gives_nan_species(self, tmp_path: Path) -> None:
        """Taxids not in virus_db get NaN for species_taxid."""
        meta = _write_tsv(tmp_path / "meta.tsv", pd.DataFrame({
            "assembly_accession": ["GCA_000001.1"],
            "taxid": ["00000"],
            "organism_name": ["Unknown"],
            "source_database": ["GenBank"],
        }))
        db = _write_tsv(tmp_path / "db.tsv", pd.DataFrame({
            "taxid": ["12345"], "taxid_species": ["12345"], "name": ["V"],
        }))
        gdir = _make_genome_dir(tmp_path, ["GCA_000001.1"])

        out_meta = str(tmp_path / "out.txt")
        prepare_metadata(str(meta), str(db), str(gdir), out_meta, str(tmp_path / "g"))

        result = pd.read_csv(out_meta, sep="\t", dtype=str)
        assert pd.isna(result.iloc[0]["species_taxid"]) or result.iloc[0]["species_taxid"] == ""

    def test_gzipped_metadata_input(
        self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path],
    ) -> None:
        """Gzipped metadata is handled transparently by pandas."""
        _, db_path, gdir = standard_inputs
        meta_gz = tmp_path / "meta.tsv.gz"
        METADATA_DF.to_csv(meta_gz, sep="\t", index=False)

        out_meta = str(tmp_path / "out.txt")
        prepare_metadata(str(meta_gz), str(db_path), str(gdir), out_meta, str(tmp_path / "g"))

        result = pd.read_csv(out_meta, sep="\t", dtype=str)
        assert len(result) == 3
