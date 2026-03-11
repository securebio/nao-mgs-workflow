"""Tests for prepare_viral_metadata.py."""

import csv
import gzip
from pathlib import Path

import pytest

from prepare_viral_metadata import (
    build_species_taxid_map,
    match_genomes_to_accessions,
    prepare_metadata,
    symlink_genomes,
)

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _write_tsv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        w.writerows(rows)
    return path


def _make_genome_dir(tmp_path: Path, accessions: list[str]) -> Path:
    gdir = tmp_path / "genomes"
    gdir.mkdir(parents=True)
    for acc in accessions:
        (gdir / f"{acc}_genomic.fna.gz").write_bytes(b"dummy")
    return gdir


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with open(path) as f:
        return list(csv.DictReader(f, delimiter="\t"))


# ─── Shared constants ────────────────────────────────────────────────────────

ACCS = ["GCA_000001.1", "GCA_000002.1", "GCA_000003.1"]
META_HEADER = ["assembly_accession", "taxid", "organism_name", "source_database"]
META_ROWS = [
    ["GCA_000001.1", "12345", "Virus A", "GenBank"],
    ["GCA_000002.1", "67890", "Virus B", "GenBank"],
    ["GCA_000003.1", "99999", "Virus C", "RefSeq"],
]
DB_HEADER = ["taxid", "taxid_species", "name"]
DB_ROWS = [
    ["12345", "12345", "Virus A"],
    ["67890", "67000", "Virus B"],
    ["99999", "99000", "Virus C"],
]


@pytest.fixture()
def standard_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Return (metadata_path, virus_db_path, genome_dir) with 3 accessions."""
    meta = _write_tsv(tmp_path / "meta.tsv", META_HEADER, META_ROWS)
    db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, DB_ROWS)
    gdir = _make_genome_dir(tmp_path, ACCS)
    return meta, db, gdir


# ─── Tests for build_species_taxid_map ────────────────────────────────────────


class TestBuildSpeciesTaxidMap:
    def test_builds_map(self, tmp_path: Path) -> None:
        db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, DB_ROWS)
        result = build_species_taxid_map(str(db))
        assert result == {"12345": "12345", "67890": "67000", "99999": "99000"}

    def test_empty_db(self, tmp_path: Path) -> None:
        db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, [])
        assert build_species_taxid_map(str(db)) == {}


# ─── Tests for match_genomes_to_accessions ────────────────────────────────────


class TestMatchGenomesToAccessions:
    @pytest.mark.parametrize(
        ("accessions", "dir_contents", "expected"),
        [
            (["GCA_000001.1", "GCA_000002.1"], ["GCA_000001.1", "GCA_000002.1"],
             {"GCA_000001.1": "GCA_000001.1_genomic.fna.gz",
              "GCA_000002.1": "GCA_000002.1_genomic.fna.gz"}),
            (["GCA_000001.1", "GCA_MISSING.1"], ["GCA_000001.1"],
             {"GCA_000001.1": "GCA_000001.1_genomic.fna.gz"}),
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


# ─── Tests for symlink_genomes ────────────────────────────────────────────────


class TestSymlinkGenomes:
    def test_creates_symlinks(self, tmp_path: Path) -> None:
        gdir = _make_genome_dir(tmp_path, ["GCA_000001.1"])
        out = tmp_path / "out"
        symlink_genomes({"GCA_000001.1": "GCA_000001.1_genomic.fna.gz"}, gdir, out)
        result = list(out.glob("*.fna.gz"))
        assert len(result) == 1
        assert result[0].is_symlink()

    def test_empty_mapping(self, tmp_path: Path) -> None:
        gdir = _make_genome_dir(tmp_path, [])
        out = tmp_path / "out"
        symlink_genomes({}, gdir, out)
        assert out.exists()
        assert list(out.iterdir()) == []


# ─── Tests for prepare_metadata ───────────────────────────────────────────────


class TestPrepareMetadata:
    def test_standard_output(self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]) -> None:
        meta_path, db_path, gdir = standard_inputs
        out_meta = str(tmp_path / "output.txt")
        out_genomes = str(tmp_path / "ncbi_genomes")
        prepare_metadata(str(meta_path), str(db_path), str(gdir), out_meta, out_genomes)
        rows = _read_tsv(Path(out_meta))
        assert len(rows) == 3
        assert all(k in rows[0] for k in ("species_taxid", "local_filename"))
        species = {r["taxid"]: r["species_taxid"] for r in rows}
        assert species == {"12345": "12345", "67890": "67000", "99999": "99000"}
        assert all(r["local_filename"].startswith(out_genomes) for r in rows)
        assert len(list(Path(out_genomes).glob("*.fna.gz"))) == 3

    def test_missing_genome_drops_row(self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]) -> None:
        _, db_path, _ = standard_inputs
        meta = _write_tsv(tmp_path / "m.tsv", META_HEADER, [META_ROWS[0], ["GCA_MISSING.1", "67890", "B", "GenBank"]])
        gdir = _make_genome_dir(tmp_path / "sub", ["GCA_000001.1"])
        out_meta = str(tmp_path / "out.txt")
        prepare_metadata(str(meta), str(db_path), str(gdir), out_meta, str(tmp_path / "g"))
        rows = _read_tsv(Path(out_meta))
        assert len(rows) == 1
        assert rows[0]["assembly_accession"] == "GCA_000001.1"

    def test_unmapped_taxid_gives_empty_species(self, tmp_path: Path) -> None:
        meta = _write_tsv(tmp_path / "m.tsv", META_HEADER, [["GCA_000001.1", "00000", "X", "GenBank"]])
        db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, [["12345", "12345", "V"]])
        gdir = _make_genome_dir(tmp_path, ["GCA_000001.1"])
        out_meta = str(tmp_path / "out.txt")
        prepare_metadata(str(meta), str(db), str(gdir), out_meta, str(tmp_path / "g"))
        rows = _read_tsv(Path(out_meta))
        assert rows[0]["species_taxid"] == ""

    def test_gzipped_metadata_input(self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]) -> None:
        _, db_path, gdir = standard_inputs
        meta_gz = tmp_path / "meta.tsv.gz"
        with gzip.open(meta_gz, "wt", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(META_HEADER)
            w.writerows(META_ROWS)
        out_meta = str(tmp_path / "out.txt")
        prepare_metadata(str(meta_gz), str(db_path), str(gdir), out_meta, str(tmp_path / "g"))
        assert len(_read_tsv(Path(out_meta))) == 3
