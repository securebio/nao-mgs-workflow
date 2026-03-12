"""Tests for prepare_viral_metadata.py."""

import csv
import gzip
from pathlib import Path

import pytest
from prepare_viral_metadata import build_species_taxid_map, match_genomes_to_accessions, prepare_metadata

ACCS = ["GCA_000001.1", "GCA_000002.1", "GCA_000003.1"]
META_HEADER = ["assembly_accession", "taxid", "organism_name", "source_database"]
META_ROWS = [
    ["GCA_000001.1", "12345", "Virus A", "GenBank"],
    ["GCA_000002.1", "67890", "Virus B", "GenBank"],
    ["GCA_000003.1", "99999", "Virus C", "RefSeq"],
]
DB_HEADER = ["taxid", "taxid_species", "name"]
DB_ROWS = [["12345", "12345", "Virus A"], ["67890", "67000", "Virus B"], ["99999", "99000", "Virus C"]]

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

def _run_prepare(tmp_path, meta_path, db_path, gdir) -> list[dict[str, str]]:
    """Run prepare_metadata and return output rows."""
    out_meta, out_genomes = str(tmp_path / "out.txt"), str(tmp_path / "ncbi_genomes")
    prepare_metadata(str(meta_path), str(db_path), str(gdir), out_meta, out_genomes)
    return _read_tsv(Path(out_meta))

@pytest.fixture()
def standard_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    meta = _write_tsv(tmp_path / "meta.tsv", META_HEADER, META_ROWS)
    db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, DB_ROWS)
    return meta, db, _make_genome_dir(tmp_path, ACCS)

class TestBuildSpeciesTaxidMap:
    @pytest.mark.parametrize(("db_rows", "expected"), [
        (DB_ROWS, {"12345": "12345", "67890": "67000", "99999": "99000"}),
        ([], {}),
    ], ids=["populated", "empty"])
    def test_builds_map(self, tmp_path: Path, db_rows: list, expected: dict) -> None:
        db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, db_rows)
        assert build_species_taxid_map(str(db)) == expected

class TestMatchGenomesToAccessions:
    @pytest.mark.parametrize(("accessions", "dir_contents", "expected"), [
        (["GCA_000001.1", "GCA_000002.1"], ["GCA_000001.1", "GCA_000002.1"],
         {"GCA_000001.1": "GCA_000001.1_genomic.fna.gz", "GCA_000002.1": "GCA_000002.1_genomic.fna.gz"}),
        (["GCA_000001.1", "GCA_MISSING.1"], ["GCA_000001.1"],
         {"GCA_000001.1": "GCA_000001.1_genomic.fna.gz"}),
        (["GCA_000001.1"], [], {}),
        ([], ["GCA_000001.1"], {}),
    ], ids=["all_match", "partial_match", "empty_dir", "empty_accessions"])
    def test_match(self, tmp_path: Path, accessions: list[str],
                   dir_contents: list[str], expected: dict[str, str]) -> None:
        assert match_genomes_to_accessions(_make_genome_dir(tmp_path, dir_contents), accessions) == expected

class TestPrepareMetadata:
    def test_standard_output(self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]) -> None:
        meta_path, db_path, gdir = standard_inputs
        out_genomes = str(tmp_path / "ncbi_genomes")
        out_meta = str(tmp_path / "out.txt")
        prepare_metadata(str(meta_path), str(db_path), str(gdir), out_meta, out_genomes)
        rows = _read_tsv(Path(out_meta))
        assert len(rows) == 3
        assert {r["taxid"]: r["species_taxid"] for r in rows} == {"12345": "12345", "67890": "67000", "99999": "99000"}
        assert all(r["local_filename"].startswith(out_genomes) for r in rows)
        symlinks = list(Path(out_genomes).glob("*.fna.gz"))
        assert len(symlinks) == 3 and all(s.is_symlink() for s in symlinks)

    def test_empty_metadata_writes_header_only(self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]) -> None:
        _, db_path, gdir = standard_inputs
        meta = _write_tsv(tmp_path / "empty.tsv", META_HEADER, [])
        out_meta = str(tmp_path / "out.txt")
        out_genomes = str(tmp_path / "ncbi_genomes")
        prepare_metadata(str(meta), str(db_path), str(gdir), out_meta, out_genomes)
        rows = _read_tsv(Path(out_meta))
        assert len(rows) == 0
        with open(out_meta) as f:
            header = f.readline().strip().split("\t")
        assert header == META_HEADER + ["species_taxid", "local_filename"]

    def test_missing_genome_drops_row(self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]) -> None:
        _, db_path, _ = standard_inputs
        meta = _write_tsv(tmp_path / "m.tsv", META_HEADER, [META_ROWS[0], ["GCA_MISSING.1", "67890", "B", "GenBank"]])
        gdir = _make_genome_dir(tmp_path / "sub", ["GCA_000001.1"])
        rows = _run_prepare(tmp_path / "run", meta, db_path, gdir)
        assert len(rows) == 1 and rows[0]["assembly_accession"] == "GCA_000001.1"

    def test_unmapped_taxid_gives_empty_species(self, tmp_path: Path) -> None:
        meta = _write_tsv(tmp_path / "m.tsv", META_HEADER, [["GCA_000001.1", "00000", "X", "GenBank"]])
        db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, [["12345", "12345", "V"]])
        rows = _run_prepare(tmp_path / "run", meta, db, _make_genome_dir(tmp_path, ["GCA_000001.1"]))
        assert rows[0]["species_taxid"] == ""

    def test_gzipped_metadata_input(self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]) -> None:
        _, db_path, gdir = standard_inputs
        meta_gz = tmp_path / "meta.tsv.gz"
        with gzip.open(meta_gz, "wt", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(META_HEADER)
            w.writerows(META_ROWS)
        rows = _run_prepare(tmp_path / "run", meta_gz, db_path, gdir)
        assert len(rows) == 3
