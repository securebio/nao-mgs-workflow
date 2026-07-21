"""Tests for prepare_viral_metadata.py."""

import csv
import gzip
from pathlib import Path

import pytest
from prepare_viral_metadata import (
    build_species_taxid_map,
    prepare_metadata,
    read_accession_map,
)

META_HEADER = [
    "assembly_accession",
    "taxid",
    "organism_name",
    "source_database",
    "assembly_status",
]
META_ROWS = [
    ["GCA_000001.1", "12345", "Virus A", "GenBank", "current"],
    ["GCA_000002.1", "67890", "Virus B", "GenBank", "current"],
    ["GCA_000003.1", "99999", "Virus C", "RefSeq", "current"],
]
DB_HEADER = ["taxid", "taxid_species", "name"]
DB_ROWS = [
    ["12345", "12345", "Virus A"],
    ["67890", "67000", "Virus B"],
    ["99999", "99000", "Virus C"],
]
MAP_HEADER = ["assembly_accession", "genome_id"]
# GCA_000002.1 is multi-segment (two genome IDs); the others are single.
MAP_ROWS = [
    ["GCA_000001.1", "seq1.1"],
    ["GCA_000002.1", "seq2a.1"],
    ["GCA_000002.1", "seq2b.1"],
    ["GCA_000003.1", "seq3.1"],
]


def _write_tsv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        w.writerows(rows)
    return path


def _read_tsv(path: Path) -> list[dict[str, str]]:
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _run_prepare(
    tmp_path: Path, meta_path: Path, db_path: Path, map_path: Path
) -> list[dict[str, str]]:
    """Run prepare_metadata and return the output rows."""
    out_meta = str(tmp_path / "out.tsv.gz")
    prepare_metadata(str(meta_path), str(db_path), str(map_path), out_meta)
    return _read_tsv(Path(out_meta))


@pytest.fixture()
def standard_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    meta = _write_tsv(tmp_path / "meta.tsv", META_HEADER, META_ROWS)
    db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, DB_ROWS)
    amap = _write_tsv(tmp_path / "map.tsv", MAP_HEADER, MAP_ROWS)
    return meta, db, amap


class TestBuildSpeciesTaxidMap:
    @pytest.mark.parametrize(
        ("db_rows", "expected"),
        [
            (DB_ROWS, {"12345": "12345", "67890": "67000", "99999": "99000"}),
            ([], {}),
        ],
        ids=["populated", "empty"],
    )
    def test_builds_map(self, tmp_path: Path, db_rows: list, expected: dict) -> None:
        db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, db_rows)
        assert build_species_taxid_map(str(db)) == expected

    def test_gzipped_db(self, tmp_path: Path) -> None:
        """Gzipped virus DB is read correctly via open_by_suffix."""
        db_gz = tmp_path / "db.tsv.gz"
        with gzip.open(db_gz, "wt", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(DB_HEADER)
            w.writerows(DB_ROWS)
        assert build_species_taxid_map(str(db_gz)) == {
            "12345": "12345",
            "67890": "67000",
            "99999": "99000",
        }


class TestReadAccessionMap:
    def test_groups_genome_ids_by_accession_in_order(self, tmp_path: Path) -> None:
        amap = _write_tsv(tmp_path / "map.tsv", MAP_HEADER, MAP_ROWS)
        assert read_accession_map(str(amap)) == {
            "GCA_000001.1": ["seq1.1"],
            "GCA_000002.1": ["seq2a.1", "seq2b.1"],
            "GCA_000003.1": ["seq3.1"],
        }

    def test_empty_map(self, tmp_path: Path) -> None:
        amap = _write_tsv(tmp_path / "map.tsv", MAP_HEADER, [])
        assert read_accession_map(str(amap)) == {}

    def test_gzipped_map(self, tmp_path: Path) -> None:
        amap_gz = tmp_path / "map.tsv.gz"
        with gzip.open(amap_gz, "wt", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(MAP_HEADER)
            w.writerows(MAP_ROWS)
        assert read_accession_map(str(amap_gz)) == {
            "GCA_000001.1": ["seq1.1"],
            "GCA_000002.1": ["seq2a.1", "seq2b.1"],
            "GCA_000003.1": ["seq3.1"],
        }


class TestPrepareMetadata:
    def test_expands_to_one_row_per_genome_id(
        self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]
    ) -> None:
        meta_path, db_path, map_path = standard_inputs
        rows = _run_prepare(tmp_path, meta_path, db_path, map_path)
        # 4 genome IDs total (GCA_000002.1 contributes two).
        assert len(rows) == 4
        assert [r["genome_id"] for r in rows] == [
            "seq1.1",
            "seq2a.1",
            "seq2b.1",
            "seq3.1",
        ]
        # species_taxid joined from the virus DB; local_filename is gone.
        assert {r["genome_id"]: r["species_taxid"] for r in rows} == {
            "seq1.1": "12345",
            "seq2a.1": "67000",
            "seq2b.1": "67000",
            "seq3.1": "99000",
        }
        assert "local_filename" not in rows[0]

    def test_output_columns(
        self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]
    ) -> None:
        meta_path, db_path, map_path = standard_inputs
        out_meta = tmp_path / "out.tsv.gz"
        prepare_metadata(str(meta_path), str(db_path), str(map_path), str(out_meta))
        with gzip.open(out_meta, "rt") as f:
            header = f.readline().strip().split("\t")
        assert header == META_HEADER + ["species_taxid", "genome_id"]

    def test_drops_undownloaded_accession(
        self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]
    ) -> None:
        _, db_path, _ = standard_inputs
        meta = _write_tsv(
            tmp_path / "m.tsv",
            META_HEADER,
            [
                META_ROWS[0],
                ["GCA_MISSING.1", "67890", "B", "GenBank", "current"],
            ],
        )
        # Map only covers GCA_000001.1; the missing accession is dropped.
        amap = _write_tsv(
            tmp_path / "map.tsv", MAP_HEADER, [["GCA_000001.1", "seq1.1"]]
        )
        out_meta = tmp_path / "out.tsv.gz"
        prepare_metadata(str(meta), str(db_path), str(amap), str(out_meta))
        rows = _read_tsv(out_meta)
        assert len(rows) == 1
        assert rows[0]["assembly_accession"] == "GCA_000001.1"
        assert rows[0]["genome_id"] == "seq1.1"

    @pytest.mark.parametrize(
        ("meta_rows", "map_rows", "expected_accession"),
        [
            # Assembly row appears first; assembly wins.
            (
                [
                    [
                        "GCA_000001.1",
                        "12345",
                        "V",
                        "SOURCE_DATABASE_GENBANK",
                        "current",
                    ],
                    ["NC_045512.2", "12345", "V", "SOURCE_DATABASE_REFSEQ", ""],
                ],
                [["GCA_000001.1", "NC_045512.2"], ["NC_045512.2", "NC_045512.2"]],
                "GCA_000001.1",
            ),
            # Sequence row appears first; assembly still wins (order-independent).
            (
                [
                    ["NC_045512.2", "12345", "V", "SOURCE_DATABASE_REFSEQ", ""],
                    ["GCF_000001.1", "12345", "V", "SOURCE_DATABASE_REFSEQ", "current"],
                ],
                [["NC_045512.2", "NC_045512.2"], ["GCF_000001.1", "NC_045512.2"]],
                "GCF_000001.1",
            ),
        ],
        ids=["assembly_first", "sequence_first"],
    )
    def test_dedups_genome_id_preferring_assembly_row(
        self,
        tmp_path: Path,
        meta_rows: list[list[str]],
        map_rows: list[list[str]],
        expected_accession: str,
    ) -> None:
        """A genome_id reached via both an assembly and a sequence record collapses
        to one row, keeping the assembly-branch (GCA_/GCF_) provenance regardless
        of input order."""
        meta = _write_tsv(tmp_path / "m.tsv", META_HEADER, meta_rows)
        db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, [["12345", "12345", "V"]])
        amap = _write_tsv(tmp_path / "map.tsv", MAP_HEADER, map_rows)
        out_meta = tmp_path / "out.tsv.gz"
        prepare_metadata(str(meta), str(db), str(amap), str(out_meta))
        rows = _read_tsv(out_meta)
        assert len(rows) == 1
        assert rows[0]["genome_id"] == "NC_045512.2"
        assert rows[0]["assembly_accession"] == expected_accession

    def test_empty_metadata_writes_header_only(
        self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]
    ) -> None:
        _, db_path, map_path = standard_inputs
        meta = _write_tsv(tmp_path / "empty.tsv", META_HEADER, [])
        out_meta = tmp_path / "out.tsv.gz"
        prepare_metadata(str(meta), str(db_path), str(map_path), str(out_meta))
        rows = _read_tsv(out_meta)
        assert len(rows) == 0
        with gzip.open(out_meta, "rt") as f:
            header = f.readline().strip().split("\t")
        assert header == META_HEADER + ["species_taxid", "genome_id"]

    def test_unmapped_taxid_gives_empty_species(self, tmp_path: Path) -> None:
        meta = _write_tsv(
            tmp_path / "m.tsv",
            META_HEADER,
            [["GCA_000001.1", "00000", "X", "GenBank", "current"]],
        )
        db = _write_tsv(tmp_path / "db.tsv", DB_HEADER, [["12345", "12345", "V"]])
        amap = _write_tsv(
            tmp_path / "map.tsv", MAP_HEADER, [["GCA_000001.1", "seq1.1"]]
        )
        out_meta = tmp_path / "out.tsv.gz"
        prepare_metadata(str(meta), str(db), str(amap), str(out_meta))
        rows = _read_tsv(out_meta)
        assert rows[0]["species_taxid"] == ""

    def test_gzipped_metadata_input(
        self, tmp_path: Path, standard_inputs: tuple[Path, Path, Path]
    ) -> None:
        _, db_path, map_path = standard_inputs
        meta_gz = tmp_path / "meta.tsv.gz"
        with gzip.open(meta_gz, "wt", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(META_HEADER)
            w.writerows(META_ROWS)
        out_meta = tmp_path / "out.tsv.gz"
        prepare_metadata(str(meta_gz), str(db_path), str(map_path), str(out_meta))
        assert len(_read_tsv(out_meta)) == 4
