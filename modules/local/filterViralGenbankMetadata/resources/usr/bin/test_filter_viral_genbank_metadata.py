"""Tests for filter-viral-genbank-metadata.py.

Invoked via subprocess because the script's filename uses hyphens
(`filter-viral-genbank-metadata.py`), which can't be imported directly.
"""

import csv
import gzip
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent / "filter-viral-genbank-metadata.py"

META_HEADER = [
    "assembly_accession",
    "taxid",
    "organism_name",
    "source_database",
    "assembly_status",
    "species_taxid",
    "local_filename",
]
DB_HEADER = [
    "taxid",
    "infection_status_human",
    "infection_status_vertebrate",
]


def _write_tsv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        w.writerows(rows)
    return path


def _run(tmp_path: Path, meta: Path, db: Path, host_taxa: str) -> dict[str, Path]:
    out_db = tmp_path / "filtered.tsv.gz"
    out_acc = tmp_path / "accessions.csv"
    out_paths = tmp_path / "paths.csv"
    subprocess.run(
        [
            "python3",
            str(SCRIPT),
            str(meta),
            str(db),
            host_taxa,
            str(out_db),
            str(out_acc),
            str(out_paths),
        ],
        check=True,
    )
    return {"db": out_db, "accessions": out_acc, "paths": out_paths}


def _read_gz_tsv(path: Path) -> list[dict[str, str]]:
    with gzip.open(path, "rt") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@pytest.fixture()
def virus_db(tmp_path: Path) -> Path:
    """Two host-vertebrate-positive virus taxids (1, 2) and one negative (3)."""
    return _write_tsv(
        tmp_path / "db.tsv",
        DB_HEADER,
        [
            ["1", "1", "1"],
            ["2", "0", "1"],
            ["3", "0", "0"],
        ],
    )


def _row(acc: str, taxid: str, status: str) -> list[str]:
    """Build a metadata row for a host-vertebrate-positive taxid (1 or 2)."""
    return [acc, taxid, "Org", "GenBank", status, taxid, f"ncbi_genomes/{acc}.fna.gz"]


class TestFilter:
    """End-to-end filter behavior: drop assembly_status='previous' AND drop
    rows whose taxid does not pass the host-taxa screen."""

    @pytest.mark.parametrize(
        ("input_rows", "expected_accessions"),
        [
            # mixed: previous dropped, current kept across two assemblies
            (
                [
                    _row("GCA_001.1", "1", "previous"),
                    _row("GCA_001.2", "1", "current"),
                    _row("GCA_002.1", "2", "current"),
                    _row("GCF_002.1", "2", "current"),
                ],
                {"GCA_001.2", "GCA_002.1", "GCF_002.1"},
            ),
            # all-previous: empty output
            (
                [
                    _row("GCA_001.1", "1", "previous"),
                    _row("GCA_001.2", "1", "previous"),
                ],
                set(),
            ),
            # host-taxa filter: taxid 3 is not vertebrate-infecting per the DB
            # fixture, so it must be dropped even with assembly_status=current
            (
                [
                    _row("GCA_001.1", "1", "current"),
                    _row("GCA_X.1", "3", "current"),
                ],
                {"GCA_001.1"},
            ),
        ],
        ids=["drops-previous-keeps-current", "all-previous", "host-taxa-screen"],
    )
    def test_filter(
        self,
        tmp_path: Path,
        virus_db: Path,
        input_rows: list[list[str]],
        expected_accessions: set[str],
    ) -> None:
        meta = _write_tsv(tmp_path / "meta.tsv", META_HEADER, input_rows)
        rows = _read_gz_tsv(_run(tmp_path, meta, virus_db, "vertebrate")["db"])
        assert {r["assembly_accession"] for r in rows} == expected_accessions
