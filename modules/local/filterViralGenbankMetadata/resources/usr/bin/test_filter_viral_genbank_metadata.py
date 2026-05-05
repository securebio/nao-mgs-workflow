"""Tests for filter-viral-genbank-metadata.py.

The script is invoked via subprocess because its hyphenated filename
isn't importable. The test focuses on the new `assembly_status='previous'`
drop and the pre-existing host-taxa screen.
"""

import csv
import gzip
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent / "filter-viral-genbank-metadata.py"
META_COLS = "assembly_accession\ttaxid\torganism_name\tsource_database\tassembly_status\tspecies_taxid\tlocal_filename"
# Two vertebrate-infecting virus taxids (1, 2) and one non-infecting (3).
VIRUS_DB = "taxid\tinfection_status_human\tinfection_status_vertebrate\n1\t1\t1\n2\t0\t1\n3\t0\t0\n"


def _row(acc: str, taxid: str, status: str) -> str:
    return f"{acc}\t{taxid}\tOrg\tGenBank\t{status}\t{taxid}\tncbi_genomes/{acc}.fna.gz"


def _run_filter(tmp_path: Path, meta_rows: list[str]) -> set[str]:
    """Run the filter script and return the set of surviving accessions."""
    meta = tmp_path / "meta.tsv"
    meta.write_text(META_COLS + "\n" + "\n".join(meta_rows) + "\n")
    db = tmp_path / "db.tsv"
    db.write_text(VIRUS_DB)
    out_db = tmp_path / "filtered.tsv.gz"
    subprocess.run(
        ["python3", str(SCRIPT), str(meta), str(db), "vertebrate",
         str(out_db), str(tmp_path / "acc.csv"), str(tmp_path / "paths.csv")],
        check=True,
    )
    with gzip.open(out_db, "rt") as f:
        return {r["assembly_accession"] for r in csv.DictReader(f, delimiter="\t")}


@pytest.mark.parametrize(("input_rows", "expected"), [
    # `previous` dropped, `current` kept across both assemblies.
    ([_row("GCA_001.1", "1", "previous"),
      _row("GCA_001.2", "1", "current"),
      _row("GCA_002.1", "2", "current")],
     {"GCA_001.2", "GCA_002.1"}),
    # All `previous` -> empty output.
    ([_row("GCA_001.1", "1", "previous")], set()),
    # Host-taxa screen: taxid 3 is non-vertebrate per VIRUS_DB, dropped
    # even with assembly_status='current'.
    ([_row("GCA_001.1", "1", "current"),
      _row("GCA_X.1", "3", "current")],
     {"GCA_001.1"}),
], ids=["drops-previous", "all-previous", "host-taxa-screen"])
def test_filter(tmp_path: Path, input_rows: list[str], expected: set[str]) -> None:
    assert _run_filter(tmp_path, input_rows) == expected
