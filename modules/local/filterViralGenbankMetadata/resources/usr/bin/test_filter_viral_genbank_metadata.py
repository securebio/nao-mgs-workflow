#!/usr/bin/env python

"""Tests for filter-viral-genbank-metadata.py.

The script is invoked via subprocess because its hyphenated filename
isn't importable.
"""

import gzip
import subprocess
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).parent / "filter-viral-genbank-metadata.py"
META_HEADER = "assembly_accession\ttaxid\torganism_name\tsource_database\tassembly_status\tspecies_taxid\tlocal_filename\n"
# Two vertebrate-infecting virus taxids (1, 2) and one non-infecting (3).
VIRUS_DB = (
    "taxid\tinfection_status_human\tinfection_status_vertebrate\n"
    "1\t1\t1\n"
    "2\t0\t1\n"
    "3\t0\t0\n"
)


def _meta_row(acc: str, taxid: str, status: str) -> str:
    return f"{acc}\t{taxid}\tOrg\tGenBank\t{status}\t{taxid}\tncbi_genomes/{acc}.fna.gz\n"


class TestFilterViralGenbankMetadata:
    """Test the filter-viral-genbank-metadata.py script end-to-end."""

    @pytest.mark.parametrize(
        "meta_rows,expected_accessions",
        [
            (
                [
                    _meta_row("GCA_001.1", "1", "previous"),
                    _meta_row("GCA_001.2", "1", "current"),
                    _meta_row("GCA_002.1", "2", "current"),
                ],
                {"GCA_001.2", "GCA_002.1"},
            ),
            (
                [_meta_row("GCA_001.1", "1", "previous")],
                set(),
            ),
            (
                [
                    _meta_row("GCA_001.1", "1", "current"),
                    _meta_row("GCA_X.1", "3", "current"),
                ],
                {"GCA_001.1"},
            ),
        ],
        ids=["drops_previous", "all_previous", "host_taxa_screen"],
    )
    def test_filter(self, tsv_factory: Any, meta_rows: list[str], expected_accessions: set[str]) -> None:
        """Test that filter drops superseded assemblies and rows failing host-taxa screen."""
        meta_file = tsv_factory.create_plain("meta.tsv", META_HEADER + "".join(meta_rows))
        db_file = tsv_factory.create_plain("db.tsv", VIRUS_DB)
        out_db = tsv_factory.get_path("filtered.tsv.gz")
        subprocess.run(
            ["python3", str(SCRIPT), meta_file, db_file, "vertebrate",
             out_db, tsv_factory.get_path("acc.csv"), tsv_factory.get_path("paths.csv")],
            check=True,
        )
        with gzip.open(out_db, "rt") as f:
            lines = f.read().splitlines()
        accessions = {line.split("\t")[0] for line in lines[1:]}
        assert accessions == expected_accessions
