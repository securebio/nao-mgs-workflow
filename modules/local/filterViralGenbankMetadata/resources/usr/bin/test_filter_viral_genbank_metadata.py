"""Tests for filter_viral_genbank_metadata.py."""

import io

import pandas as pd
import pytest
from filter_viral_genbank_metadata import filter_metadata

# Two vertebrate-infecting virus taxids (1, 2) and one non-infecting (3).
VIRUS_DB = pd.read_csv(io.StringIO(
    "taxid\tinfection_status_human\tinfection_status_vertebrate\n"
    "1\t1\t1\n"
    "2\t0\t1\n"
    "3\t0\t0\n",
), sep="\t", dtype=str)

META_COLS = ["assembly_accession", "taxid", "organism_name",
             "source_database", "assembly_status", "species_taxid", "local_filename"]


def _meta(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    """Build a meta_db DataFrame from (assembly_accession, taxid, assembly_status) tuples."""
    return pd.DataFrame(
        [(acc, taxid, "Org", "GenBank", status, taxid, f"ncbi_genomes/{acc}.fna.gz")
         for acc, taxid, status in rows],
        columns=META_COLS,
    )


class TestFilterMetadata:
    @pytest.mark.parametrize(
        "meta_rows,expected",
        [
            (
                [("GCA_001.1", "1", "previous"),
                 ("GCA_001.2", "1", "current"),
                 ("GCA_002.1", "2", "current")],
                [("GCA_001.2", "1", "current"),
                 ("GCA_002.1", "2", "current")],
            ),
            (
                [("GCA_001.1", "1", "previous")],
                [],
            ),
            (
                [("GCA_001.1", "1", "current"),
                 ("GCA_X.1", "3", "current")],
                [("GCA_001.1", "1", "current")],
            ),
            (
                [("GCA_001.1", "1", "replaced"),
                 ("GCA_001.2", "1", "current"),
                 ("GCA_002.1", "2", "suppressed")],
                [("GCA_001.2", "1", "current")],
            ),
        ],
        ids=["drops_previous", "all_previous", "host_taxa_screen", "drops_other_non_current"],
    )
    def test_filter(self, meta_rows: list[tuple[str, str, str]],
                    expected: list[tuple[str, str, str]]) -> None:
        """Drops superseded assemblies and rows failing host-taxa screen, keeping
        only `current` host-infecting rows with the original accession/taxid."""
        result = filter_metadata(_meta(meta_rows), VIRUS_DB, ["vertebrate"])
        actual = list(zip(result["assembly_accession"], result["taxid"], result["assembly_status"], strict=True))
        assert sorted(actual) == sorted(expected)

    def test_missing_assembly_status_column_raises(self) -> None:
        """Pandas raises KeyError when the schema-required `assembly_status` column is absent."""
        meta_no_status = pd.DataFrame(
            [("GCA_001.1", "1", "Org", "GenBank", "1", "ncbi_genomes/GCA_001.1.fna.gz")],
            columns=["assembly_accession", "taxid", "organism_name",
                     "source_database", "species_taxid", "local_filename"],
        )
        with pytest.raises(KeyError):
            filter_metadata(meta_no_status, VIRUS_DB, ["vertebrate"])
