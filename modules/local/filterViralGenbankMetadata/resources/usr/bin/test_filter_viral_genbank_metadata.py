"""Tests for filter_viral_genbank_metadata.py."""

import io
from pathlib import Path

import pandas as pd
import pytest
from filter_viral_genbank_metadata import filter_metadata, write_accession_chunks

# Two vertebrate-infecting virus taxids (1, 2) and one non-infecting (3).
# `taxid_species` rolls strain-level taxids 11, 21 up to species-level 1, 2.
VIRUS_DB = pd.read_csv(io.StringIO(
    "taxid\ttaxid_species\tinfection_status_human\tinfection_status_vertebrate\n"
    "1\t1\t1\t1\n"
    "2\t2\t0\t1\n"
    "3\t3\t0\t0\n"
    "11\t1\t1\t1\n"
    "21\t2\t0\t1\n",
), sep="\t", dtype=str)

META_COLS = ["assembly_accession", "taxid", "organism_name",
             "source_database", "assembly_status"]


def _meta(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    """Build a meta_db DataFrame from (assembly_accession, taxid, assembly_status) tuples."""
    return pd.DataFrame(
        [(acc, taxid, "Org", "GenBank", status)
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
            (
                # Strain-level taxid 11 rolls up to species 1 (host-infecting),
                # 21 rolls up to species 2 (host-infecting). Both should pass.
                [("GCA_011.1", "11", "current"),
                 ("GCA_021.1", "21", "current")],
                [("GCA_011.1", "11", "current"),
                 ("GCA_021.1", "21", "current")],
            ),
        ],
        ids=["drops_previous", "all_previous", "host_taxa_screen",
             "drops_other_non_current", "rolls_up_to_species"],
    )
    def test_filter(self, meta_rows: list[tuple[str, str, str]],
                    expected: list[tuple[str, str, str]]) -> None:
        """Drops superseded assemblies and rows failing host-taxa screen, keeping
        only `current` host-infecting rows. Strain-level taxids match through
        species-level rollup (`taxid_species`)."""
        result = filter_metadata(_meta(meta_rows), VIRUS_DB, ["vertebrate"])
        actual = list(zip(result["assembly_accession"], result["taxid"], result["assembly_status"], strict=True))
        assert sorted(actual) == sorted(expected)

    def test_missing_assembly_status_column_raises(self) -> None:
        """Pandas raises KeyError when the schema-required `assembly_status` column is absent."""
        meta_no_status = pd.DataFrame(
            [("GCA_001.1", "1", "Org", "GenBank")],
            columns=["assembly_accession", "taxid", "organism_name", "source_database"],
        )
        with pytest.raises(KeyError):
            filter_metadata(meta_no_status, VIRUS_DB, ["vertebrate"])


class TestWriteAccessionChunks:
    def test_exact_chunk_boundary(self, tmp_path: Path) -> None:
        """Splits exactly at chunk_size with zero-padded indices."""
        accs = pd.Series([f"GCA_{i:03d}" for i in range(6)])
        n = write_accession_chunks(accs, tmp_path, 2)
        assert n == 3
        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "chunk_0001.txt", "chunk_0002.txt", "chunk_0003.txt",
        ]
        assert (tmp_path / "chunk_0001.txt").read_text() == "GCA_000\nGCA_001\n"
        assert (tmp_path / "chunk_0003.txt").read_text() == "GCA_004\nGCA_005\n"

    def test_partial_final_chunk(self, tmp_path: Path) -> None:
        """Final chunk holds the remainder when count doesn't divide evenly."""
        accs = pd.Series([f"GCA_{i:03d}" for i in range(5)])
        n = write_accession_chunks(accs, tmp_path, 2)
        assert n == 3
        assert (tmp_path / "chunk_0003.txt").read_text() == "GCA_004\n"

    def test_chunk_size_one(self, tmp_path: Path) -> None:
        """chunk_size=1 produces one file per accession (used by run-test config)."""
        accs = pd.Series(["GCA_A", "GCA_B"])
        n = write_accession_chunks(accs, tmp_path, 1)
        assert n == 2
        assert (tmp_path / "chunk_0001.txt").read_text() == "GCA_A\n"
        assert (tmp_path / "chunk_0002.txt").read_text() == "GCA_B\n"

    def test_empty_input_writes_empty_chunk(self, tmp_path: Path) -> None:
        """Empty filter result writes one empty chunk so downstream channel is non-empty."""
        n = write_accession_chunks(pd.Series([], dtype=str), tmp_path, 10)
        assert n == 1
        assert (tmp_path / "chunk_0001.txt").read_text() == ""

    def test_invalid_chunk_size_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="chunk_size must be >= 1"):
            write_accession_chunks(pd.Series(["GCA_A"]), tmp_path, 0)
