"""Tests for filter_viral_genbank_metadata.py."""

import io
from pathlib import Path

import pandas as pd
import pytest
from filter_viral_genbank_metadata import filter_metadata, write_accession_chunks

# Two vertebrate-infecting virus taxids (1, 2) and one non-infecting (3).
# `taxid_species` rolls strain-level taxids 11, 21, 31 up to species-level 1, 2, 3.
VIRUS_DB = pd.read_csv(
    io.StringIO(
        "taxid\ttaxid_species\tinfection_status_human\tinfection_status_vertebrate\n"
        "1\t1\t1\t1\n"
        "2\t2\t0\t1\n"
        "3\t3\t0\t0\n"
        "11\t1\t1\t1\n"
        "21\t2\t0\t1\n"
        "31\t3\t0\t0\n",
    ),
    sep="\t",
    dtype=str,
)

META_COLS = [
    "assembly_accession",
    "taxid",
    "organism_name",
    "source_database",
    "assembly_status",
]


def _meta(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    """Build a meta_db DataFrame from (assembly_accession, taxid, assembly_status) tuples."""
    return pd.DataFrame(
        [(acc, taxid, "Org", "GenBank", status) for acc, taxid, status in rows],
        columns=META_COLS,
    )


class TestFilterMetadata:
    @pytest.mark.parametrize(
        "meta_rows,expected",
        [
            (
                [
                    ("GCA_001.1", "1", "previous"),
                    ("GCA_001.2", "1", "current"),
                    ("GCA_002.1", "2", "current"),
                ],
                [("GCA_001.2", "1", "current"), ("GCA_002.1", "2", "current")],
            ),
            (
                [("GCA_001.1", "1", "previous")],
                [],
            ),
            (
                [("GCA_001.1", "1", "current"), ("GCA_X.1", "3", "current")],
                [("GCA_001.1", "1", "current")],
            ),
            (
                [
                    ("GCA_001.1", "1", "replaced"),
                    ("GCA_001.2", "1", "current"),
                    ("GCA_002.1", "2", "suppressed"),
                ],
                [("GCA_001.2", "1", "current")],
            ),
            (
                # Strain-level taxid 11 rolls up to species 1 (host-infecting),
                # 21 rolls up to species 2 (host-infecting). Both should pass.
                [("GCA_011.1", "11", "current"), ("GCA_021.1", "21", "current")],
                [("GCA_011.1", "11", "current"), ("GCA_021.1", "21", "current")],
            ),
            (
                # Strain-level taxid 31 rolls up to species 3 (non-infecting).
                # Rollup must propagate the non-infecting status — strain dropped.
                [("GCA_031.1", "31", "current")],
                [],
            ),
            (
                # Sequence-sourced rows (NCBI Virus / nuccore) carry no assembly
                # status (empty); they pass the status filter alongside current
                # assemblies, while non-current assemblies are still dropped.
                [
                    ("NC_045512.2", "1", ""),
                    ("GCA_001.1", "1", "current"),
                    ("GCA_002.1", "2", "previous"),
                ],
                [("NC_045512.2", "1", ""), ("GCA_001.1", "1", "current")],
            ),
        ],
        ids=[
            "drops_previous",
            "all_previous",
            "host_taxa_screen",
            "drops_other_non_current",
            "rolls_up_to_species",
            "rolls_up_to_non_infecting_species",
            "keeps_sequence_rows_empty_status",
        ],
    )
    def test_filter(
        self,
        meta_rows: list[tuple[str, str, str]],
        expected: list[tuple[str, str, str]],
    ) -> None:
        """Drops superseded assemblies and rows failing host-taxa screen, keeping
        only `current` host-infecting rows. Strain-level taxids match through
        species-level rollup (`taxid_species`)."""
        result = filter_metadata(_meta(meta_rows), VIRUS_DB, ["vertebrate"])
        actual = list(
            zip(
                result["assembly_accession"],
                result["taxid"],
                result["assembly_status"],
                strict=True,
            )
        )
        assert sorted(actual) == sorted(expected)

    def test_empty_taxid_species_matches_only_via_direct_taxid(self) -> None:
        """When taxid_species is empty, species-level rollup yields NaN (via
        pd.read_csv dtype=str default NA handling), which isin() excludes. The
        row still passes if its taxid matches directly."""
        virus_db = pd.read_csv(
            io.StringIO("taxid\ttaxid_species\tinfection_status_vertebrate\n1\t\t1\n"),
            sep="\t",
            dtype=str,
        )
        meta = _meta([("GCA_001.1", "1", "current"), ("GCA_999.1", "999", "current")])
        result = filter_metadata(meta, virus_db, ["vertebrate"])
        assert list(result["assembly_accession"]) == ["GCA_001.1"]

    def test_nan_assembly_status_passes(self) -> None:
        """Sequence rows read from TSV have an empty assembly_status cell, which
        pd.read_csv(dtype=str) parses as NaN; those rows must still pass."""
        meta = pd.read_csv(
            io.StringIO(
                "assembly_accession\ttaxid\torganism_name\tsource_database\tassembly_status\n"
                "NC_045512.2\t1\tOrg\tSOURCE_DATABASE_REFSEQ\t\n"
                "GCA_002.1\t2\tOrg\tSOURCE_DATABASE_GENBANK\tprevious\n"
            ),
            sep="\t",
            dtype=str,
        )
        result = filter_metadata(meta, VIRUS_DB, ["vertebrate"])
        assert list(result["assembly_accession"]) == ["NC_045512.2"]

    def test_missing_assembly_status_column_raises(self) -> None:
        """Pandas raises KeyError when the schema-required `assembly_status` column is absent."""
        meta_no_status = pd.DataFrame(
            [("GCA_001.1", "1", "Org", "GenBank")],
            columns=["assembly_accession", "taxid", "organism_name", "source_database"],
        )
        with pytest.raises(KeyError):
            filter_metadata(meta_no_status, VIRUS_DB, ["vertebrate"])


class TestWriteAccessionChunks:
    @pytest.mark.parametrize(
        ("accessions", "chunk_size", "expected_chunks"),
        [
            # Exactly divides — every chunk full, zero-padded indices.
            (
                [f"GCA_{i:03d}" for i in range(6)],
                2,
                {
                    "chunk_0001.txt": "GCA_000\nGCA_001\n",
                    "chunk_0002.txt": "GCA_002\nGCA_003\n",
                    "chunk_0003.txt": "GCA_004\nGCA_005\n",
                },
            ),
            # Doesn't divide evenly — final chunk holds the remainder.
            (
                [f"GCA_{i:03d}" for i in range(5)],
                2,
                {
                    "chunk_0001.txt": "GCA_000\nGCA_001\n",
                    "chunk_0002.txt": "GCA_002\nGCA_003\n",
                    "chunk_0003.txt": "GCA_004\n",
                },
            ),
            # chunk_size=1 — one file per accession (used by run-test config).
            (
                ["GCA_A", "GCA_B"],
                1,
                {
                    "chunk_0001.txt": "GCA_A\n",
                    "chunk_0002.txt": "GCA_B\n",
                },
            ),
        ],
        ids=["exact_boundary", "partial_final_chunk", "size_one"],
    )
    def test_chunking(
        self,
        tmp_path: Path,
        accessions: list[str],
        chunk_size: int,
        expected_chunks: dict[str, str],
    ) -> None:
        n = write_accession_chunks(
            pd.Series(accessions, dtype=str), tmp_path, chunk_size
        )
        assert n == len(expected_chunks)
        actual = {p.name: p.read_text() for p in tmp_path.iterdir()}
        assert actual == expected_chunks

    def test_empty_input_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="No accessions passed filter"):
            write_accession_chunks(pd.Series([], dtype=str), tmp_path, 10)

    def test_invalid_chunk_size_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="chunk_size must be >= 1"):
            write_accession_chunks(pd.Series(["GCA_A"]), tmp_path, 0)
