"""Tests for filter_metadata_to_fasta.py."""

import csv
import gzip
from pathlib import Path

import pytest
from filter_metadata_to_fasta import (
    filter_metadata,
    open_by_suffix,
    read_fasta_genome_ids,
)

METADATA_HEADER = ["assembly_accession", "taxid", "species_taxid", "genome_id"]

# One row carrying every column PREPARE_VIRAL_METADATA emits, for the cases that
# turn on which columns are per-assembly and which are per-sequence.
FULL_ROW = {
    "assembly_accession": "GCA_000000000.1",
    "taxid": "11111",
    "organism_name": "Test virus A",
    "source_database": "SOURCE_DATABASE_GENBANK",
    "assembly_status": "current",
    "release_date": "2020-01-01",
    "species_taxid": "11111",
    "genome_id": "AB1.1",
}


def write_fasta(path: Path, records: list[tuple[str, str]]) -> str:
    """Write a FASTA file, gzipped if the path ends in .gz."""
    text = "".join(f">{header}\n{seq}\n" for header, seq in records)
    if str(path).endswith(".gz"):
        with gzip.open(path, "wt") as f:
            f.write(text)
    else:
        path.write_text(text)
    return str(path)


def write_metadata(path: Path, genome_ids: list[str]) -> str:
    """Write a minimal metadata TSV with one row per genome_id.
    Accessions ascend with row order, so the first row for a genome_id is also
    the one the dedup rule keeps.
    """
    return write_rows(
        path,
        [[f"GCA_{i:09d}.1", "11111", "11111", gid] for i, gid in enumerate(genome_ids)],
    )


def write_rows(
    path: Path, rows: list[list[str]], header: list[str] | None = None
) -> str:
    """Write a metadata TSV from explicit rows."""
    with open_by_suffix(str(path), "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(header if header is not None else METADATA_HEADER)
        writer.writerows(rows)
    return str(path)


def read_output(path: str) -> list[dict[str, str]]:
    """Read a gzipped TSV back into a list of row dicts."""
    with open_by_suffix(path, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


class TestReadFastaGenomeIds:
    @pytest.mark.parametrize("suffix", [".fasta", ".fasta.gz"])
    def test_handles_both_compressions(self, tmp_path: Path, suffix: str) -> None:
        fasta = write_fasta(
            tmp_path / f"genomes{suffix}", [("AB1.1 some description", "ACGT")]
        )
        assert read_fasta_genome_ids(fasta) == {"AB1.1"}

    def test_takes_first_token_only(self, tmp_path: Path) -> None:
        # The ID is what aligners key on, so the description must not be part of it.
        fasta = write_fasta(
            tmp_path / "genomes.fasta",
            [("AB1.1 Influenza A virus segment 4", "ACGT"), ("AB2.1", "TTTT")],
        )
        assert read_fasta_genome_ids(fasta) == {"AB1.1", "AB2.1"}

    @pytest.mark.parametrize(
        "text,match",
        [
            ("", "No sequence headers"),
            (">AB1.1\nACGT\n>\nTTTT\n", "no sequence ID at line 3"),
        ],
        ids=["headerless", "header_without_id"],
    )
    def test_rejects_malformed_fasta(
        self, tmp_path: Path, text: str, match: str
    ) -> None:
        fasta = tmp_path / "genomes.fasta"
        fasta.write_text(text)
        with pytest.raises(ValueError, match=match):
            read_fasta_genome_ids(str(fasta))


class TestFilterMetadata:
    @pytest.mark.parametrize(
        "fasta_ids,metadata_ids,expected_counts,expected_kept",
        [
            (["AB1.1"], ["AB1.1", "AB2.1", "AB3.1"], (1, 2, 0), ["AB1.1"]),
            (["AB1.1", "AB2.1"], ["AB1.1", "AB2.1"], (2, 0, 0), ["AB1.1", "AB2.1"]),
            (["AB1.1"], ["AB1.1", "AB1.1"], (1, 0, 1), ["AB1.1"]),
        ],
        ids=["drops_absent", "keeps_all", "dedups_duplicate_rows"],
    )
    def test_row_filtering(
        self,
        tmp_path: Path,
        fasta_ids: list[str],
        metadata_ids: list[str],
        expected_counts: tuple[int, int, int],
        expected_kept: list[str],
    ) -> None:
        fasta = write_fasta(
            tmp_path / "genomes.fasta.gz", [(i, "ACGT") for i in fasta_ids]
        )
        metadata = write_metadata(tmp_path / "meta.tsv.gz", metadata_ids)
        out = str(tmp_path / "out.tsv.gz")
        assert filter_metadata(metadata, fasta, out) == expected_counts
        assert [r["genome_id"] for r in read_output(out)] == expected_kept

    def test_dedup_keeps_the_smallest_assembly_accession(self, tmp_path: Path) -> None:
        # The FASTA reaches `seqkit rmdup` in accession order, so the record it
        # keeps is the one from the smallest accession — not whichever row the
        # metadata, which is not accession-sorted, happens to list first.
        fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
        metadata = write_rows(
            tmp_path / "meta.tsv.gz",
            [
                ["GCA_000000009.1", "11111", "11111", "AB1.1"],
                ["GCA_000000002.1", "11111", "11111", "AB1.1"],
            ],
        )
        out = str(tmp_path / "out.tsv.gz")
        filter_metadata(metadata, fasta, out)
        assert [r["assembly_accession"] for r in read_output(out)] == [
            "GCA_000000002.1"
        ]

    def test_superseding_a_row_does_not_reorder_the_table(self, tmp_path: Path) -> None:
        # AB1.1's row is replaced by one that appears after AB2.1's, but AB1.1
        # keeps the position of its first appearance.
        fasta = write_fasta(
            tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT"), ("AB2.1", "TTTT")]
        )
        metadata = write_rows(
            tmp_path / "meta.tsv.gz",
            [
                ["GCA_000000009.1", "11111", "11111", "AB1.1"],
                ["GCA_000000005.1", "22222", "22222", "AB2.1"],
                ["GCA_000000002.1", "11111", "11111", "AB1.1"],
            ],
        )
        out = str(tmp_path / "out.tsv.gz")
        filter_metadata(metadata, fasta, out)
        assert [
            (r["genome_id"], r["assembly_accession"]) for r in read_output(out)
        ] == [
            ("AB1.1", "GCA_000000002.1"),
            ("AB2.1", "GCA_000000005.1"),
        ]

    @pytest.mark.parametrize("field", ["taxid", "species_taxid", "organism_name"])
    def test_raises_on_duplicate_rows_disagreeing_outside_assembly_fields(
        self, tmp_path: Path, field: str
    ) -> None:
        fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
        conflicting = dict(FULL_ROW, assembly_accession="GCA_000000001.1")
        conflicting[field] = "something else"
        metadata = write_rows(
            tmp_path / "meta.tsv.gz",
            [list(FULL_ROW.values()), list(conflicting.values())],
            header=list(FULL_ROW),
        )
        with pytest.raises(ValueError, match=f"AB1.1 disagree on {field}"):
            filter_metadata(metadata, fasta, str(tmp_path / "out.tsv.gz"))

    def test_allows_duplicate_rows_to_differ_on_assembly_fields(
        self, tmp_path: Path
    ) -> None:
        # A sequence packaged in two assemblies carries two rows that differ on
        # every per-assembly column. Only the per-sequence columns must agree,
        # so this must reconcile rather than fail the index build.
        fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
        other_assembly = dict(
            FULL_ROW,
            assembly_accession="GCA_000000009.1",
            source_database="SOURCE_DATABASE_REFSEQ",
            assembly_status="suppressed",
            release_date="2024-06-30",
        )
        metadata = write_rows(
            tmp_path / "meta.tsv.gz",
            [list(other_assembly.values()), list(FULL_ROW.values())],
            header=list(FULL_ROW),
        )
        out = str(tmp_path / "out.tsv.gz")
        assert filter_metadata(metadata, fasta, out) == (1, 0, 1)
        assert read_output(out) == [FULL_ROW]

    def test_preserves_column_content_and_row_order(self, tmp_path: Path) -> None:
        fasta = write_fasta(
            tmp_path / "genomes.fasta.gz", [("AB3.1", "ACGT"), ("AB1.1", "TTTT")]
        )
        metadata = write_metadata(tmp_path / "meta.tsv.gz", ["AB1.1", "AB2.1", "AB3.1"])
        out = str(tmp_path / "out.tsv.gz")
        filter_metadata(metadata, fasta, out)
        rows = read_output(out)
        # Every column of a kept row survives verbatim, and metadata row order
        # is preserved: FASTA order does not reorder the table.
        assert rows == [
            {
                "assembly_accession": "GCA_000000000.1",
                "taxid": "11111",
                "species_taxid": "11111",
                "genome_id": "AB1.1",
            },
            {
                "assembly_accession": "GCA_000000002.1",
                "taxid": "11111",
                "species_taxid": "11111",
                "genome_id": "AB3.1",
            },
        ]
        assert list(rows[0].keys()) == METADATA_HEADER

    def test_empty_metadata_raises(self, tmp_path: Path) -> None:
        # Header-only metadata leaves every published sequence unresolvable.
        fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
        metadata = write_metadata(tmp_path / "meta.tsv.gz", [])
        with pytest.raises(ValueError, match="AB1.1"):
            filter_metadata(metadata, fasta, str(tmp_path / "out.tsv.gz"))

    def test_raises_on_sequence_with_no_metadata_row(self, tmp_path: Path) -> None:
        fasta = write_fasta(
            tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT"), ("AB9.1", "TTTT")]
        )
        metadata = write_metadata(tmp_path / "meta.tsv.gz", ["AB1.1"])
        out = str(tmp_path / "out.tsv.gz")
        with pytest.raises(ValueError, match="AB9.1"):
            filter_metadata(metadata, fasta, out)

    @pytest.mark.parametrize(
        "organism_name",
        ['Escherichia phage "vB_x"', "Influenza A virus\twith a tab"],
        ids=["quote", "tab"],
    )
    def test_round_trips_quoted_fields(
        self, tmp_path: Path, organism_name: str
    ) -> None:
        metadata = write_rows(
            tmp_path / "meta.tsv.gz",
            [["GCA_000000001.1", organism_name, "11111", "11111", "AB1.1"]],
            header=[*METADATA_HEADER[:1], "organism_name", *METADATA_HEADER[1:]],
        )
        fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
        out = str(tmp_path / "out.tsv.gz")
        filter_metadata(metadata, fasta, out)
        assert read_output(out)[0]["organism_name"] == organism_name
