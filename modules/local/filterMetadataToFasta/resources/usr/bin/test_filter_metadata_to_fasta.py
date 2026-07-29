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

METADATA_HEADER = ["assembly_accession", "taxid", "genome_id"]


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
    """Write a minimal metadata TSV with one row per genome_id."""
    with open_by_suffix(str(path), "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(METADATA_HEADER)
        for i, gid in enumerate(genome_ids):
            writer.writerow([f"GCA_{i:09d}.1", "11111", gid])
    return str(path)


def read_output(path: str) -> list[dict[str, str]]:
    """Read a gzipped TSV back into a list of row dicts."""
    with open_by_suffix(path, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@pytest.mark.parametrize("suffix", [".fasta", ".fasta.gz"])
def test_read_fasta_genome_ids_handles_both_compressions(
    tmp_path: Path, suffix: str
) -> None:
    fasta = write_fasta(
        tmp_path / f"genomes{suffix}", [("AB1.1 some description", "ACGT")]
    )
    assert read_fasta_genome_ids(fasta) == {"AB1.1"}


def test_read_fasta_genome_ids_takes_first_token_only(tmp_path: Path) -> None:
    # The ID is what aligners key on, so the description must not be part of it.
    fasta = write_fasta(
        tmp_path / "genomes.fasta",
        [("AB1.1 Influenza A virus segment 4", "ACGT"), ("AB2.1", "TTTT")],
    )
    assert read_fasta_genome_ids(fasta) == {"AB1.1", "AB2.1"}


def test_read_fasta_genome_ids_rejects_headerless_file(tmp_path: Path) -> None:
    empty = tmp_path / "genomes.fasta"
    empty.write_text("")
    with pytest.raises(ValueError, match="No sequence headers"):
        read_fasta_genome_ids(str(empty))


def test_read_fasta_genome_ids_rejects_header_without_id(tmp_path: Path) -> None:
    # A bare '>' would otherwise raise a bare IndexError, undiagnosable from the
    # Nextflow error report.
    fasta = tmp_path / "genomes.fasta"
    fasta.write_text(">AB1.1\nACGT\n>\nTTTT\n")
    with pytest.raises(ValueError, match="no sequence ID at line 3"):
        read_fasta_genome_ids(str(fasta))


def test_filter_metadata_drops_rows_absent_from_fasta(tmp_path: Path) -> None:
    fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
    metadata = write_metadata(tmp_path / "meta.tsv.gz", ["AB1.1", "AB2.1", "AB3.1"])
    out = str(tmp_path / "out.tsv.gz")
    assert filter_metadata(metadata, fasta, out) == (1, 2)
    rows = read_output(out)
    assert [r["genome_id"] for r in rows] == ["AB1.1"]


def test_filter_metadata_preserves_columns_and_order(tmp_path: Path) -> None:
    fasta = write_fasta(
        tmp_path / "genomes.fasta.gz", [("AB3.1", "ACGT"), ("AB1.1", "TTTT")]
    )
    metadata = write_metadata(tmp_path / "meta.tsv.gz", ["AB1.1", "AB2.1", "AB3.1"])
    out = str(tmp_path / "out.tsv.gz")
    filter_metadata(metadata, fasta, out)
    rows = read_output(out)
    # Metadata row order is preserved; FASTA order does not reorder the table.
    assert [r["genome_id"] for r in rows] == ["AB1.1", "AB3.1"]
    assert list(rows[0].keys()) == METADATA_HEADER


def test_filter_metadata_keeps_every_row_when_nothing_was_removed(
    tmp_path: Path,
) -> None:
    ids = ["AB1.1", "AB2.1"]
    fasta = write_fasta(tmp_path / "genomes.fasta.gz", [(i, "ACGT") for i in ids])
    metadata = write_metadata(tmp_path / "meta.tsv.gz", ids)
    out = str(tmp_path / "out.tsv.gz")
    assert filter_metadata(metadata, fasta, out) == (2, 0)


def test_filter_metadata_keeps_duplicate_rows_for_one_sequence(tmp_path: Path) -> None:
    # One genome_id can carry several metadata rows (e.g. the same sequence
    # reached the DB under more than one assembly); all of them are retained.
    fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
    metadata = write_metadata(tmp_path / "meta.tsv.gz", ["AB1.1", "AB1.1"])
    out = str(tmp_path / "out.tsv.gz")
    assert filter_metadata(metadata, fasta, out) == (2, 0)


def test_filter_metadata_raises_on_sequence_with_no_metadata_row(
    tmp_path: Path,
) -> None:
    # A published sequence RUN cannot resolve to a taxid is a build-time bug.
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
def test_filter_metadata_round_trips_quoted_fields(
    tmp_path: Path, organism_name: str
) -> None:
    # The input is written by PREPARE_VIRAL_METADATA with csv's default dialect,
    # which quotes exactly the fields that need it. Reading and writing with the
    # same default returns them byte-identically; QUOTE_NONE cannot emit them at
    # all. NCBI organism names are outside our control, so this is reachable.
    metadata = tmp_path / "meta.tsv.gz"
    with open_by_suffix(str(metadata), "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["assembly_accession", "organism_name", "genome_id"],
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerow(
            {
                "assembly_accession": "GCA_000000001.1",
                "organism_name": organism_name,
                "genome_id": "AB1.1",
            }
        )
    fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
    out = str(tmp_path / "out.tsv.gz")
    filter_metadata(str(metadata), fasta, out)
    assert read_output(out)[0]["organism_name"] == organism_name


def test_filter_metadata_raises_without_genome_id_column(tmp_path: Path) -> None:
    fasta = write_fasta(tmp_path / "genomes.fasta.gz", [("AB1.1", "ACGT")])
    metadata = tmp_path / "meta.tsv.gz"
    with open_by_suffix(str(metadata), "w", newline="") as f:
        f.write("assembly_accession\ttaxid\nGCA_1.1\t11111\n")
    with pytest.raises(ValueError, match="lacks a genome_id column"):
        filter_metadata(str(metadata), fasta, str(tmp_path / "out.tsv.gz"))
