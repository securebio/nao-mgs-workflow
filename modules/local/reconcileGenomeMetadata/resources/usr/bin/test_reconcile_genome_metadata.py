"""Tests for reconcile_genome_metadata.py."""

import csv
import gzip
from pathlib import Path

import pytest
from reconcile_genome_metadata import (
    JOIN_PLACEHOLDER,
    check_metadata_present,
    check_rows_agree,
    read_metadata_schema,
    reconcile_metadata,
    reduce_rows,
    supersedes,
)

COLUMNS = [
    "assembly_accession",
    "taxid",
    "organism_name",
    "source_database",
    "assembly_status",
    "release_date",
    "species_taxid",
    "genome_id",
    "seq_length",
    "seq_hash",
]

BASE = {
    "assembly_accession": "GCA_000000001.1",
    "taxid": "11111",
    "organism_name": "Test virus A",
    "source_database": "SOURCE_DATABASE_GENBANK",
    "assembly_status": "current",
    "release_date": "2020-01-01",
    "species_taxid": "11111",
    "genome_id": "AB1.1",
    "seq_length": "100",
    "seq_hash": "deadbeef",
}


def row(**overrides: str) -> list[str]:
    """Build a joined row, overriding named columns."""
    values = {**BASE, **overrides}
    return [values[c] for c in COLUMNS]


SCHEMA = read_metadata_schema(COLUMNS)


def write_joined(path: Path, rows: list[list[str]]) -> str:
    """Write a joined TSV with a header, gzipped if the path ends in .gz."""
    text = "\t".join(COLUMNS) + "\n" + "".join("\t".join(r) + "\n" for r in rows)
    if str(path).endswith(".gz"):
        with gzip.open(path, "wt") as f:
            f.write(text)
    else:
        path.write_text(text)
    return str(path)


def read_output(path: str) -> list[dict[str, str]]:
    """Read a written metadata TSV into dicts."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", newline="") as f:  # type: ignore[operator]
        return list(csv.DictReader(f, delimiter="\t"))


########################
# read_metadata_schema #
########################


def test_read_metadata_schema_locates_columns() -> None:
    assert SCHEMA.genome_id_idx == COLUMNS.index("genome_id")
    assert SCHEMA.assembly_accession_idx == COLUMNS.index("assembly_accession")
    # Assembly-level columns are excluded from the agreement check.
    compared = {COLUMNS[i] for i in SCHEMA.idxs_to_compare}
    assert "taxid" in compared and "seq_hash" in compared
    assert "assembly_accession" not in compared and "release_date" not in compared


@pytest.mark.parametrize("missing", ["genome_id", "assembly_accession"])
def test_read_metadata_schema_requires_key_columns(missing: str) -> None:
    columns = [c for c in COLUMNS if c != missing]
    with pytest.raises(ValueError, match=f"missing required column '{missing}'"):
        read_metadata_schema(columns)


##########################
# check_metadata_present #
##########################


def test_check_metadata_present_accepts_a_real_row() -> None:
    check_metadata_present(row(), SCHEMA)


def test_check_metadata_present_rejects_join_placeholder() -> None:
    with pytest.raises(ValueError, match="no metadata row"):
        check_metadata_present(row(assembly_accession=JOIN_PLACEHOLDER), SCHEMA)


####################
# check_rows_agree #
####################


@pytest.mark.parametrize(
    "field",
    ["assembly_accession", "source_database", "assembly_status", "release_date"],
)
def test_check_rows_agree_allows_assembly_level_differences(field: str) -> None:
    check_rows_agree("AB1.1", row(), row(**{field: "different"}), SCHEMA)


@pytest.mark.parametrize(
    "field", ["taxid", "organism_name", "species_taxid", "seq_hash"]
)
def test_check_rows_agree_rejects_sequence_level_differences(field: str) -> None:
    with pytest.raises(ValueError, match=f"disagree on {field}"):
        check_rows_agree("AB1.1", row(), row(**{field: "different"}), SCHEMA)


##############
# supersedes #
##############


@pytest.mark.parametrize(
    ("kept", "candidate", "expected"),
    [
        ("GCA_000000002.1", "GCA_000000001.1", True),
        ("GCA_000000001.1", "GCA_000000002.1", False),
        ("GCA_000000001.1", "GCA_000000001.1", False),
    ],
)
def test_supersedes_prefers_the_lowest_accession(
    kept: str, candidate: str, expected: bool
) -> None:
    assert (
        supersedes(
            row(assembly_accession=kept), row(assembly_accession=candidate), SCHEMA
        )
        is expected
    )


###############
# reduce_rows #
###############


def test_reduce_rows_collapses_one_sequence_in_two_assemblies() -> None:
    rows = [
        row(assembly_accession="GCA_000000004.1"),
        row(assembly_accession="GCA_000000001.1"),
    ]
    reduced = list(reduce_rows(iter(rows), SCHEMA))
    assert len(reduced) == 1
    kept, n_rows = reduced[0]
    assert kept[SCHEMA.assembly_accession_idx] == "GCA_000000001.1"
    assert n_rows == 2


def test_reduce_rows_keeps_distinct_sequences_separate() -> None:
    rows = [row(genome_id="AB1.1"), row(genome_id="AB2.1")]
    reduced = list(reduce_rows(iter(rows), SCHEMA))
    assert [r[SCHEMA.genome_id_idx] for r, _ in reduced] == ["AB1.1", "AB2.1"]
    assert [n for _, n in reduced] == [1, 1]


def test_reduce_rows_rejects_unsorted_input() -> None:
    rows = [row(genome_id="AB2.1"), row(genome_id="AB1.1")]
    with pytest.raises(ValueError, match="not sorted by genome_id"):
        list(reduce_rows(iter(rows), SCHEMA))


def test_reduce_rows_is_empty_for_empty_input() -> None:
    assert list(reduce_rows(iter([]), SCHEMA)) == []


######################
# reconcile_metadata #
######################


def test_reconcile_metadata_writes_one_row_per_sequence(tmp_path: Path) -> None:
    joined = write_joined(
        tmp_path / "joined.tsv",
        [
            row(genome_id="AB1.1", assembly_accession="GCA_000000004.1"),
            row(genome_id="AB1.1", assembly_accession="GCA_000000001.1"),
            row(
                genome_id="AB2.1", assembly_accession="GCA_000000002.1", seq_hash="cafe"
            ),
        ],
    )
    out = str(tmp_path / "out.tsv.gz")
    assert reconcile_metadata(joined, out) == (2, 1)
    written = read_output(out)
    assert [r["genome_id"] for r in written] == ["AB1.1", "AB2.1"]
    # The stated policy picked the lower accession, not the first row seen.
    assert written[0]["assembly_accession"] == "GCA_000000001.1"
    # Sequence-derived columns from the join survive into the published table.
    assert written[0]["seq_hash"] == "deadbeef"
    assert written[0]["seq_length"] == "100"
    assert list(written[0]) == COLUMNS


def test_reconcile_metadata_fails_on_sequence_without_metadata(tmp_path: Path) -> None:
    joined = write_joined(
        tmp_path / "joined.tsv",
        [row(genome_id="AB9.1", assembly_accession=JOIN_PLACEHOLDER)],
    )
    with pytest.raises(ValueError, match="no metadata row"):
        reconcile_metadata(joined, str(tmp_path / "out.tsv.gz"))


def test_reconcile_metadata_fails_on_disagreeing_rows(tmp_path: Path) -> None:
    joined = write_joined(
        tmp_path / "joined.tsv",
        [
            row(assembly_accession="GCA_000000001.1", taxid="11111"),
            row(assembly_accession="GCA_000000002.1", taxid="22222"),
        ],
    )
    with pytest.raises(ValueError, match="disagree on taxid"):
        reconcile_metadata(joined, str(tmp_path / "out.tsv.gz"))


def test_reconcile_metadata_fails_on_header_only_input(tmp_path: Path) -> None:
    joined = write_joined(tmp_path / "joined.tsv", [])
    with pytest.raises(ValueError, match="described no sequences"):
        reconcile_metadata(joined, str(tmp_path / "out.tsv.gz"))


def test_reconcile_metadata_handles_gzipped_input(tmp_path: Path) -> None:
    joined = write_joined(tmp_path / "joined.tsv.gz", [row()])
    out = str(tmp_path / "out.tsv.gz")
    assert reconcile_metadata(joined, out) == (1, 0)
    assert read_output(out)[0]["genome_id"] == "AB1.1"
