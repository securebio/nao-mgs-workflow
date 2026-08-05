"""Tests for annotate_validation_status.py."""

import gzip
from pathlib import Path
from typing import Any

import pytest
from annotate_validation_status import (
    MISSING,
    STATUS_ALIGNED,
    STATUS_NO_ALIGNMENT,
    STATUS_NOT_SAMPLED,
    annotate_validation_status,
    open_by_suffix,
    read_header,
    read_key_set,
    read_validation_table,
)

#############
# CONSTANTS #
#############

HITS_HEADER = "seq_id\tsample\taligner_taxid_lca"
VAL_HEADER = "seq_id\tvalidation_staxid_lca\tvalidation_distance_aligner"

####################
# HELPER FUNCTIONS #
####################


def write_tsv(path: Path, header: str, rows: list[str]) -> Path:
    """Write a TSV, gzipping if the path ends in .gz, and return the path."""
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "wt") as fh:
        fh.write("\n".join([header, *rows]) + "\n" if rows else header + "\n")
    return path


def read_output(path: Path) -> tuple[list[str], list[list[str]]]:
    """Read an output TSV into (header fields, list of row field lists)."""
    with open_by_suffix(str(path)) as fh:
        lines = [line.rstrip("\n") for line in fh if line.strip()]
    return lines[0].split("\t"), [line.split("\t") for line in lines[1:]]


def status_of(rows: list[list[str]]) -> dict[str, str]:
    """Map seq_id to the final (status) column of each row."""
    return {row[0]: row[-1] for row in rows}


def build_case(tmp_path: Path, suffix: str = "tsv") -> tuple[Path, Path, Path]:
    """Build a standard three-read case: one aligned, one sampled-no-hit, one unsampled."""
    hits = write_tsv(
        tmp_path / f"hits.{suffix}",
        HITS_HEADER,
        ["read_a\ts1\t10239", "read_b\ts1\t10239", "read_c\ts1\t10239"],
    )
    validation = write_tsv(tmp_path / f"val.{suffix}", VAL_HEADER, ["read_a\t11676\t2"])
    sampled = write_tsv(tmp_path / f"sampled.{suffix}", "seq_id", ["read_a", "read_b"])
    return hits, validation, sampled


###############
# read_header #
###############


def test_read_header_splits_columns(tmp_path: Path) -> None:
    """The header line is split on tabs."""
    path = write_tsv(tmp_path / "t.tsv", HITS_HEADER, [])
    with open_by_suffix(str(path)) as fh:
        assert read_header(fh, str(path)) == ["seq_id", "sample", "aligner_taxid_lca"]


def test_read_header_rejects_empty_file(tmp_path: Path) -> None:
    """An empty file is a hard error rather than an empty header."""
    path = tmp_path / "empty.tsv"
    path.write_text("")
    with open_by_suffix(str(path)) as fh, pytest.raises(ValueError, match="empty"):
        read_header(fh, str(path))


################
# read_key_set #
################


def test_read_key_set_collects_column_values(tmp_path: Path) -> None:
    """Values of the named column are collected into a set."""
    path = write_tsv(tmp_path / "s.tsv", "seq_id", ["read_a", "read_b", "read_a"])
    assert read_key_set(str(path), "seq_id") == {"read_a", "read_b"}


def test_read_key_set_empty_body(tmp_path: Path) -> None:
    """A header-only file yields an empty set, not an error."""
    path = write_tsv(tmp_path / "s.tsv", "seq_id", [])
    assert read_key_set(str(path), "seq_id") == set()


def test_read_key_set_rejects_missing_column(tmp_path: Path) -> None:
    """A missing column is a hard error naming the available columns."""
    path = write_tsv(tmp_path / "s.tsv", "seq_id", ["read_a"])
    with pytest.raises(ValueError, match="not found in header"):
        read_key_set(str(path), "nope")


#########################
# read_validation_table #
#########################


def test_read_validation_table_excludes_key_column(tmp_path: Path) -> None:
    """The key column is dropped from both the value columns and the values."""
    path = write_tsv(tmp_path / "v.tsv", VAL_HEADER, ["read_a\t11676\t2"])
    columns, table = read_validation_table(str(path), "seq_id")
    assert columns == ["validation_staxid_lca", "validation_distance_aligner"]
    assert table == {"read_a": ["11676", "2"]}


def test_read_validation_table_rejects_duplicate_key(tmp_path: Path) -> None:
    """Duplicate keys are a hard error, since the join would be ambiguous."""
    path = write_tsv(
        tmp_path / "v.tsv", VAL_HEADER, ["read_a\t11676\t2", "read_a\t11677\t3"]
    )
    with pytest.raises(ValueError, match="Duplicate key"):
        read_validation_table(str(path), "seq_id")


def test_read_validation_table_rejects_ragged_row(tmp_path: Path) -> None:
    """A row with the wrong field count is a hard error."""
    path = write_tsv(tmp_path / "v.tsv", VAL_HEADER, ["read_a\t11676"])
    with pytest.raises(ValueError, match="fields, expected"):
        read_validation_table(str(path), "seq_id")


##############################
# annotate_validation_status #
##############################


@pytest.mark.parametrize("suffix", ["tsv", "tsv.gz"])
def test_annotate_assigns_three_statuses(tmp_path: Path, suffix: Any) -> None:
    """Each read gets the status matching its own evidence, compressed or not."""
    hits, validation, sampled = build_case(tmp_path, suffix)
    out = tmp_path / f"out.{suffix}"
    counts = annotate_validation_status(
        str(hits),
        str(validation),
        str(sampled),
        str(out),
        "seq_id",
        "validation_status",
    )
    _header, rows = read_output(out)
    assert status_of(rows) == {
        "read_a": STATUS_ALIGNED,
        "read_b": STATUS_NO_ALIGNMENT,
        "read_c": STATUS_NOT_SAMPLED,
    }
    assert counts[STATUS_ALIGNED] == 1
    assert counts[STATUS_NO_ALIGNMENT] == 1
    assert counts[STATUS_NOT_SAMPLED] == 1


def test_annotate_appends_columns_in_order(tmp_path: Path) -> None:
    """Output columns are hits columns, then validation columns, then status."""
    hits, validation, sampled = build_case(tmp_path)
    out = tmp_path / "out.tsv"
    annotate_validation_status(
        str(hits),
        str(validation),
        str(sampled),
        str(out),
        "seq_id",
        "validation_status",
    )
    header, _rows = read_output(out)
    assert header == [
        "seq_id",
        "sample",
        "aligner_taxid_lca",
        "validation_staxid_lca",
        "validation_distance_aligner",
        "validation_status",
    ]


def test_annotate_populates_only_aligned_reads(tmp_path: Path) -> None:
    """Validation values appear only on the read they were measured from."""
    hits, validation, sampled = build_case(tmp_path)
    out = tmp_path / "out.tsv"
    annotate_validation_status(
        str(hits),
        str(validation),
        str(sampled),
        str(out),
        "seq_id",
        "validation_status",
    )
    _header, rows = read_output(out)
    by_id = {row[0]: row for row in rows}
    assert by_id["read_a"][3:5] == ["11676", "2"]
    assert by_id["read_b"][3:5] == [MISSING, MISSING]
    assert by_id["read_c"][3:5] == [MISSING, MISSING]


def test_annotate_preserves_row_count_and_order(tmp_path: Path) -> None:
    """Every hit appears exactly once, in input order."""
    hits = write_tsv(
        tmp_path / "hits.tsv",
        HITS_HEADER,
        [f"read_{i}\ts1\t10239" for i in range(50)],
    )
    validation = write_tsv(tmp_path / "v.tsv", VAL_HEADER, ["read_7\t11676\t0"])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", ["read_7"])
    out = tmp_path / "out.tsv"
    annotate_validation_status(
        str(hits),
        str(validation),
        str(sampled),
        str(out),
        "seq_id",
        "validation_status",
    )
    _header, rows = read_output(out)
    assert [row[0] for row in rows] == [f"read_{i}" for i in range(50)]


def test_annotate_empty_validation_table(tmp_path: Path) -> None:
    """With no alignments at all, sampled reads are no_alignment and others unsampled."""
    hits = write_tsv(
        tmp_path / "hits.tsv", HITS_HEADER, ["read_a\ts1\t10239", "read_b\ts1\t10239"]
    )
    validation = write_tsv(tmp_path / "v.tsv", VAL_HEADER, [])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", ["read_a"])
    out = tmp_path / "out.tsv"
    annotate_validation_status(
        str(hits),
        str(validation),
        str(sampled),
        str(out),
        "seq_id",
        "validation_status",
    )
    _header, rows = read_output(out)
    assert status_of(rows) == {
        "read_a": STATUS_NO_ALIGNMENT,
        "read_b": STATUS_NOT_SAMPLED,
    }


def test_annotate_header_only_hits(tmp_path: Path) -> None:
    """A hits file with no rows yields a header-only output."""
    hits = write_tsv(tmp_path / "hits.tsv", HITS_HEADER, [])
    validation = write_tsv(tmp_path / "v.tsv", VAL_HEADER, [])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", [])
    out = tmp_path / "out.tsv"
    counts = annotate_validation_status(
        str(hits),
        str(validation),
        str(sampled),
        str(out),
        "seq_id",
        "validation_status",
    )
    _header, rows = read_output(out)
    assert rows == []
    assert sum(counts.values()) == 0


def test_annotate_rejects_validated_read_absent_from_sample(tmp_path: Path) -> None:
    """An alignment for an unsampled read means the inputs are inconsistent."""
    hits = write_tsv(tmp_path / "hits.tsv", HITS_HEADER, ["read_a\ts1\t10239"])
    validation = write_tsv(tmp_path / "v.tsv", VAL_HEADER, ["read_a\t11676\t2"])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", [])
    with pytest.raises(ValueError, match="absent from the"):
        annotate_validation_status(
            str(hits),
            str(validation),
            str(sampled),
            str(tmp_path / "out.tsv"),
            "seq_id",
            "validation_status",
        )


def test_annotate_rejects_colliding_columns(tmp_path: Path) -> None:
    """Validation columns that already exist in the hits table are a hard error."""
    hits = write_tsv(tmp_path / "hits.tsv", HITS_HEADER, ["read_a\ts1\t10239"])
    validation = write_tsv(tmp_path / "v.tsv", "seq_id\tsample", ["read_a\tclobbered"])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", ["read_a"])
    with pytest.raises(ValueError, match="collide"):
        annotate_validation_status(
            str(hits),
            str(validation),
            str(sampled),
            str(tmp_path / "out.tsv"),
            "seq_id",
            "validation_status",
        )


def test_annotate_rejects_status_column_present_in_hits(tmp_path: Path) -> None:
    """A status column already in the hits table would emit a duplicate column name."""
    hits = write_tsv(
        tmp_path / "hits.tsv",
        f"{HITS_HEADER}\tvalidation_status",
        ["read_a\ts1\t10239\tstale"],
    )
    validation = write_tsv(tmp_path / "v.tsv", VAL_HEADER, [])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", [])
    with pytest.raises(ValueError, match="already present"):
        annotate_validation_status(
            str(hits),
            str(validation),
            str(sampled),
            str(tmp_path / "out.tsv"),
            "seq_id",
            "validation_status",
        )


def test_annotate_rejects_status_column_present_in_validation(tmp_path: Path) -> None:
    """The same applies when the validation table supplies the status column name."""
    hits = write_tsv(tmp_path / "hits.tsv", HITS_HEADER, ["read_a\ts1\t10239"])
    validation = write_tsv(
        tmp_path / "v.tsv", "seq_id\tvalidation_status", ["read_a\taligned"]
    )
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", ["read_a"])
    with pytest.raises(ValueError, match="already present"):
        annotate_validation_status(
            str(hits),
            str(validation),
            str(sampled),
            str(tmp_path / "out.tsv"),
            "seq_id",
            "validation_status",
        )


def test_annotate_rejects_sampled_read_absent_from_hits(tmp_path: Path) -> None:
    """A sampled read missing from the hits table means the inputs disagree.

    Without this check the read is dropped silently: no row, no status, no error.
    """
    hits = write_tsv(tmp_path / "hits.tsv", HITS_HEADER, ["read_a\ts1\t10239"])
    validation = write_tsv(tmp_path / "v.tsv", VAL_HEADER, [])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", ["read_a", "read_ghost"])
    with pytest.raises(ValueError, match="absent from the hits table"):
        annotate_validation_status(
            str(hits),
            str(validation),
            str(sampled),
            str(tmp_path / "out.tsv"),
            "seq_id",
            "validation_status",
        )


def test_annotate_accepts_unsampled_reads_in_hits(tmp_path: Path) -> None:
    """The consistency check is one-directional: hits may exceed the sampled set.

    That is the normal case -- most reads are never sampled -- so the new check must not
    reject it.
    """
    hits, validation, sampled = build_case(tmp_path)
    out = tmp_path / "out.tsv"
    counts = annotate_validation_status(
        str(hits),
        str(validation),
        str(sampled),
        str(out),
        "seq_id",
        "validation_status",
    )
    assert counts[STATUS_NOT_SAMPLED] == 1


def test_annotate_rejects_missing_key_column_in_hits(tmp_path: Path) -> None:
    """A hits table without the join key is a hard error."""
    hits = write_tsv(tmp_path / "hits.tsv", "other\tsample", ["x\ts1"])
    validation = write_tsv(tmp_path / "v.tsv", VAL_HEADER, [])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", [])
    with pytest.raises(ValueError, match="not found in header"):
        annotate_validation_status(
            str(hits),
            str(validation),
            str(sampled),
            str(tmp_path / "out.tsv"),
            "seq_id",
            "validation_status",
        )


def test_annotate_rejects_ragged_hits_row(tmp_path: Path) -> None:
    """A hits row with the wrong field count is a hard error."""
    hits = write_tsv(tmp_path / "hits.tsv", HITS_HEADER, ["read_a\ts1"])
    validation = write_tsv(tmp_path / "v.tsv", VAL_HEADER, [])
    sampled = write_tsv(tmp_path / "s.tsv", "seq_id", [])
    with pytest.raises(ValueError, match="fields, expected"):
        annotate_validation_status(
            str(hits),
            str(validation),
            str(sampled),
            str(tmp_path / "out.tsv"),
            "seq_id",
            "validation_status",
        )
