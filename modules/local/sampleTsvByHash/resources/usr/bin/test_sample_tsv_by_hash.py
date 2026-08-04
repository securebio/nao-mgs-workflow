"""Tests for sample_tsv_by_hash.py."""

import gzip
from pathlib import Path
from typing import Any

import pytest
from sample_tsv_by_hash import (
    hash_key,
    open_by_suffix,
    read_data_lines,
    read_key,
    read_key_index,
    sample_tsv_by_hash,
    select_indices,
)

#############
# CONSTANTS #
#############

HEADER = "seq_id\tspecies\tscore"


def make_tsv(tmp_path: Path, name: str, n_rows: int, header: str = HEADER) -> Path:
    """Write a TSV with n_rows data rows and return its path."""
    path = tmp_path / name
    lines = [header] + [f"read_{i}\t10239\t{i}" for i in range(n_rows)]
    opener = gzip.open if name.endswith(".gz") else open
    with opener(path, "wt") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


def read_tsv(path: Path) -> tuple[str, list[str]]:
    """Read a TSV and return (header, list of data lines)."""
    with open_by_suffix(str(path)) as fh:
        lines = [line.rstrip("\n") for line in fh if line.strip()]
    return lines[0], lines[1:]


def seq_ids(data_lines: list[str]) -> list[str]:
    """Extract the seq_id column from data lines."""
    return [line.split("\t")[0] for line in data_lines]


####################
# open_by_suffix   #
####################


@pytest.mark.parametrize("name", ["plain.tsv", "compressed.tsv.gz"])
def test_open_by_suffix_roundtrip(tmp_path: Path, name: Any) -> None:
    """Both plain and gzipped paths round-trip through open_by_suffix."""
    path = tmp_path / name
    with open_by_suffix(str(path), "w") as fh:
        fh.write("hello\n")
    with open_by_suffix(str(path)) as fh:
        assert fh.read() == "hello\n"


############
# hash_key #
############


def test_hash_key_is_deterministic_across_calls() -> None:
    """The same key always hashes to the same value (not salted per process)."""
    assert hash_key("read_1") == hash_key("read_1")


def test_hash_key_is_128_bit() -> None:
    """Digests span the full 128-bit range, so ties are unreachable in practice."""
    assert 0 <= hash_key("read_1") < 2**128


def test_hash_key_distinguishes_similar_keys() -> None:
    """Near-identical keys map to unrelated values."""
    assert hash_key("read_1") != hash_key("read_2")


###################
# read_data_lines #
###################


def test_read_data_lines_yields_full_lines(tmp_path: Path) -> None:
    """Data lines are yielded unmodified, including the trailing newline."""
    path = make_tsv(tmp_path, "in.tsv", 2)
    with open_by_suffix(str(path)) as fh:
        fh.readline()  # discard header
        assert list(read_data_lines(fh)) == [
            "read_0\t10239\t0\n",
            "read_1\t10239\t1\n",
        ]


def test_read_data_lines_skips_blank_lines(tmp_path: Path) -> None:
    """Blank lines are ignored, so both passes agree on row indices."""
    path = tmp_path / "in.tsv"
    path.write_text(f"{HEADER}\nread_0\t10239\t0\n\nread_1\t10239\t1\n")
    with open_by_suffix(str(path)) as fh:
        fh.readline()
        assert len(list(read_data_lines(fh))) == 2


############
# read_key #
############


def test_read_key_extracts_named_column() -> None:
    """The key is taken from the given column index."""
    assert read_key("read_0\t10239\t5\n", 0) == "read_0"
    assert read_key("read_0\t10239\t5\n", 1) == "10239"


def test_read_key_rejects_short_row() -> None:
    """A row too short to contain the key column is a hard error."""
    with pytest.raises(ValueError, match="not have enough fields"):
        read_key("only_one_field\n", 2)


##################
# read_key_index #
##################


def test_read_key_index_locates_column(tmp_path: Path) -> None:
    """The header is returned with a trailing newline, alongside the key index."""
    path = make_tsv(tmp_path, "in.tsv", 1)
    with open_by_suffix(str(path)) as fh:
        header_line, key_index = read_key_index(fh, "score", str(path))
    assert header_line == HEADER + "\n"
    assert key_index == 2


def test_read_key_index_rejects_empty_file(tmp_path: Path) -> None:
    """A completely empty input is a hard error, not a silent empty output."""
    path = tmp_path / "empty.tsv"
    path.write_text("")
    with open_by_suffix(str(path)) as fh, pytest.raises(ValueError, match="empty"):
        read_key_index(fh, "seq_id", str(path))


def test_read_key_index_rejects_missing_column(tmp_path: Path) -> None:
    """An absent key column is a hard error naming the available columns."""
    path = make_tsv(tmp_path, "in.tsv", 1)
    with (
        open_by_suffix(str(path)) as fh,
        pytest.raises(ValueError, match="not found in header"),
    ):
        read_key_index(fh, "missing", str(path))


##################
# select_indices #
##################


@pytest.mark.parametrize(
    ("n_rows", "n_sample", "expected"),
    [
        (100, 10, 10),  # normal downsampling
        (10, 100, 10),  # target exceeds input: keep everything
        (10, 10, 10),  # target equals input
        (10, 0, 0),  # zero target keeps nothing
        (10, -5, 0),  # negative target keeps nothing
        (0, 10, 0),  # empty input
    ],
)
def test_select_indices_returns_expected_count(
    n_rows: Any, n_sample: Any, expected: Any
) -> None:
    """Selection size is min(n_sample, n_rows), clamped at zero."""
    keys = iter(f"read_{i}" for i in range(n_rows))
    selected, n_total = select_indices(keys, n_sample)
    assert len(selected) == expected
    assert n_total == n_rows


def test_select_indices_is_order_independent() -> None:
    """Reversing the input selects the same keys, not the same positions."""
    keys = [f"read_{i}" for i in range(200)]
    forward, _ = select_indices(iter(keys), 20)
    backward, _ = select_indices(iter(reversed(keys)), 20)
    assert {keys[i] for i in forward} == {list(reversed(keys))[i] for i in backward}


def test_select_indices_selects_smallest_hashes() -> None:
    """Selection is exactly the bottom-N of the hash distribution."""
    keys = [f"read_{i}" for i in range(200)]
    selected, _ = select_indices(iter(keys), 20)
    assert {keys[i] for i in selected} == set(sorted(keys, key=hash_key)[:20])


def test_select_indices_is_nested_across_sample_sizes() -> None:
    """A smaller sample is a subset of a larger one, so raising N only adds reads."""
    keys = [f"read_{i}" for i in range(500)]
    small, _ = select_indices(iter(keys), 10)
    large, _ = select_indices(iter(keys), 50)
    assert small <= large


def test_select_indices_is_approximately_uniform() -> None:
    """Selection does not favour any region of the input (no order bias)."""
    keys = [f"read_{i}" for i in range(10000)]
    selected, _ = select_indices(iter(keys), 1000)
    first_half = sum(1 for i in selected if i < 5000)
    # Under uniform sampling this is Binomial(1000, 0.5): 350/650 is ~9 sigma out.
    assert 350 < first_half < 650


#######################
# sample_tsv_by_hash  #
#######################


@pytest.mark.parametrize("suffix", ["tsv", "tsv.gz"])
def test_sample_tsv_by_hash_writes_header_and_sample(
    tmp_path: Path, suffix: Any
) -> None:
    """Output carries the header plus exactly n_sample rows, compressed or not."""
    in_path = make_tsv(tmp_path, f"in.{suffix}", 100)
    out_path = tmp_path / f"out.{suffix}"
    sample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 10)
    header, data = read_tsv(out_path)
    assert header == HEADER
    assert len(data) == 10


def test_sample_tsv_by_hash_is_reproducible(tmp_path: Path) -> None:
    """Two independent invocations select the same rows."""
    in_path = make_tsv(tmp_path, "in.tsv", 100)
    out_a, out_b = tmp_path / "a.tsv", tmp_path / "b.tsv"
    sample_tsv_by_hash(str(in_path), str(out_a), "seq_id", 10)
    sample_tsv_by_hash(str(in_path), str(out_b), "seq_id", 10)
    assert read_tsv(out_a) == read_tsv(out_b)


def test_sample_tsv_by_hash_header_only_input(tmp_path: Path) -> None:
    """A header-only input yields a header-only output rather than failing."""
    in_path = make_tsv(tmp_path, "in.tsv", 0)
    out_path = tmp_path / "out.tsv"
    sample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 10)
    header, data = read_tsv(out_path)
    assert header == HEADER
    assert data == []


def test_sample_tsv_by_hash_uses_named_key_column(tmp_path: Path) -> None:
    """Selection keys on the named column, not on column position."""
    in_path = tmp_path / "in.tsv"
    # seq_id is the third column; all rows share species and score
    rows = [f"10239\t0\tread_{i}" for i in range(100)]
    in_path.write_text("species\tscore\tseq_id\n" + "\n".join(rows) + "\n")
    out_path = tmp_path / "out.tsv"
    sample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 10)
    _header, data = read_tsv(out_path)
    keys = [line.split("\t")[2] for line in data]
    assert set(keys) == set(
        sorted((f"read_{i}" for i in range(100)), key=hash_key)[:10]
    )


def test_sample_tsv_by_hash_preserves_input_order(tmp_path: Path) -> None:
    """Selected rows are written in input order, so upstream sorting survives."""
    in_path = make_tsv(tmp_path, "in.tsv", 200)
    out_path = tmp_path / "out.tsv"
    sample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 20)
    _header, data = read_tsv(out_path)
    positions = [int(sid.removeprefix("read_")) for sid in seq_ids(data)]
    assert positions == sorted(positions)


def test_sample_tsv_by_hash_propagates_input_errors(tmp_path: Path) -> None:
    """Header validation errors surface from the top-level entry point too."""
    in_path = make_tsv(tmp_path, "in.tsv", 10)
    with pytest.raises(ValueError, match="not found in header"):
        sample_tsv_by_hash(str(in_path), str(tmp_path / "out.tsv"), "missing", 5)


def test_sample_tsv_by_hash_partition_independence(tmp_path: Path) -> None:
    """Sampling is per-file: splitting a species across files changes the sample.

    This documents the requirement that each species arrives as a single partition.
    """
    all_path = make_tsv(tmp_path, "all.tsv", 100)
    out_all = tmp_path / "out_all.tsv"
    sample_tsv_by_hash(str(all_path), str(out_all), "seq_id", 10)
    half_path = tmp_path / "half.tsv"
    rows = [f"read_{i}\t10239\t{i}" for i in range(50)]
    half_path.write_text(HEADER + "\n" + "\n".join(rows) + "\n")
    out_half = tmp_path / "out_half.tsv"
    sample_tsv_by_hash(str(half_path), str(out_half), "seq_id", 10)
    # The half-file sample is drawn only from the rows it contains
    assert set(seq_ids(read_tsv(out_half)[1])) <= {f"read_{i}" for i in range(50)}
    # ...and is therefore generally not the same as the whole-file sample
    assert set(seq_ids(read_tsv(out_half)[1])) != set(seq_ids(read_tsv(out_all)[1]))
