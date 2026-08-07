"""Tests for downsample_tsv_by_hash.py."""

import gzip
from pathlib import Path
from typing import Any

import pytest
from downsample_tsv_by_hash import (
    downsample_tsv_by_hash,
    hash_key,
    is_eligible,
    open_by_suffix,
    read_data_lines,
    read_key,
    read_key_index,
    same_compression,
    select_indices,
)

#############
# CONSTANTS #
#############

HEADER = "seq_id\tspecies\tscore"

####################
# HELPER FUNCTIONS #
####################


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


##################
# open_by_suffix #
##################


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


def test_hash_key_spans_the_full_128_bit_range() -> None:
    """Digests must be 128 bits wide, not merely below 2**128.

    An upper bound alone is satisfied by any narrower hash -- BLAKE2s at digest_size=8,
    or the CRC32 the module explicitly rejects -- so it would not catch a narrowing.
    Collision-freedom, and with it the order-independence guarantee, depends on the real
    width, so assert that some key actually reaches the top nibble. Deterministic: the
    keys and the hash are both fixed, so this either always passes or always fails.
    """
    values = [hash_key(f"read_{i}") for i in range(1000)]
    assert all(0 <= value < 2**128 for value in values)
    assert max(values) > 2**124


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


def test_read_data_lines_skips_whitespace_only_lines(tmp_path: Path) -> None:
    """A line of only spaces or tabs counts as blank, not as a ragged row."""
    path = tmp_path / "in.tsv"
    path.write_text(f"{HEADER}\nread_0\t10239\t0\n   \t \n")
    with open_by_suffix(str(path)) as fh:
        fh.readline()
        assert len(list(read_data_lines(fh))) == 1


############
# read_key #
############


@pytest.mark.parametrize(
    ("line", "key_index", "expected"),
    [
        ("read_0\t10239\t5\n", 0, "read_0"),  # first column
        ("read_0\t10239\t5\n", 1, "10239"),  # middle column
        ("read_0\t10239\t5\n", 2, "5"),  # last column: newline must be stripped
        ("read_0\t10239\t5", 2, "5"),  # last column, no trailing newline
    ],
)
def test_read_key_extracts_named_column(
    line: Any, key_index: Any, expected: Any
) -> None:
    """The key is taken from the given column index, without the line terminator."""
    assert read_key(line, key_index) == expected


def test_read_key_rejects_short_row() -> None:
    """A row too short to contain the key column is a hard error."""
    with pytest.raises(ValueError, match="not have enough fields"):
        read_key("only_one_field\n", 2)


def test_read_key_tolerates_tabs_beyond_the_key() -> None:
    """Bounded splitting must not corrupt a key that precedes further columns."""
    assert read_key("read_0\ta\tb\tc\td\n", 0) == "read_0"


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


####################
# same_compression #
####################


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        ("x.tsv", "y.tsv", True),
        ("x.tsv.gz", "y.tsv.gz", True),
        ("x.tsv", "y.tsv.gz", False),
        ("x.tsv.gz", "y.tsv", False),
    ],
)
def test_same_compression(a: Any, b: Any, expected: Any) -> None:
    """Compression is compared by suffix, in both directions."""
    assert same_compression(a, b) is expected


##################
# select_indices #
##################


@pytest.mark.parametrize(
    ("n_rows", "n_sample", "expected"),
    [
        (100, 10, 10),  # normal downsampling
        (10, 10, 10),  # target equals input: everything retained
        (10, 11, 10),  # target one above input
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
    # None signals "everything retained", which is n_rows rows
    assert (n_rows if selected is None else len(selected)) == expected
    assert n_total == n_rows


@pytest.mark.parametrize(("n_rows", "n_sample"), [(10, 10), (10, 11), (0, 5)])
def test_select_indices_signals_all_retained(n_rows: Any, n_sample: Any) -> None:
    """Inputs that fit under the cap return None, so the caller can copy verbatim."""
    keys = iter(f"read_{i}" for i in range(n_rows))
    selected, _ = select_indices(keys, n_sample)
    assert selected is None


def test_select_indices_does_not_signal_all_retained_when_capping() -> None:
    """One row over the cap is enough to switch to real selection."""
    keys = iter(f"read_{i}" for i in range(11))
    selected, _ = select_indices(keys, 10)
    assert selected is not None
    assert len(selected) == 10


def test_select_indices_zero_sample_is_not_all_retained() -> None:
    """A zero cap keeps nothing, which must not be confused with keeping everything."""
    selected, n_total = select_indices(iter(["a", "b"]), 0)
    assert selected == set()
    assert n_total == 2


def test_select_indices_is_order_independent() -> None:
    """Reversing the input selects the same keys, not the same positions."""
    keys = [f"read_{i}" for i in range(200)]
    forward, _ = select_indices(iter(keys), 20)
    backward, _ = select_indices(iter(reversed(keys)), 20)
    assert forward is not None and backward is not None
    reversed_keys = list(reversed(keys))
    assert {keys[i] for i in forward} == {reversed_keys[i] for i in backward}


@pytest.mark.parametrize("n_sample", [1, 2, 20, 199])
def test_select_indices_selects_smallest_hashes(n_sample: Any) -> None:
    """Selection is exactly the bottom-N of the hash distribution."""
    keys = [f"read_{i}" for i in range(200)]
    selected, _ = select_indices(iter(keys), n_sample)
    assert selected is not None
    assert {keys[i] for i in selected} == set(sorted(keys, key=hash_key)[:n_sample])


def test_select_indices_is_nested_across_sample_sizes() -> None:
    """A smaller sample is a subset of a larger one, so raising N only adds reads."""
    keys = [f"read_{i}" for i in range(500)]
    small, _ = select_indices(iter(keys), 10)
    large, _ = select_indices(iter(keys), 50)
    assert small is not None and large is not None
    assert small <= large


def test_select_indices_is_approximately_uniform() -> None:
    """Selection does not favour any region of the input (no order bias)."""
    keys = [f"read_{i}" for i in range(10000)]
    selected, _ = select_indices(iter(keys), 1000)
    assert selected is not None
    first_half = sum(1 for i in selected if i < 5000)
    # Under uniform sampling this is Binomial(1000, 0.5): 350/650 is ~9 sigma out.
    assert 350 < first_half < 650


##########################
# downsample_tsv_by_hash #
##########################


@pytest.mark.parametrize("suffix", ["tsv", "tsv.gz"])
def test_downsample_writes_header_and_sample(tmp_path: Path, suffix: Any) -> None:
    """Output carries the header plus exactly n_sample rows, compressed or not."""
    in_path = make_tsv(tmp_path, f"in.{suffix}", 100)
    out_path = tmp_path / f"out.{suffix}"
    downsample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 10)
    header, data = read_tsv(out_path)
    assert header == HEADER
    assert len(data) == 10


def test_downsample_is_reproducible(tmp_path: Path) -> None:
    """Two independent invocations select the same rows."""
    in_path = make_tsv(tmp_path, "in.tsv", 100)
    out_a, out_b = tmp_path / "a.tsv", tmp_path / "b.tsv"
    downsample_tsv_by_hash(str(in_path), str(out_a), "seq_id", 10)
    downsample_tsv_by_hash(str(in_path), str(out_b), "seq_id", 10)
    assert read_tsv(out_a) == read_tsv(out_b)


def test_downsample_preserves_input_order(tmp_path: Path) -> None:
    """Selected rows are written in input order, so upstream sorting survives."""
    in_path = make_tsv(tmp_path, "in.tsv", 200)
    out_path = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 20)
    _header, data = read_tsv(out_path)
    positions = [int(sid.removeprefix("read_")) for sid in seq_ids(data)]
    assert positions == sorted(positions)


@pytest.mark.parametrize("suffix", ["tsv", "tsv.gz"])
def test_downsample_retains_everything_under_the_cap(
    tmp_path: Path, suffix: Any
) -> None:
    """When the cap exceeds the row count, every row survives unchanged.

    This exercises the verbatim-copy path, so the content must still be correct.
    """
    in_path = make_tsv(tmp_path, f"in.{suffix}", 10)
    out_path = tmp_path / f"out.{suffix}"
    downsample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 1000)
    assert read_tsv(out_path) == read_tsv(in_path)


def test_downsample_recompresses_when_compression_differs(tmp_path: Path) -> None:
    """The verbatim-copy shortcut must not fire when in/out compression differs.

    A raw copy would otherwise write gzip bytes to a .tsv path (or vice versa).
    """
    in_path = make_tsv(tmp_path, "in.tsv.gz", 10)
    out_path = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 1000)
    # Readable as plaintext, and complete
    assert out_path.read_text().startswith(HEADER)
    assert read_tsv(out_path) == read_tsv(in_path)


def test_downsample_compresses_plain_input_to_gz(tmp_path: Path) -> None:
    """The reverse direction is also handled: plaintext in, gzip out."""
    in_path = make_tsv(tmp_path, "in.tsv", 10)
    out_path = tmp_path / "out.tsv.gz"
    downsample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 1000)
    with gzip.open(out_path, "rt") as fh:  # would raise if not really gzipped
        assert fh.readline().rstrip("\n") == HEADER
    assert read_tsv(out_path) == read_tsv(in_path)


def test_downsample_header_only_input(tmp_path: Path) -> None:
    """A header-only input yields a header-only output rather than failing."""
    in_path = make_tsv(tmp_path, "in.tsv", 0)
    out_path = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 10)
    header, data = read_tsv(out_path)
    assert header == HEADER
    assert data == []


def test_downsample_zero_sample_keeps_header_only(tmp_path: Path) -> None:
    """A zero cap writes the header and no rows."""
    in_path = make_tsv(tmp_path, "in.tsv", 10)
    out_path = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 0)
    header, data = read_tsv(out_path)
    assert header == HEADER
    assert data == []


def test_downsample_uses_named_key_column(tmp_path: Path) -> None:
    """Selection keys on the named column, not on column position."""
    in_path = tmp_path / "in.tsv"
    # seq_id is the third column; all rows share species and score
    rows = [f"10239\t0\tread_{i}" for i in range(100)]
    in_path.write_text("species\tscore\tseq_id\n" + "\n".join(rows) + "\n")
    out_path = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(in_path), str(out_path), "seq_id", 10)
    _header, data = read_tsv(out_path)
    keys = [line.split("\t")[2] for line in data]
    assert set(keys) == set(
        sorted((f"read_{i}" for i in range(100)), key=hash_key)[:10]
    )


def test_downsample_propagates_input_errors(tmp_path: Path) -> None:
    """Header validation errors surface from the top-level entry point too."""
    in_path = make_tsv(tmp_path, "in.tsv", 10)
    with pytest.raises(ValueError, match="not found in header"):
        downsample_tsv_by_hash(str(in_path), str(tmp_path / "out.tsv"), "missing", 5)


def test_downsample_is_independent_per_file(tmp_path: Path) -> None:
    """Sampling is per-file: splitting rows across files changes the sample.

    This documents the requirement that each partition arrives as a single file.
    """
    all_path = make_tsv(tmp_path, "all.tsv", 100)
    out_all = tmp_path / "out_all.tsv"
    downsample_tsv_by_hash(str(all_path), str(out_all), "seq_id", 10)
    half_path = tmp_path / "half.tsv"
    rows = [f"read_{i}\t10239\t{i}" for i in range(50)]
    half_path.write_text(HEADER + "\n" + "\n".join(rows) + "\n")
    out_half = tmp_path / "out_half.tsv"
    downsample_tsv_by_hash(str(half_path), str(out_half), "seq_id", 10)
    # The half-file sample is drawn only from the rows it contains
    assert set(seq_ids(read_tsv(out_half)[1])) <= {f"read_{i}" for i in range(50)}
    # ...and is therefore generally not the same as the whole-file sample
    assert set(seq_ids(read_tsv(out_half)[1])) != set(seq_ids(read_tsv(out_all)[1]))


##################
# match_columns  #
##################


def make_exemplar_tsv(tmp_path: Path, name: str, rows: list[tuple[str, str]]) -> Path:
    """Write a TSV of (seq_id, exemplar) rows plus a payload column."""
    path = tmp_path / name
    body = "".join(f"{a}\t{b}\tpayload\n" for a, b in rows)
    path.write_text("seq_id\texemplar\tpayload\n" + body)
    return path


@pytest.mark.parametrize(
    ("line", "indices", "expected"),
    [
        ("r1\tr1\tx\n", (0, 1), True),
        ("r2\tr1\tx\n", (0, 1), False),
        ("r2\tr1\tx\n", None, True),
    ],
)
def test_is_eligible(line: Any, indices: Any, expected: Any) -> None:
    """Eligibility is equality of the two named columns, or unconditional when unset."""
    assert is_eligible(line, indices) is expected


def test_match_columns_restricts_selection_to_matching_rows(tmp_path: Path) -> None:
    """Only rows whose two columns agree survive, and non-matching rows are dropped."""
    rows = [("r1", "r1"), ("r2", "r1"), ("r3", "r3"), ("r4", "r3")]
    src = make_exemplar_tsv(tmp_path, "in.tsv", rows)
    out = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(src), str(out), "seq_id", 10, ("seq_id", "exemplar"))
    kept = [ln.split("\t")[0] for ln in out.read_text().splitlines()[1:]]
    assert kept == ["r1", "r3"]


def test_match_columns_caps_the_eligible_rows_not_the_input(tmp_path: Path) -> None:
    """The cap applies after the restriction, so it bounds eligible rows only."""
    rows = [("r1", "r1"), ("r2", "r1"), ("r3", "r3"), ("r4", "r3"), ("r5", "r5")]
    src = make_exemplar_tsv(tmp_path, "in.tsv", rows)
    out = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(src), str(out), "seq_id", 2, ("seq_id", "exemplar"))
    kept = [ln.split("\t")[0] for ln in out.read_text().splitlines()[1:]]
    assert len(kept) == 2
    assert set(kept) <= {"r1", "r3", "r5"}


def test_match_columns_writes_the_rows_it_selected(tmp_path: Path) -> None:
    """The two passes agree on row indices, so the payload matches the selected seq_id.

    Filtering shifts indices between the counting and writing passes; if they disagreed
    the wrong rows would be emitted, which a seq_id-only check would not detect.
    """
    rows = [(f"r{i}", f"r{i}" if i % 2 else "other") for i in range(1, 9)]
    path = tmp_path / "in.tsv"
    path.write_text(
        "seq_id\texemplar\tpayload\n" + "".join(f"{a}\t{b}\tpay_{a}\n" for a, b in rows)
    )
    out = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(path), str(out), "seq_id", 3, ("seq_id", "exemplar"))
    for line in out.read_text().splitlines()[1:]:
        seq_id, exemplar, payload = line.split("\t")
        assert seq_id == exemplar
        assert payload == f"pay_{seq_id}"


def test_match_columns_with_no_eligible_rows_yields_header_only(tmp_path: Path) -> None:
    """An empty eligible set is a valid outcome, not an error."""
    src = make_exemplar_tsv(tmp_path, "in.tsv", [("r1", "x"), ("r2", "y")])
    out = tmp_path / "out.tsv"
    downsample_tsv_by_hash(str(src), str(out), "seq_id", 5, ("seq_id", "exemplar"))
    assert out.read_text().splitlines() == ["seq_id\texemplar\tpayload"]


def test_match_columns_rejects_a_missing_column(tmp_path: Path) -> None:
    """A named column that is absent is a hard error rather than a silent no-op."""
    src = make_exemplar_tsv(tmp_path, "in.tsv", [("r1", "r1")])
    out = tmp_path / "out.tsv"
    with pytest.raises(ValueError, match="not found in header"):
        downsample_tsv_by_hash(str(src), str(out), "seq_id", 5, ("seq_id", "missing"))
