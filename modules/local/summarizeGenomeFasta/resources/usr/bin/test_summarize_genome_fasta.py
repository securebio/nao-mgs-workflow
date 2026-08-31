"""Tests for summarize_genome_fasta.py."""

import gzip
from pathlib import Path

import pytest
from summarize_genome_fasta import (
    GenomeRecord,
    hash_sequence,
    iter_genome_records,
    normalise_sequence,
    parse_genome_id,
    reverse_complement,
    summarise_genome_fasta,
)


def write_fasta(path: Path, records: list[tuple[str, str]], wrap: int = 0) -> str:
    """Write a FASTA file, gzipped if the path ends in .gz.

    Args:
        path: Destination path.
        records: (header, sequence) pairs.
        wrap: Line width for the sequence, or 0 for one line per record.
    """
    lines = []
    for header, seq in records:
        lines.append(f">{header}")
        if wrap:
            lines.extend(seq[i : i + wrap] for i in range(0, len(seq), wrap))
        else:
            lines.append(seq)
    text = "\n".join(lines) + "\n"
    if str(path).endswith(".gz"):
        with gzip.open(path, "wt") as f:
            f.write(text)
    else:
        path.write_text(text)
    return str(path)


def read_summary(path: str) -> list[list[str]]:
    """Read a summary TSV into a list of rows, header included."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:  # type: ignore[operator]
        return [line.rstrip("\n").split("\t") for line in f]


#######################
# normalise_sequence  #
#######################


@pytest.mark.parametrize(
    ("sequence", "expected"),
    [
        (b"ACGT", b"ACGT"),
        (b"acgt", b"ACGT"),
        (b"ACGU", b"ACGT"),
        (b"acgu", b"ACGT"),
        (b"NRYSWKMBDHV", b"NRYSWKMBDHV"),
        (b"nryswkmbdhv", b"NRYSWKMBDHV"),
    ],
)
def test_normalise_sequence_uppercases_and_maps_u_to_t(
    sequence: bytes, expected: bytes
) -> None:
    assert normalise_sequence(sequence, "ID1") == expected


@pytest.mark.parametrize("symbol", [b"X", b"Z", b"-", b"*", b" "])
def test_normalise_sequence_rejects_non_iupac(symbol: bytes) -> None:
    with pytest.raises(ValueError, match="non-IUPAC symbol"):
        normalise_sequence(b"ACGT" + symbol, "ID1")


#######################
# reverse_complement  #
#######################


@pytest.mark.parametrize(
    ("sequence", "expected"),
    [
        (b"ACGT", b"ACGT"),  # palindrome
        (b"AAAA", b"TTTT"),
        (b"GGGGGGGG", b"CCCCCCCC"),
        (b"ACGTN", b"NACGT"),
        (b"RYKM", b"KMRY"),
        (b"SWN", b"NWS"),  # self-complementary codes
    ],
)
def test_reverse_complement(sequence: bytes, expected: bytes) -> None:
    assert reverse_complement(sequence) == expected


def test_reverse_complement_is_an_involution() -> None:
    # The canonical form is only well defined if complementing twice is the
    # identity, so this holds for every IUPAC code.
    sequence = b"ACGTRYSWKMBDHVN"
    assert reverse_complement(reverse_complement(sequence)) == sequence


##################
# hash_sequence  #
##################


# Pins the published seq_hash format. Downstream tables key on this value, so a
# change to the digest, the digest size or the canonical form is a contract
# change and must be a deliberate edit to this literal, not a silent drift.
EXPECTED_DIGEST_ACGTACGTAA = (
    "8b3d25ee6eb4a46a57340fb7db31c86cf27bc1571809cd4cd3851d7dfa158ca5"
)


def test_hash_sequence_matches_pinned_digest() -> None:
    assert hash_sequence(b"ACGTACGTAA") == EXPECTED_DIGEST_ACGTACGTAA
    assert len(EXPECTED_DIGEST_ACGTACGTAA) == 64  # 32-byte digest, hex encoded


def test_hash_sequence_matches_reverse_complement() -> None:
    assert hash_sequence(b"AAAACCCGGT") == hash_sequence(
        reverse_complement(b"AAAACCCGGT")
    )


def test_hash_sequence_distinguishes_different_sequences() -> None:
    assert hash_sequence(b"ACGTACGTAA") != hash_sequence(b"ACGTACGTAC")


def test_hash_sequence_ignores_case_and_u_via_normalisation() -> None:
    assert hash_sequence(normalise_sequence(b"acgu", "a")) == hash_sequence(b"ACGT")


###################
# GenomeRecord    #
###################


def test_genome_record_is_a_named_tuple() -> None:
    record = GenomeRecord("S1", 4, "abc")
    assert (record.genome_id, record.seq_length, record.seq_hash) == ("S1", 4, "abc")


#####################
# parse_genome_id   #
#####################


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (b">S1", "S1"),
        (b">S1 description here", "S1"),
        (b">S1\tdescription", "S1"),
    ],
)
def test_parse_genome_id(header: bytes, expected: str) -> None:
    assert parse_genome_id(header, 1, "f.fasta") == expected


########################
# iter_genome_records  #
########################


def test_iter_genome_records_reads_ids_lengths_and_order(tmp_path: Path) -> None:
    path = write_fasta(
        tmp_path / "g.fasta",
        [("S1 first record", "ACGTACGTAA"), ("S2 second", "TTTTGGGGCC")],
    )
    records = list(iter_genome_records(path))
    assert [r.genome_id for r in records] == ["S1", "S2"]
    assert [r.seq_length for r in records] == [10, 10]


def test_iter_genome_records_joins_wrapped_lines(tmp_path: Path) -> None:
    sequence = "ACGTACGTAA" * 5
    wrapped = write_fasta(tmp_path / "w.fasta", [("S1", sequence)], wrap=7)
    flat = write_fasta(tmp_path / "f.fasta", [("S1", sequence)])
    assert list(iter_genome_records(wrapped)) == list(iter_genome_records(flat))


def test_iter_genome_records_handles_gzip(tmp_path: Path) -> None:
    plain = write_fasta(tmp_path / "g.fasta", [("S1", "ACGTACGTAA")])
    gzipped = write_fasta(tmp_path / "g.fasta.gz", [("S1", "ACGTACGTAA")])
    assert list(iter_genome_records(plain)) == list(iter_genome_records(gzipped))


def test_iter_genome_records_groups_reverse_complements(tmp_path: Path) -> None:
    path = write_fasta(
        tmp_path / "rc.fasta", [("FWD", "AAAACCCGGT"), ("REV", "ACCGGGTTTT")]
    )
    hashes = {r.genome_id: r.seq_hash for r in iter_genome_records(path)}
    assert hashes["FWD"] == hashes["REV"]


@pytest.mark.parametrize(
    ("sequence_line", "match"),
    [
        (b"ACGT ", "non-IUPAC symbol"),  # trailing space
        (b"AC GT", "non-IUPAC symbol"),  # internal space
        (b"ACGT\t", "non-IUPAC symbol"),  # trailing tab
    ],
)
def test_iter_genome_records_rejects_whitespace_in_sequence(
    tmp_path: Path, sequence_line: bytes, match: str
) -> None:
    # Stripping all whitespace rather than just line endings would let these
    # through, defeating the alphabet check.
    path = tmp_path / "ws.fasta"
    path.write_bytes(b">S1\n" + sequence_line + b"\n")
    with pytest.raises(ValueError, match=match):
        list(iter_genome_records(str(path)))


def test_iter_genome_records_tolerates_crlf(tmp_path: Path) -> None:
    crlf = tmp_path / "crlf.fasta"
    crlf.write_bytes(b">S1 desc\r\nACGTACGTAA\r\n")
    lf = tmp_path / "lf.fasta"
    lf.write_bytes(b">S1 desc\nACGTACGTAA\n")
    assert list(iter_genome_records(str(crlf))) == list(iter_genome_records(str(lf)))


def test_iter_genome_records_rejects_content_before_first_header(
    tmp_path: Path,
) -> None:
    # Silently dropping it would omit sequence from the summary while the task
    # still succeeded.
    path = tmp_path / "pre.fasta"
    path.write_text("ACGTACGTAA\n>S1\nTTTTGGGGCC\n")
    with pytest.raises(ValueError, match="Content before the first header"):
        list(iter_genome_records(str(path)))


def test_iter_genome_records_allows_blank_lines_before_first_header(
    tmp_path: Path,
) -> None:
    path = tmp_path / "blank.fasta"
    path.write_text("\n\n>S1\nACGTACGTAA\n")
    assert [r.genome_id for r in iter_genome_records(str(path))] == ["S1"]


def test_iter_genome_records_rejects_headerless_id(tmp_path: Path) -> None:
    path = tmp_path / "bad.fasta"
    path.write_text(">\nACGT\n")
    with pytest.raises(ValueError, match="no sequence ID"):
        list(iter_genome_records(str(path)))


###########################
# summarise_genome_fasta  #
###########################


def test_summarise_genome_fasta_writes_one_row_per_record(tmp_path: Path) -> None:
    fasta = write_fasta(
        tmp_path / "g.fasta", [("S1 desc", "ACGTACGTAA"), ("S2", "TTTTGGGGCC")]
    )
    out = str(tmp_path / "summary.tsv.gz")
    assert summarise_genome_fasta(fasta, out) == 2
    rows = read_summary(out)
    assert rows[0] == ["genome_id", "seq_length", "seq_hash"]
    assert [r[0] for r in rows[1:]] == ["S1", "S2"]
    assert [r[1] for r in rows[1:]] == ["10", "10"]


def test_summarise_genome_fasta_gives_duplicates_one_hash(tmp_path: Path) -> None:
    # The whole point of the module: sequence-identical records, and a
    # reverse-complement pair, collapse to one hash; a distinct sequence does not.
    fasta = write_fasta(
        tmp_path / "g.fasta",
        [
            ("SAME_A", "AAAACCCGGT"),
            ("SAME_B", "AAAACCCGGT"),
            ("RC_OF_SAME", "ACCGGGTTTT"),
            ("DIFFERENT", "AAAACCCGGA"),
        ],
    )
    out = str(tmp_path / "summary.tsv")
    summarise_genome_fasta(fasta, out)
    hashes = {row[0]: row[2] for row in read_summary(out)[1:]}
    assert hashes["SAME_A"] == hashes["SAME_B"] == hashes["RC_OF_SAME"]
    assert hashes["DIFFERENT"] != hashes["SAME_A"]


def test_summarise_genome_fasta_rejects_duplicate_ids(tmp_path: Path) -> None:
    fasta = write_fasta(
        tmp_path / "g.fasta", [("S1", "ACGTACGTAA"), ("S1", "TTTTGGGGCC")]
    )
    with pytest.raises(ValueError, match="Duplicate sequence ID S1"):
        summarise_genome_fasta(fasta, str(tmp_path / "out.tsv"))


def test_summarise_genome_fasta_rejects_empty_fasta(tmp_path: Path) -> None:
    empty = tmp_path / "empty.fasta"
    empty.write_text("")
    with pytest.raises(ValueError, match="No sequence headers"):
        summarise_genome_fasta(str(empty), str(tmp_path / "out.tsv"))


def test_summarise_genome_fasta_propagates_bad_alphabet(tmp_path: Path) -> None:
    fasta = write_fasta(tmp_path / "g.fasta", [("S1", "ACGTX")])
    with pytest.raises(ValueError, match="non-IUPAC symbol"):
        summarise_genome_fasta(fasta, str(tmp_path / "out.tsv"))
