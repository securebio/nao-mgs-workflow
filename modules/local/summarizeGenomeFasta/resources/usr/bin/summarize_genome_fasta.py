#!/usr/bin/env python

"""Summarise a genome FASTA as one row per record.

Emits `genome_id`, `seq_length` and `seq_hash` for every record, in input order,
in a single streaming pass. `seq_hash` is a digest of the canonicalised
sequence, so downstream steps can group records by sequence identity without
carrying sequence bytes through a sort or a join.

Two records share a `seq_hash` exactly when their sequences are identical after
normalisation, or are reverse complements of one another. Reverse complements
count as identical because every aligner this database feeds is
strand-agnostic. Equality is *not* modulo circular rotation.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import hashlib
import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import IO, NamedTuple, cast

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created, UTC).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(UTCFormatter("[%(asctime)s] %(message)s"))
logger.handlers.clear()
logger.addHandler(handler)

#############
# CONSTANTS #
#############

# Digest size in bytes. 256 bits makes accidental collisions unreachable at any
# genome-database scale, and costs one fixed-width column either way.
DIGEST_BYTES = 32

# IUPAC nucleotide codes and their complements. The mapping is an involution:
# complementing twice is the identity, which is what makes taking the
# lexicographic minimum of a sequence and its reverse complement a well-defined
# canonical form. S, W and N are self-complementary.
IUPAC_CODES = b"ACGTRYSWKMBDHVN"
IUPAC_COMPLEMENTS = b"TGCAYRSWMKVHDBN"
COMPLEMENT_TABLE = bytes.maketrans(IUPAC_CODES, IUPAC_COMPLEMENTS)

# Normalisation applied before hashing: upper-case, and U -> T so RNA and DNA
# spellings of the same sequence agree (and so the complement table stays an
# involution, which it would not be with U present).
NORMALISE_TABLE = bytes.maketrans(b"acgturyswkmbdhvnU", b"ACGTTRYSWKMBDHVNT")

ALLOWED_CODES = frozenset(IUPAC_CODES)

OUTPUT_COLUMNS = ["genome_id", "seq_length", "seq_hash"]

#############
# FUNCTIONS #
#############


def open_by_suffix(path: str, mode: str = "r") -> IO[bytes]:
    """Open a file in binary mode, transparently handling .gz compression.

    Args:
        path: Path to open.
        mode: Base mode, without the binary flag.
    Returns:
        Binary file object.
    """
    if path.endswith(".gz"):
        return cast(IO[bytes], gzip.open(path, mode + "b"))
    return cast(IO[bytes], open(path, mode + "b"))


def normalise_sequence(sequence: bytes, genome_id: str) -> bytes:
    """Upper-case a sequence and map U to T, rejecting non-IUPAC symbols.

    Args:
        sequence: Raw concatenated sequence bytes for one record.
        genome_id: Sequence ID, for the error message.
    Returns:
        Normalised sequence bytes.
    Raises:
        ValueError: If the sequence contains a symbol outside the IUPAC set.
    """
    normalised = sequence.translate(NORMALISE_TABLE)
    unexpected = sorted(set(normalised) - ALLOWED_CODES)
    if unexpected:
        symbols = ", ".join(repr(chr(code)) for code in unexpected)
        raise ValueError(
            f"Sequence {genome_id} contains non-IUPAC symbol(s) {symbols}; "
            "refusing to hash a sequence whose alphabet we do not understand"
        )
    return normalised


def reverse_complement(sequence: bytes) -> bytes:
    """Reverse-complement a normalised sequence."""
    return sequence.translate(COMPLEMENT_TABLE)[::-1]


def hash_sequence(sequence: bytes) -> str:
    """Hash a normalised sequence in a strand-independent way.

    Hashes the lexicographic minimum of the sequence and its reverse
    complement, so a record and its reverse complement produce one digest.

    Args:
        sequence: Normalised sequence bytes.
    Returns:
        Hex digest of the canonical form.
    """
    canonical = min(sequence, reverse_complement(sequence))
    return hashlib.blake2b(canonical, digest_size=DIGEST_BYTES).hexdigest()


class GenomeRecord(NamedTuple):
    """A FASTA record reduced to what grouping needs.

    Attributes:
        genome_id: First whitespace-delimited token of the header.
        seq_length: Length of the normalised sequence.
        seq_hash: Digest of the canonicalised sequence.
    """

    genome_id: str
    seq_length: int
    seq_hash: str


def parse_genome_id(header_line: bytes, line_no: int, path: str) -> str:
    """Extract the sequence ID from a FASTA header line.

    Args:
        header_line: Header line, including the leading '>'.
        line_no: Line number, for the error message.
        path: Path being read, for the error message.
    Returns:
        The first whitespace-delimited token after '>'.
    Raises:
        ValueError: If the header carries no ID.
    """
    tokens = header_line[1:].split(maxsplit=1)
    if not tokens:
        raise ValueError(f"Header with no sequence ID at line {line_no} of {path}")
    return tokens[0].decode()


def iter_genome_records(path: str) -> Iterator[GenomeRecord]:
    """Stream a FASTA, yielding one summary per record in input order.

    Holds at most one record's sequence in memory, so peak memory is set by the
    longest sequence rather than by the size of the file.

    Args:
        path: Path to a (optionally gzipped) FASTA file.
    Yields:
        One GenomeRecord per FASTA record.
    Raises:
        ValueError: If a header carries no ID, a sequence has a symbol outside
            the IUPAC set, or the file has content before its first header.
    """
    with open_by_suffix(path) as f:
        genome_id: str | None = None
        chunks: list[bytes] = []
        for line_no, line in enumerate(f, start=1):
            if line.startswith(b">"):
                if genome_id is not None:
                    yield summarise_record(genome_id, chunks)
                genome_id = parse_genome_id(line, line_no, path)
                chunks = []
            else:
                # Strip line endings only. Stripping all whitespace would let a
                # stray space or tab inside a sequence line pass the alphabet
                # check silently, which is exactly what that check exists to catch.
                sequence_line = line.rstrip(b"\r\n")
                # Content before the first header is not part of any record, so
                # accepting it would silently drop sequence from the summary.
                if genome_id is None:
                    if sequence_line:
                        raise ValueError(
                            f"Content before the first header at line {line_no} of "
                            f"{path}: {sequence_line[:40]!r}"
                        )
                    continue
                chunks.append(sequence_line)
        if genome_id is not None:
            yield summarise_record(genome_id, chunks)


def summarise_record(genome_id: str, chunks: list[bytes]) -> GenomeRecord:
    """Reduce one record's accumulated sequence lines to a summary.

    Args:
        genome_id: Sequence ID from the record's header.
        chunks: Sequence lines, stripped of line endings.
    Returns:
        Summary of the record.
    """
    sequence = normalise_sequence(b"".join(chunks), genome_id)
    return GenomeRecord(genome_id, len(sequence), hash_sequence(sequence))


def summarise_genome_fasta(fasta_path: str, output_path: str) -> int:
    """Write one summary row per record in a genome FASTA.

    Args:
        fasta_path: Path to the genome FASTA to summarise.
        output_path: Output path for the summary TSV (gzip if .gz).
    Returns:
        Number of records summarised.
    Raises:
        ValueError: If the FASTA contains no records, a header carries no ID, a
            sequence has a non-IUPAC symbol, or a genome_id repeats.
    """
    seen: set[str] = set()
    n_records = 0
    with open_by_suffix(output_path, "w") as f_out:
        f_out.write(("\t".join(OUTPUT_COLUMNS) + "\n").encode())
        for record in iter_genome_records(fasta_path):
            # One summary row has to describe one sequence, so a repeated ID
            # would make the row ambiguous for every downstream join.
            if record.genome_id in seen:
                raise ValueError(
                    f"Duplicate sequence ID {record.genome_id} in {fasta_path}; "
                    "one summary row cannot describe two records"
                )
            seen.add(record.genome_id)
            f_out.write(
                f"{record.genome_id}\t{record.seq_length}\t{record.seq_hash}\n".encode()
            )
            n_records += 1
    if n_records == 0:
        raise ValueError(f"No sequence headers found in {fasta_path}")
    logger.info("Summarised %d record(s) from %s", n_records, fasta_path)
    return n_records


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fasta", help="Path to the genome FASTA to summarise.")
    parser.add_argument("output", help="Output path for the summary TSV (gzip).")
    return parser.parse_args()


def main() -> None:
    start_time = time.time()
    logger.info("Starting summarize_genome_fasta.")
    args = parse_arguments()
    summarise_genome_fasta(args.fasta, args.output)
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
