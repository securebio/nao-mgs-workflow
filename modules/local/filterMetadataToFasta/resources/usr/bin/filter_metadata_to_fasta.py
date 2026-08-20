#!/usr/bin/env python
"""Filter genome metadata down to one row per sequence in a genome FASTA."""

import argparse
import csv
import gzip
import io
import logging
import time
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import IO, NamedTuple, cast


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


def open_by_suffix(path: str, mode: str = "r", newline: str | None = None) -> IO[str]:
    """Open a file, transparently handling .gz compression (text mode)."""
    if path.endswith(".gz"):
        return cast(IO[str], gzip.open(path, mode + "t", newline=newline))
    return cast(IO[str], open(path, mode, newline=newline))


# Columns describing the assembly a sequence was packaged in rather than the
# sequence itself; rows sharing a genome_id may legitimately disagree on these.
# Every other column must agree.
ASSEMBLY_FIELDS = frozenset(
    {"assembly_accession", "source_database", "assembly_status", "release_date"}
)


class MetadataSchema(NamedTuple):
    """Positions of the metadata columns filtering depends on.
    Attributes:
        column_names: Column names from the metadata header row.
        genome_id_idx: Index of the genome_id column.
        assembly_accession_idx: Index of the assembly_accession column.
        idxs_to_compare: Indices of the columns that rows sharing a genome_id
            must agree on.
    """

    column_names: list[str]
    genome_id_idx: int
    assembly_accession_idx: int
    idxs_to_compare: list[int]


def read_metadata_schema(column_names: list[str]) -> MetadataSchema:
    """Locate the columns filtering depends on within a metadata header.
    Args:
        column_names: Column names from the metadata header row.
    Returns:
        Schema giving the index of each column filtering reads.
    """
    return MetadataSchema(
        column_names=column_names,
        genome_id_idx=column_names.index("genome_id"),
        assembly_accession_idx=column_names.index("assembly_accession"),
        idxs_to_compare=[
            i for i, c in enumerate(column_names) if c not in ASSEMBLY_FIELDS
        ],
    )


def encode_row(row: list[str]) -> str:
    """Render a row as a TSV line, quoting it as csv would."""
    buffer = io.StringIO()
    csv.writer(buffer, delimiter="\t", lineterminator="\n").writerow(row)
    return buffer.getvalue()


def decode_row(line: str) -> list[str]:
    """Parse an encoded TSV line back into fields."""
    return next(csv.reader(io.StringIO(line), delimiter="\t"))


def read_fasta_genome_ids(fasta_path: str) -> set[str]:
    """Collect sequence IDs from a FASTA file's headers.
    The ID is the first whitespace-delimited token after '>'.
    Args:
        fasta_path: Path to a (optionally gzipped) FASTA file.
    Returns:
        Set of sequence IDs present in the file.
    Raises:
        ValueError: If the file contains no sequence headers, a header carries
            no ID, or an ID repeats.
    """
    ids = set()
    with open_by_suffix(fasta_path) as f:
        for line_no, line in enumerate(f, start=1):
            if line.startswith(">"):
                tokens = line[1:].split(maxsplit=1)
                if not tokens:
                    raise ValueError(
                        f"Header with no sequence ID at line {line_no} of {fasta_path}"
                    )
                # Genome DB should not have duplicates.
                if tokens[0] in ids:
                    raise ValueError(
                        f"Duplicate sequence ID {tokens[0]} at line {line_no} of "
                        f"{fasta_path}; one metadata row cannot describe two copies"
                    )
                ids.add(tokens[0])
    if not ids:
        raise ValueError(f"No sequence headers found in {fasta_path}")
    logger.info("Read %d sequence ID(s) from %s", len(ids), fasta_path)
    return ids


def reconcile_duplicate(
    genome_id: str, previous: list[str], row: list[str], schema: MetadataSchema
) -> bool:
    """Reconcile two metadata rows sharing a genome_id.
    Args:
        genome_id: The genome_id both rows carry.
        previous: Row kept for this sequence so far.
        row: Newly encountered row for the same sequence.
        schema: Positions of the metadata columns filtering depends on.
    Returns:
        Whether `row` supersedes `previous` as the row to keep.
    Raises:
        ValueError: If the rows disagree outside ASSEMBLY_FIELDS.
    """
    conflicts = [i for i in schema.idxs_to_compare if previous[i] != row[i]]
    if conflicts:
        raise ValueError(
            f"Metadata rows for {genome_id} disagree on "
            f"{', '.join(schema.column_names[i] for i in conflicts)}: "
            f"{[previous[i] for i in conflicts]} vs "
            f"{[row[i] for i in conflicts]}"
        )
    # Accessions are sorted before chunking and each chunk is concatenated in
    # accession order, so the copy `seqkit rmdup` retains is the first
    # occurrence: the one from the smallest assembly_accession.
    accession = schema.assembly_accession_idx
    return row[accession] < previous[accession]


def filter_rows_to_fasta(
    reader: Iterator[list[str]], schema: MetadataSchema, fasta_ids: set[str]
) -> tuple[dict[str, str], int, int]:
    """Reduce metadata rows to one per sequence present in the FASTA.
    Args:
        reader: Metadata rows, positioned past the header.
        schema: Positions of the metadata columns filtering depends on.
        fasta_ids: Sequence IDs present in the genome FASTA.
    Returns:
        Tuple of (kept rows as encoded TSV lines keyed by genome_id, rows
        dropped as absent, rows dropped as duplicates).
    """
    n_absent = n_duplicate = 0
    # Rows are kept encoded to reduce memory, only parsed when comparing
    # genome_id duplicates. Peak memory scales with number of sequences in
    # FASTA, since metadata rows for absent sequences are skipped.
    kept: dict[str, str] = {}
    for row in reader:
        genome_id = row[schema.genome_id_idx]
        if genome_id not in fasta_ids:
            n_absent += 1
            continue
        previous = kept.get(genome_id)
        if previous is None:
            kept[genome_id] = encode_row(row)
            continue
        if reconcile_duplicate(genome_id, decode_row(previous), row, schema):
            kept[genome_id] = encode_row(row)
        n_duplicate += 1
    return kept, n_absent, n_duplicate


def check_fasta_coverage(fasta_ids: set[str], kept: dict[str, str]) -> None:
    """Check that every sequence in the genome FASTA has a metadata row.
    Args:
        fasta_ids: Sequence IDs present in the genome FASTA.
        kept: Encoded metadata lines, keyed by genome_id.
    Raises:
        ValueError: If any sequence in the FASTA has no metadata row.
    """
    unmatched = sorted(fasta_ids - set(kept))
    if unmatched:
        raise ValueError(
            f"{len(unmatched)} sequence(s) in the genome DB have no metadata row "
            f"(e.g. {', '.join(unmatched[:5])})"
        )


def write_metadata(
    output_path: str, column_names: list[str], rows: Iterable[str]
) -> None:
    """Write a header and pre-encoded metadata lines to a gzipped TSV.
    Args:
        output_path: Output path for the filtered metadata TSV (gzip).
        column_names: Column names to write as the header row.
        rows: Metadata rows, already encoded as TSV lines.
    """
    with open_by_suffix(output_path, "w", newline="") as f_out:
        f_out.write(encode_row(column_names))
        f_out.writelines(rows)


def filter_metadata(
    metadata_path: str, fasta_path: str, output_path: str
) -> tuple[int, int, int]:
    """Write out one metadata row per sequence present in the FASTA.
    Args:
        metadata_path: Path to the genome metadata TSV, with a genome_id column.
        fasta_path: Path to the genome FASTA the metadata should describe.
        output_path: Output path for the filtered metadata TSV (gzip).
    Returns:
        Tuple of (rows written, rows dropped as absent, rows dropped as duplicates).
    Raises:
        ValueError: If the metadata has no header, if any sequence in the FASTA
            has no metadata row, or if rows sharing a genome_id disagree outside
            ASSEMBLY_FIELDS.
    """
    fasta_ids = read_fasta_genome_ids(fasta_path)
    with open_by_suffix(metadata_path, newline="") as f_in:
        reader = csv.reader(f_in, delimiter="\t")
        column_names = next(reader, None)
        if column_names is None:
            raise ValueError(f"Metadata {metadata_path} has no header row")
        kept, n_absent, n_duplicate = filter_rows_to_fasta(
            reader, read_metadata_schema(column_names), fasta_ids
        )
    check_fasta_coverage(fasta_ids, kept)
    write_metadata(output_path, column_names, kept.values())
    n_out = len(kept)
    logger.info(
        "Wrote %d metadata row(s) for %d sequence(s), dropped %d row(s) describing "
        "sequences absent from the genome DB and %d duplicate row(s)",
        n_out,
        len(fasta_ids),
        n_absent,
        n_duplicate,
    )
    return n_out, n_absent, n_duplicate


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metadata", help="Path to genome metadata TSV.")
    parser.add_argument("fasta", help="Path to the genome FASTA to filter against.")
    parser.add_argument("output", help="Output path for filtered metadata TSV (gzip).")
    return parser.parse_args()


def main() -> None:
    start_time = time.time()
    logger.info("Starting filter_metadata_to_fasta.")
    args = parse_arguments()
    filter_metadata(args.metadata, args.fasta, args.output)
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
