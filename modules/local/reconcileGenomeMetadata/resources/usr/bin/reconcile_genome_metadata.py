#!/usr/bin/env python

"""Reduce joined genome metadata to one published row per sequence.

Input is the assembly metadata right-joined onto the per-record sequence
summary on `genome_id`, sorted by `genome_id`. That join has two consequences
this script resolves:

1. A sequence packaged in more than one assembly carries one metadata row per
   assembly, so several adjacent rows share a `genome_id`. Exactly one is
   published, chosen by an explicit policy (see WINNING_ASSEMBLY_POLICY).
2. A sequence with no metadata at all arrives with its metadata columns filled
   with the join's placeholder. That means the genome DB holds a sequence RUN
   cannot resolve to a taxid, so it is fatal.

Because the input is sorted, rows sharing a `genome_id` are adjacent and the
whole reduction streams in constant memory.
"""

###########
# IMPORTS #
###########

import argparse
import csv
import gzip
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

# Which assembly to credit when one sequence is packaged in several. NCBI
# accession.version is immutable, so every copy of a `genome_id` carries the
# same sequence and the choice cannot affect the published FASTA -- only which
# assembly the metadata attributes it to. The choice is therefore arbitrary,
# but it must be deterministic and stated rather than inherited from the order
# records happen to appear in.
WINNING_ASSEMBLY_POLICY = "lowest assembly_accession"

# Columns describing the assembly a sequence was packaged in rather than the
# sequence itself; rows sharing a genome_id may legitimately disagree on these.
# Every other column must agree, and a disagreement means the accession.version
# contract has been violated upstream.
ASSEMBLY_FIELDS = frozenset(
    {"assembly_accession", "source_database", "assembly_status", "release_date"}
)

# JOIN_TSVS fills the absent side of an outer join with this literal. No real
# assembly_accession can take this value.
JOIN_PLACEHOLDER = "NA"

#############
# FUNCTIONS #
#############


def open_by_suffix(path: str, mode: str = "r", newline: str | None = None) -> IO[str]:
    """Open a file, transparently handling .gz compression (text mode)."""
    if path.endswith(".gz"):
        return cast(IO[str], gzip.open(path, mode + "t", newline=newline))
    return cast(IO[str], open(path, mode, newline=newline))


class MetadataSchema(NamedTuple):
    """Positions of the columns the reduction depends on.

    Attributes:
        column_names: Column names from the header row.
        genome_id_idx: Index of the genome_id column.
        assembly_accession_idx: Index of the assembly_accession column.
        idxs_to_compare: Indices of columns rows sharing a genome_id must agree on.
    """

    column_names: list[str]
    genome_id_idx: int
    assembly_accession_idx: int
    idxs_to_compare: list[int]


def read_metadata_schema(column_names: list[str]) -> MetadataSchema:
    """Locate the columns the reduction depends on within a header.

    Args:
        column_names: Column names from the header row.
    Returns:
        Schema giving the index of each column the reduction reads.
    Raises:
        ValueError: If a required column is absent.
    """
    for required in ("genome_id", "assembly_accession"):
        if required not in column_names:
            raise ValueError(f"Joined metadata is missing required column {required!r}")
    return MetadataSchema(
        column_names=column_names,
        genome_id_idx=column_names.index("genome_id"),
        assembly_accession_idx=column_names.index("assembly_accession"),
        idxs_to_compare=[
            i for i, c in enumerate(column_names) if c not in ASSEMBLY_FIELDS
        ],
    )


def check_metadata_present(row: list[str], schema: MetadataSchema) -> None:
    """Fail if a sequence arrived with no metadata row to describe it.

    Args:
        row: A joined row.
        schema: Positions of the columns the reduction reads.
    Raises:
        ValueError: If the metadata side of the join is a placeholder.
    """
    if row[schema.assembly_accession_idx] == JOIN_PLACEHOLDER:
        raise ValueError(
            f"Sequence {row[schema.genome_id_idx]} is in the genome DB but has no "
            "metadata row; RUN could not resolve it to a taxid"
        )


def check_rows_agree(
    genome_id: str, kept: list[str], row: list[str], schema: MetadataSchema
) -> None:
    """Check two rows for the same sequence agree outside assembly fields.

    Args:
        genome_id: The genome_id both rows carry.
        kept: Row retained for this sequence so far.
        row: Another row for the same sequence.
        schema: Positions of the columns the reduction reads.
    Raises:
        ValueError: If the rows disagree outside ASSEMBLY_FIELDS.
    """
    conflicts = [i for i in schema.idxs_to_compare if kept[i] != row[i]]
    if conflicts:
        raise ValueError(
            f"Metadata rows for {genome_id} disagree on "
            f"{', '.join(schema.column_names[i] for i in conflicts)}: "
            f"{[kept[i] for i in conflicts]} vs {[row[i] for i in conflicts]}. "
            "NCBI accession.version is immutable, so copies of one genome_id "
            "should be identical outside assembly-level columns"
        )


def supersedes(kept: list[str], row: list[str], schema: MetadataSchema) -> bool:
    """Whether `row` beats `kept` under WINNING_ASSEMBLY_POLICY."""
    accession = schema.assembly_accession_idx
    return row[accession] < kept[accession]


def reduce_rows(
    reader: Iterator[list[str]], schema: MetadataSchema
) -> Iterator[tuple[list[str], int]]:
    """Reduce adjacent rows sharing a genome_id to the single winning row.

    Args:
        reader: Joined rows, sorted by genome_id, positioned past the header.
        schema: Positions of the columns the reduction reads.
    Yields:
        Tuples of (winning row, number of rows that shared its genome_id).
    Raises:
        ValueError: If the input is not sorted by genome_id, if a sequence has
            no metadata, or if rows disagree outside ASSEMBLY_FIELDS.
    """
    kept: list[str] | None = None
    genome_id = ""
    n_rows = 0
    for row in reader:
        check_metadata_present(row, schema)
        row_id = row[schema.genome_id_idx]
        if kept is None or row_id != genome_id:
            # Adjacency is what makes this streaming reduction correct, so an
            # unsorted input has to fail rather than silently emit duplicates.
            if kept is not None:
                if row_id < genome_id:
                    raise ValueError(
                        f"Joined metadata is not sorted by genome_id: {row_id} "
                        f"follows {genome_id}"
                    )
                yield kept, n_rows
            kept, genome_id, n_rows = row, row_id, 1
            continue
        check_rows_agree(genome_id, kept, row, schema)
        n_rows += 1
        if supersedes(kept, row, schema):
            kept = row
    if kept is not None:
        yield kept, n_rows


def reconcile_metadata(joined_path: str, output_path: str) -> tuple[int, int]:
    """Write one metadata row per sequence in the genome DB.

    Args:
        joined_path: Metadata right-joined onto the sequence summary, sorted by
            genome_id.
        output_path: Output path for the published metadata TSV (gzip).
    Returns:
        Tuple of (rows written, rows dropped as duplicate assemblies).
    Raises:
        ValueError: If the header is missing or malformed, the input is
            unsorted, a sequence has no metadata, or rows disagree.
    """
    n_out = n_dropped = 0
    with (
        open_by_suffix(joined_path, newline="") as f_in,
        open_by_suffix(output_path, "w", newline="") as f_out,
    ):
        reader = csv.reader(f_in, delimiter="\t")
        column_names = next(reader, None)
        if column_names is None:
            raise ValueError(f"Joined metadata {joined_path} has no header row")
        schema = read_metadata_schema(column_names)
        writer = csv.writer(f_out, delimiter="\t", lineterminator="\n")
        writer.writerow(column_names)
        for row, n_rows in reduce_rows(reader, schema):
            writer.writerow(row)
            n_out += 1
            n_dropped += n_rows - 1
    if n_out == 0:
        raise ValueError(f"Joined metadata {joined_path} described no sequences")
    logger.info(
        "Wrote %d metadata row(s), one per sequence, dropping %d row(s) for "
        "sequences packaged in more than one assembly (policy: keep the %s)",
        n_out,
        n_dropped,
        WINNING_ASSEMBLY_POLICY,
    )
    return n_out, n_dropped


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "joined", help="Metadata joined onto the sequence summary, sorted by genome_id."
    )
    parser.add_argument("output", help="Output path for published metadata TSV (gzip).")
    return parser.parse_args()


def main() -> None:
    start_time = time.time()
    logger.info("Starting reconcile_genome_metadata.")
    args = parse_arguments()
    reconcile_metadata(args.joined, args.output)
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
