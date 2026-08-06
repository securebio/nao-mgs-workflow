#!/usr/bin/env python3
DESC = """
Annotate a viral hits TSV with post-hoc validation results, read by read.

The DOWNSTREAM workflow validates a downsampled subset of putative viral reads by
aligning them against a large reference database. This script joins those results back
onto the full hits table and records, explicitly and per read, which of three states the
read is in:

- "aligned":       the read was selected for validation and the aligner returned at
                   least one hit passing the score filters. Validation columns are
                   populated.
- "no_alignment":  the read was selected for validation but no hit survived filtering.
                   Validation columns are NA.
- "not_sampled":   the read was not selected for validation. Validation columns are NA.

No result is ever extrapolated from one read to another. A read's validation columns
describe that read's own alignment or nothing at all, which is what distinguishes this
from the cluster-representative propagation it replaces: a populated validation column
here is always first-hand evidence about the read in whose row it appears.

Memory use is proportional to the number of validated reads (the downsampled subset),
not to the number of hits, so the full hits table is streamed.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import logging
import time
from collections import Counter
from datetime import UTC, datetime
from typing import IO, cast

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

#############
# CONSTANTS #
#############

STATUS_ALIGNED = "aligned"
STATUS_NO_ALIGNMENT = "no_alignment"
STATUS_NOT_SAMPLED = "not_sampled"
MISSING = "NA"

####################
# HELPER FUNCTIONS #
####################


def open_by_suffix(filename: str, mode: str = "r") -> IO[str]:
    """Open a file, transparently handling gzip compression by suffix.

    Args:
        filename: Path to the file to open.
        mode: Mode to open the file in ("r" or "w").

    Returns:
        An open text-mode file object.
    """
    if filename.endswith(".gz"):
        return cast(IO[str], gzip.open(filename, mode + "t"))
    return open(filename, mode)


def read_header(input_file: IO[str], path: str) -> list[str]:
    """Read and split the header line of a TSV.

    Args:
        input_file: Open file object positioned at the start of the file.
        path: Path of the file, used for error messages.

    Returns:
        List of column names.

    Raises:
        ValueError: If the file is empty, or if a column name is repeated. A repeated
            name makes every lookup by that name ambiguous, and would pass through to a
            duplicate column in the output, so it is rejected here for all three inputs
            rather than at each use site.
    """
    line = input_file.readline()
    if not line:
        msg = f"Input file is empty (no header line): {path}"
        raise ValueError(msg)
    header = line.rstrip("\n").split("\t")
    duplicates = sorted(name for name, n in Counter(header).items() if n > 1)
    if duplicates:
        msg = f"Duplicate column name(s) in header of {path}: {duplicates}"
        raise ValueError(msg)
    return header


def read_key_set(path: str, key_column: str) -> set[str]:
    """Read the set of values in one column of a TSV.

    Args:
        path: Path to the TSV (optionally gzipped).
        key_column: Name of the column to collect.

    Returns:
        Set of values found in that column.

    Raises:
        ValueError: If the file is empty, lacks key_column, or contains a row whose field
            count does not match the header. A short row was previously skipped, which
            silently dropped that read from the set and so mislabelled it downstream as
            never having been sampled; the other two readers reject ragged rows, and this
            one now matches them.
    """
    with open_by_suffix(path) as inf:
        header = read_header(inf, path)
        if key_column not in header:
            msg = f"Column {key_column!r} not found in header of {path}. Available columns: {', '.join(header)}"
            raise ValueError(msg)
        index = header.index(key_column)
        keys = set()
        for line in inf:
            if not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != len(header):
                msg = f"Row in {path} has {len(fields)} fields, expected {len(header)}"
                raise ValueError(msg)
            keys.add(fields[index])
    return keys


def read_validation_table(
    path: str, key_column: str
) -> tuple[list[str], dict[str, list[str]]]:
    """Read a validation results TSV into a lookup keyed on key_column.

    Args:
        path: Path to the validation TSV (optionally gzipped).
        key_column: Name of the join key column (e.g. "seq_id").

    Returns:
        A tuple of (value column names in file order, mapping from key to value fields).
        The key column itself is excluded from both.

    Raises:
        ValueError: If the file is empty, lacks key_column, or contains a duplicate key.
    """
    with open_by_suffix(path) as inf:
        header = read_header(inf, path)
        if key_column not in header:
            msg = f"Column {key_column!r} not found in header of {path}. Available columns: {', '.join(header)}"
            raise ValueError(msg)
        key_index = header.index(key_column)
        value_columns = [c for i, c in enumerate(header) if i != key_index]
        table: dict[str, list[str]] = {}
        for line in inf:
            if not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != len(header):
                msg = f"Row in {path} has {len(fields)} fields, expected {len(header)}"
                raise ValueError(msg)
            key = fields[key_index]
            if key in table:
                msg = f"Duplicate key {key!r} in validation table {path}"
                raise ValueError(msg)
            table[key] = [f for i, f in enumerate(fields) if i != key_index]
    return value_columns, table


def annotate_validation_status(
    hits_path: str,
    validation_path: str,
    sampled_path: str,
    output_path: str,
    key_column: str,
    status_column: str,
) -> Counter[str]:
    """Join validation results onto a hits TSV and record per-read validation status.

    Args:
        hits_path: Path to the full viral hits TSV for a group.
        validation_path: Path to validation results for reads that produced alignments.
        sampled_path: Path to a TSV listing every read selected for validation.
        output_path: Path to write the annotated hits TSV.
        key_column: Column to join on (e.g. "seq_id").
        status_column: Name of the status column to append.

    Returns:
        Counter of how many hits fell into each status.

    Raises:
        ValueError: If any input is empty or missing key_column; if the validation table
            contains a duplicate key; if status_column or a validation column would
            collide with an existing column, which would emit a duplicate column name;
            or if the three inputs disagree about which reads exist -- either a validated
            read absent from the sampled set, or a sampled read absent from the hits
            table. Each disagreement means the inputs describe different read sets, so
            the status column could not be trusted.
    """
    value_columns, validation = read_validation_table(validation_path, key_column)
    sampled = read_key_set(sampled_path, key_column)
    orphans = set(validation) - sampled
    if orphans:
        example = sorted(orphans)[:3]
        msg = (
            f"{len(orphans)} read(s) in the validation table are absent from the "
            f"sampled-read list, e.g. {example}. The validation results and the "
            f"downsampled read set are inconsistent."
        )
        raise ValueError(msg)
    blank = [MISSING] * len(value_columns)
    counts: Counter[str] = Counter()
    with (
        open_by_suffix(hits_path) as inf,
        open_by_suffix(output_path, "w") as outf,
    ):
        header = read_header(inf, hits_path)
        if key_column not in header:
            msg = f"Column {key_column!r} not found in header of {hits_path}. Available columns: {', '.join(header)}"
            raise ValueError(msg)
        key_index = header.index(key_column)
        overlap = set(header) & set(value_columns)
        if overlap:
            msg = f"Validation columns collide with hits columns: {sorted(overlap)}"
            raise ValueError(msg)
        if status_column in set(header) | set(value_columns):
            msg = (
                f"Status column {status_column!r} is already present in the input "
                f"columns, which would emit two columns of that name"
            )
            raise ValueError(msg)
        outf.write("\t".join([*header, *value_columns, status_column]) + "\n")
        # Track which sampled reads are actually seen. A sampled read missing from the
        # hits table means the table being annotated is not the one that was sampled, and
        # would otherwise be dropped silently -- no row, no status, no error.
        unseen_sampled = set(sampled)
        for line in inf:
            if not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != len(header):
                msg = f"Row in {hits_path} has {len(fields)} fields, expected {len(header)}"
                raise ValueError(msg)
            key = fields[key_index]
            unseen_sampled.discard(key)
            values = validation.get(key)
            if values is not None:
                status = STATUS_ALIGNED
            else:
                values = blank
                status = STATUS_NO_ALIGNMENT if key in sampled else STATUS_NOT_SAMPLED
            counts[status] += 1
            outf.write("\t".join([*fields, *values, status]) + "\n")
    if unseen_sampled:
        example = sorted(unseen_sampled)[:3]
        msg = (
            f"{len(unseen_sampled)} sampled read(s) are absent from the hits table, "
            f"e.g. {example}. The hits table and the downsampled read set are "
            f"inconsistent."
        )
        raise ValueError(msg)
    return counts


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument(
        "--hits", "-i", required=True, help="Path to the full viral hits TSV."
    )
    parser.add_argument(
        "--validation",
        "-v",
        required=True,
        help="Path to validation results for reads that produced alignments.",
    )
    parser.add_argument(
        "--sampled",
        "-s",
        required=True,
        help="Path to a TSV listing every read selected for validation.",
    )
    parser.add_argument(
        "--output", "-o", required=True, help="Path to output annotated hits TSV."
    )
    parser.add_argument(
        "--key-column", "-k", default="seq_id", help="Column to join the tables on."
    )
    parser.add_argument(
        "--status-column",
        "-c",
        default="validation_status",
        help="Name of the status column to append.",
    )
    return parser.parse_args()


########
# MAIN #
########


def main() -> None:
    """Entry point: annotate the hits TSV and report per-status counts."""
    args = parse_arguments()
    logger.info("Initializing validation status annotation.")
    logger.info("Hits file: %s", args.hits)
    logger.info("Validation file: %s", args.validation)
    logger.info("Sampled-read file: %s", args.sampled)
    logger.info("Output file: %s", args.output)
    start_time = time.time()
    counts = annotate_validation_status(
        args.hits,
        args.validation,
        args.sampled,
        args.output,
        args.key_column,
        args.status_column,
    )
    total = sum(counts.values())
    for status in (STATUS_ALIGNED, STATUS_NO_ALIGNMENT, STATUS_NOT_SAMPLED):
        n = counts[status]
        pct = 100 * n / total if total else 0.0
        logger.info("%s: %d hits (%.2f%%)", status, n, pct)
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
