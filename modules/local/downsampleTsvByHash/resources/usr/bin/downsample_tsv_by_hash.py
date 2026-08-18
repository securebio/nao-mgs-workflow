#!/usr/bin/env python3
DESC = """
Deterministically downsample a TSV to at most N rows, selecting the rows whose key
column hashes to the smallest values ("bottom-N sketch"). Because the hash is a pure
function of the key, the sample is uniform with respect to the key, has exactly
min(N, n_rows) rows, is identical across re-runs and input orderings (so stable under
Nextflow `-resume`), and is nested in N: raising N only ever adds rows.

Both the guarantee and the cap are per file: the same keys divided differently across
files give a different overall selection, so a group that must be sampled as a unit has
to arrive as a single file. Keys are assumed unique within a file (upstream,
CHECK_TSV_DUPLICATES enforces this on seq_id); with duplicate keys the selection remains
deterministic but not order-independent.

The file is read twice -- pass one keeps only the (hash, row index) of the best N
candidates, pass two copies out the selected rows -- so peak memory scales with N keys
rather than N rows. When every row is retained (n_sample >= n_rows, as on the long-read
path), the input is copied verbatim instead of being decompressed and recompressed.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import hashlib
import heapq
import logging
import shutil
import time
from collections.abc import Iterator
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


def hash_key(key: str) -> int:
    """Map a key string to a pseudorandom integer in [0, 2**128).

    Uses BLAKE2s rather than the built-in hash(), which is salted per interpreter
    process. The 128-bit digest keeps collisions between distinct keys unreachable
    (a 32-bit hash would collide by ~77,000 keys), so ties never need breaking.

    Args:
        key: The key string to hash.

    Returns:
        An unsigned 128-bit integer derived from the digest of the key.
    """
    digest = hashlib.blake2s(key.encode(), digest_size=16).digest()
    return int.from_bytes(digest, "big")


def read_data_lines(input_file: IO[str]) -> Iterator[str]:
    """Yield non-blank lines from a headerless TSV body.

    Both passes enumerate rows through this function, so they agree on row indices.

    Args:
        input_file: Open file object positioned after the header line.

    Yields:
        Full lines, including the trailing newline.
    """
    for line in input_file:
        if not line.isspace():
            yield line


def read_key(line: str, key_index: int) -> str:
    """Extract the key column from a TSV line, splitting only as far as needed.

    Args:
        line: Full TSV line.
        key_index: 0-based index of the key column.

    Returns:
        The value of the key column.

    Raises:
        ValueError: If the line has too few fields to contain the key column.
    """
    fields = line.split("\t", key_index + 1)
    if len(fields) <= key_index:
        msg = (
            f"Row does not have enough fields to contain column "
            f"{key_index}: expected at least {key_index + 1}, got {len(fields)}"
        )
        raise ValueError(msg)
    # Only the last field on a line can carry the newline, so strip just the key
    return fields[key_index].rstrip("\n")


def select_indices(keys: Iterator[str], n_sample: int) -> tuple[set[int] | None, int]:
    """Find the row indices of the n_sample keys with the smallest hash values.

    Args:
        keys: Iterator of key values, in row order.
        n_sample: Maximum number of rows to retain. Must be non-negative.

    Returns:
        A tuple of (selected 0-based row indices, total number of rows seen). The first
        element is None when every row was retained, which lets the caller skip
        rewriting the file altogether.

    Raises:
        ValueError: If n_sample is negative.
    """
    if n_sample < 0:
        msg = f"n_sample must be non-negative, got {n_sample}"
        raise ValueError(msg)
    if n_sample == 0:
        return set(), sum(1 for _ in keys)
    # The heap holds (-hash, index) so heapq's min-heap behaves as a max-heap: heap[0]
    # is the largest retained hash, i.e. the entry a better candidate should evict.
    heap: list[tuple[int, int]] = []
    n_total = 0
    for index, key in enumerate(keys):
        n_total += 1
        entry = (-hash_key(key), index)
        if len(heap) < n_sample:
            heapq.heappush(heap, entry)
        elif entry[0] > heap[0][0]:
            heapq.heapreplace(heap, entry)
    if n_total <= n_sample:
        return None, n_total
    return {index for _neg_hash, index in heap}, n_total


def read_column_index(header_fields: list[str], column: str, path: str) -> int:
    """Locate a named column within already-parsed header fields.

    Args:
        header_fields: Header line split on tabs.
        column: Name of the column to locate.
        path: Path of the file, used for error messages.

    Returns:
        0-based index of the column.

    Raises:
        ValueError: If the column is absent.
    """
    if column not in header_fields:
        msg = (
            f"Column {column!r} not found in header of {path}. "
            f"Available columns: {', '.join(header_fields)}"
        )
        raise ValueError(msg)
    return header_fields.index(column)


def resolve_match_columns(
    header_line: str, match_columns: tuple[str, str] | None, path: str
) -> tuple[int, int] | None:
    """Resolve the pair of columns whose equality makes a row eligible.

    Args:
        header_line: The file's header line.
        match_columns: Pair of column names, or None to consider every row eligible.
        path: Path of the file, used for error messages.

    Returns:
        The pair of 0-based column indices, or None when no restriction applies.

    Raises:
        ValueError: If either column is absent.
    """
    if match_columns is None:
        return None
    header_fields = header_line.rstrip("\n").split("\t")
    return (
        read_column_index(header_fields, match_columns[0], path),
        read_column_index(header_fields, match_columns[1], path),
    )


def is_eligible(line: str, match_indices: tuple[int, int] | None) -> bool:
    """Report whether a row may be selected.

    Args:
        line: Full TSV line.
        match_indices: Pair of column indices that must hold equal values, or None to
            accept every row.

    Returns:
        True if the row is eligible for selection.
    """
    if match_indices is None:
        return True
    return read_key(line, match_indices[0]) == read_key(line, match_indices[1])


def read_key_index(input_file: IO[str], key_column: str, path: str) -> tuple[str, int]:
    """Read a TSV header and locate the key column within it.

    Args:
        input_file: Open file object positioned at the start of the file.
        key_column: Name of the column to locate.
        path: Path of the file, used for error messages.

    Returns:
        A tuple of (header line, 0-based index of the key column).

    Raises:
        ValueError: If the file is empty or does not contain key_column.
    """
    header_line = input_file.readline()
    if not header_line:
        msg = f"Input file is empty (no header line): {path}"
        raise ValueError(msg)
    header_fields = header_line.rstrip("\n").split("\t")
    if key_column not in header_fields:
        msg = (
            f"Key column {key_column!r} not found in header of {path}. "
            f"Available columns: {', '.join(header_fields)}"
        )
        raise ValueError(msg)
    if not header_line.endswith("\n"):
        header_line += "\n"
    return header_line, header_fields.index(key_column)


def same_compression(input_path: str, output_path: str) -> bool:
    """Report whether two paths imply the same on-disk compression.

    Args:
        input_path: Path to the input file.
        output_path: Path to the output file.

    Returns:
        True if both are gzipped or both are plaintext, judged by suffix.
    """
    return input_path.endswith(".gz") == output_path.endswith(".gz")


def downsample_tsv_by_hash(
    input_path: str,
    output_path: str,
    key_column: str,
    n_sample: int,
    match_columns: tuple[str, str] | None = None,
) -> None:
    """Downsample a TSV to at most n_sample rows by hash of key_column.

    The header line is always written, so an input containing only a header yields an
    output containing only a header. Selected rows are written in input order, which
    keeps any upstream sort intact.

    Args:
        input_path: Path to the input TSV (optionally gzipped).
        output_path: Path to the output TSV (gzipped if the path ends in .gz).
        key_column: Name of the column to hash for selection.
        n_sample: Maximum number of data rows to retain.
        match_columns: Optional pair of column names. When given, only rows whose values
            in those two columns are equal are eligible for selection; every other row is
            dropped. The cap therefore applies to the eligible rows, not to the input.

    Raises:
        ValueError: If the input is empty or does not contain one of the named columns.
    """
    # Pass one: hash keys, keeping only the best N (hash, index) pairs. Indices count
    # eligible rows only, and pass two applies the same predicate, so the two agree.
    n_ineligible = 0
    with open_by_suffix(input_path) as inf:
        header_line, key_index = read_key_index(inf, key_column, input_path)
        match_indices = resolve_match_columns(header_line, match_columns, input_path)

        def eligible_keys() -> Iterator[str]:
            nonlocal n_ineligible
            for line in read_data_lines(inf):
                if is_eligible(line, match_indices):
                    yield read_key(line, key_index)
                else:
                    n_ineligible += 1

        selected, n_total = select_indices(eligible_keys(), n_sample)
    if (
        selected is None
        and n_ineligible == 0
        and same_compression(input_path, output_path)
    ):
        # Every row is retained, so the output would be a byte-for-byte reproduction of
        # the input. Copy it rather than paying to decompress and recompress it.
        shutil.copyfile(input_path, output_path)
        logger.info(
            "Retained all %d rows (target %d) from %s; copied input verbatim",
            n_total,
            n_sample,
            input_path,
        )
        return
    # Pass two: copy out the selected rows
    with (
        open_by_suffix(input_path) as inf,
        open_by_suffix(output_path, "w") as outf,
    ):
        inf.readline()  # discard header, already captured above
        outf.write(header_line)
        n_written = 0
        index = 0
        for line in read_data_lines(inf):
            if not is_eligible(line, match_indices):
                continue
            if selected is None or index in selected:
                outf.write(line if line.endswith("\n") else line + "\n")
                n_written += 1
            index += 1
    logger.info(
        "Retained %d of %d eligible rows (target %d) from %s; %d rows were ineligible",
        n_written,
        n_total,
        n_sample,
        input_path,
        n_ineligible,
    )


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument("--input", "-i", required=True, help="Path to input TSV.")
    parser.add_argument("--output", "-o", required=True, help="Path to output TSV.")
    parser.add_argument(
        "--key-column",
        "-k",
        default="seq_id",
        help="Name of the column to hash when selecting rows.",
    )
    parser.add_argument(
        "--match-columns",
        "-m",
        default=None,
        help=(
            "Optional pair of comma-separated column names. Only rows whose values in "
            "these two columns are equal are eligible for selection; all other rows are "
            "dropped. Used to restrict sampling to duplicate-group exemplars."
        ),
    )
    parser.add_argument(
        "--n-sample",
        "-n",
        type=int,
        required=True,
        help="Maximum number of rows to retain.",
    )
    return parser.parse_args()


########
# MAIN #
########


def main() -> None:
    """Entry point: downsample the input TSV and report timing."""
    args = parse_arguments()
    logger.info("Initializing TSV hash downsampling.")
    logger.info("Input file: %s", args.input)
    logger.info("Output file: %s", args.output)
    logger.info("Key column: %s", args.key_column)
    logger.info("Sample size: %d", args.n_sample)
    match_columns = None
    if args.match_columns:
        parts = [c.strip() for c in args.match_columns.split(",")]
        if len(parts) != 2:
            msg = f"--match-columns needs exactly two column names, got {args.match_columns!r}"
            raise ValueError(msg)
        match_columns = (parts[0], parts[1])
        logger.info("Restricting selection to rows where %s == %s", *match_columns)
    start_time = time.time()
    downsample_tsv_by_hash(
        args.input, args.output, args.key_column, args.n_sample, match_columns
    )
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
