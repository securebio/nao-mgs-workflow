#!/usr/bin/env python3
DESC = """
Deterministically downsample a TSV to at most N rows, selecting rows by hash of a key column.

Selection keeps the N rows whose key hashes to the smallest value ("bottom-N sketch").
Because the hash is a pure function of the key, this yields:

- A uniform random sample of the input rows, with respect to the key.
- Exactly min(N, n_rows) output rows, without needing to know n_rows in advance.
- Identical output for identical input keys, regardless of input row order, of how
  the input is partitioned across files, or of how many times the pipeline is re-run.
- A sample that is nested in N: raising N only ever adds rows to the selection.

The order-independence is what makes this suitable for a Nextflow pipeline: the selection
is stable under `-resume` and under re-running the workflow, and it does not depend on the
order in which upstream processes happened to emit rows.

The file is read twice. The first pass hashes keys and retains only the (hash, row index)
of the current best N candidates; the second pass copies out the selected rows. This keeps
peak memory proportional to N *keys* rather than N *rows*, which matters because callers
may set N high enough to select everything (as the ONT config does) on inputs whose rows
each carry a full read sequence.

Keys are assumed unique within a file; upstream, CHECK_TSV_DUPLICATES enforces this on
seq_id. Given unique keys and a 128-bit digest, hash ties cannot occur in practice, which
is what makes the selection strictly order-independent rather than merely deterministic.
If duplicate keys are present, rows sharing a key are selected or rejected independently
and the order-independence guarantee no longer holds.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import hashlib
import heapq
import logging
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

    Uses BLAKE2b rather than the built-in hash(), which is randomly salted per
    interpreter process and would therefore give a different sample on every run.
    The digest is deliberately wide enough that collisions between distinct keys are
    unreachable at any plausible input size, so the selection never has to break a tie.

    Args:
        key: The key string to hash.

    Returns:
        An unsigned 128-bit integer derived from the digest of the key.
    """
    digest = hashlib.blake2b(key.encode(), digest_size=16).digest()
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
        if line.strip():
            yield line


def read_key(line: str, key_index: int) -> str:
    """Extract the key column from a TSV line.

    Args:
        line: Full TSV line.
        key_index: 0-based index of the key column.

    Returns:
        The value of the key column.

    Raises:
        ValueError: If the line has too few fields to contain the key column.
    """
    fields = line.rstrip("\n").split("\t")
    if len(fields) <= key_index:
        msg = (
            f"Row does not have enough fields to contain key column "
            f"{key_index}: expected at least {key_index + 1}, got {len(fields)}"
        )
        raise ValueError(msg)
    return fields[key_index]


def select_indices(keys: Iterator[str], n_sample: int) -> tuple[set[int], int]:
    """Find the row indices of the n_sample keys with the smallest hash values.

    Implemented as a bounded max-heap keyed on the negated hash, so the largest
    retained hash is always at the root and can be evicted in O(log N).

    Args:
        keys: Iterator of key values, in row order.
        n_sample: Maximum number of rows to retain. Non-positive values retain nothing.

    Returns:
        A tuple of (set of selected 0-based row indices, total number of rows seen).
    """
    # Heap entries are (-hash, index). Negating the hash makes heapq's min-heap behave
    # as a max-heap, so heap[0] always holds the largest retained hash -- the entry a
    # better candidate should evict. index is carried to identify the row in pass two;
    # with unique keys it is never reached as a comparison tiebreak.
    heap: list[tuple[int, int]] = []
    n_total = 0
    for index, key in enumerate(keys):
        n_total += 1
        if n_sample <= 0:
            continue
        entry = (-hash_key(key), index)
        if len(heap) < n_sample:
            heapq.heappush(heap, entry)
        elif entry[0] > heap[0][0]:
            heapq.heapreplace(heap, entry)
    return {index for _neg_hash, index in heap}, n_total


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


def sample_tsv_by_hash(
    input_path: str, output_path: str, key_column: str, n_sample: int
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

    Raises:
        ValueError: If the input is empty or does not contain key_column.
    """
    # Pass one: hash every key, keeping only the best N (hash, index) pairs
    with open_by_suffix(input_path) as inf:
        header_line, key_index = read_key_index(inf, key_column, input_path)
        keys = (read_key(line, key_index) for line in read_data_lines(inf))
        selected, n_total = select_indices(keys, n_sample)
    # Pass two: copy out the selected rows
    with (
        open_by_suffix(input_path) as inf,
        open_by_suffix(output_path, "w") as outf,
    ):
        inf.readline()  # discard header, already captured above
        outf.write(header_line)
        for index, line in enumerate(read_data_lines(inf)):
            if index in selected:
                outf.write(line if line.endswith("\n") else line + "\n")
    logger.info(
        "Sampled %d of %d rows (target %d) from %s",
        len(selected),
        n_total,
        n_sample,
        input_path,
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
    start_time = time.time()
    sample_tsv_by_hash(args.input, args.output, args.key_column, args.n_sample)
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
