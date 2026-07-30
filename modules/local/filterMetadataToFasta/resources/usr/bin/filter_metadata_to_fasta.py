#!/usr/bin/env python
"""Filter genome metadata down to the sequences present in a genome FASTA.
Raises if the FASTA carries a genome ID with no metadata row.
"""

import argparse
import csv
import gzip
import logging
import time
from datetime import UTC, datetime
from typing import IO, cast


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


def read_fasta_genome_ids(fasta_path: str) -> set[str]:
    """Collect sequence IDs from a FASTA file's headers.
    The ID is the first whitespace-delimited token after '>', matching how
    aligners name references and how genome_ids are derived at download time.
    Args:
        fasta_path: Path to a (optionally gzipped) FASTA file.
    Returns:
        Set of sequence IDs present in the file.
    Raises:
        ValueError: If the file contains no sequence headers, or a header
            carries no ID.
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
                ids.add(tokens[0])
    if not ids:
        raise ValueError(f"No sequence headers found in {fasta_path}")
    logger.info("Read %d sequence ID(s) from %s", len(ids), fasta_path)
    return ids


def filter_metadata(
    metadata_path: str, fasta_path: str, output_path: str
) -> tuple[int, int]:
    """Write out metadata rows whose genome_id is present in the FASTA.
    Args:
        metadata_path: Path to the genome metadata TSV, with a `genome_id` column.
        fasta_path: Path to the genome FASTA the metadata should describe.
        output_path: Output path for the filtered metadata TSV (gzip).
    Returns:
        Tuple of (rows written, rows dropped).
    Raises:
        ValueError: If the metadata lacks a `genome_id` column, or if any
            sequence in the FASTA has no metadata row.
    """
    fasta_ids = read_fasta_genome_ids(fasta_path)
    n_out = n_dropped = 0
    matched: set[str] = set()
    with (
        open_by_suffix(metadata_path, newline="") as f_in,
        open_by_suffix(output_path, "w", newline="") as f_out,
    ):
        reader = csv.DictReader(f_in, delimiter="\t")
        if reader.fieldnames is None or "genome_id" not in reader.fieldnames:
            raise ValueError(f"Metadata {metadata_path} lacks a genome_id column")
        writer = csv.DictWriter(
            f_out, fieldnames=reader.fieldnames, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for row in reader:
            if row["genome_id"] in fasta_ids:
                writer.writerow(row)
                matched.add(row["genome_id"])
                n_out += 1
            else:
                n_dropped += 1
    unmatched = sorted(fasta_ids - matched)
    if unmatched:
        raise ValueError(
            f"{len(unmatched)} sequence(s) in the genome DB have no metadata row "
            f"(e.g. {', '.join(unmatched[:5])}); RUN could not resolve them to a taxid"
        )
    logger.info(
        "Wrote %d metadata row(s) for %d sequence(s), dropped %d row(s) "
        "describing sequences absent from the genome DB",
        n_out,
        len(fasta_ids),
        n_dropped,
    )
    return n_out, n_dropped


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
