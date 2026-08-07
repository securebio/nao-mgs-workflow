#!/usr/bin/env python
"""Filter genome metadata down to one row per sequence in a genome FASTA.
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


# Fields a duplicate genome_id must agree on: RUN resolves a sequence to a
# taxid through this file, so rows that disagree here are not interchangeable.
TAXONOMY_FIELDS = ("taxid", "species_taxid")
# Columns the reconciliation reads. Absent any of them it cannot do its job, so
# a schema change upstream should fail here rather than silently weaken a guard.
REQUIRED_FIELDS = ("genome_id", "assembly_accession", *TAXONOMY_FIELDS)


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
) -> tuple[int, int, int]:
    """Write out one metadata row per sequence present in the FASTA.
    Rows whose genome_id is absent from the FASTA are dropped. Where several
    rows share a genome_id — the same sequence packaged in more than one
    assembly — the row kept is the one naming the lexicographically smallest
    assembly_accession, which is the copy the FASTA carries: accessions are
    sorted before chunking and each chunk is concatenated in accession order,
    so the first occurrence `seqkit rmdup` retains is the smallest. Duplicates
    must agree on TAXONOMY_FIELDS, since otherwise the sequence's taxid would
    depend on which packaging of it happened to survive deduplication.
    Args:
        metadata_path: Path to the genome metadata TSV, with REQUIRED_FIELDS.
        fasta_path: Path to the genome FASTA the metadata should describe.
        output_path: Output path for the filtered metadata TSV (gzip).
    Returns:
        Tuple of (rows written, rows dropped as absent, rows dropped as duplicates).
    Raises:
        ValueError: If the metadata lacks any of REQUIRED_FIELDS, if any
            sequence in the FASTA has no metadata row, or if rows sharing a
            genome_id disagree on TAXONOMY_FIELDS.
    """
    fasta_ids = read_fasta_genome_ids(fasta_path)
    n_absent = n_duplicate = 0
    # Keyed by genome_id, in order of first appearance: metadata row order is
    # preserved even when a later row supersedes an earlier one.
    kept: dict[str, dict[str, str]] = {}
    with open_by_suffix(metadata_path, newline="") as f_in:
        reader = csv.DictReader(f_in, delimiter="\t")
        if reader.fieldnames is None:
            raise ValueError(f"Metadata {metadata_path} has no header row")
        fieldnames = reader.fieldnames
        missing = [f for f in REQUIRED_FIELDS if f not in fieldnames]
        if missing:
            raise ValueError(
                f"Metadata {metadata_path} lacks required column(s): "
                f"{', '.join(missing)}"
            )
        for row in reader:
            genome_id = row["genome_id"]
            if genome_id not in fasta_ids:
                n_absent += 1
                continue
            previous = kept.get(genome_id)
            if previous is None:
                kept[genome_id] = row
                continue
            conflicts = [f for f in TAXONOMY_FIELDS if previous[f] != row[f]]
            if conflicts:
                raise ValueError(
                    f"Metadata rows for {genome_id} disagree on "
                    f"{', '.join(conflicts)}: "
                    f"{[previous[f] for f in conflicts]} vs "
                    f"{[row[f] for f in conflicts]}; the genome DB carries one "
                    "sequence under this ID, so there is no basis for choosing a taxid"
                )
            n_duplicate += 1
            if row["assembly_accession"] < previous["assembly_accession"]:
                kept[genome_id] = row
    unmatched = sorted(fasta_ids - set(kept))
    if unmatched:
        raise ValueError(
            f"{len(unmatched)} sequence(s) in the genome DB have no metadata row "
            f"(e.g. {', '.join(unmatched[:5])}); RUN could not resolve them to a taxid"
        )
    with open_by_suffix(output_path, "w", newline="") as f_out:
        writer = csv.DictWriter(
            f_out, fieldnames=fieldnames, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(kept.values())
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
