#!/usr/bin/env python
"""Add a `genome_id` column to a Genbank metadata TSV by parsing FASTA headers
from each row's `local_filename` genome file. Expands the metadata table so
each output row corresponds to one (assembly_accession, genome_id) pair.

Genome files are pre-fetched in parallel into a local staging directory before
reading, since serial `gzip.open()` over a Fusion-mounted source is bottlenecked
on per-file S3 GET latency × N files.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import logging
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from Bio.SeqIO.FastaIO import SimpleFastaParser

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
# FUNCTIONS #
#############


def stage_genomes_parallel(
    filepaths: list[str], staged_dir: Path, parallelism: int
) -> None:
    """Parallel-fetch each path in `filepaths` into `staged_dir/<basename>`.
    Uses `xargs -P` (driven by `cp -t`) so up to `parallelism` fetches run
    concurrently; basenames are assumed unique (one assembly per accession).
    Args:
        filepaths: Source paths to fetch (e.g. Fusion-mounted .fna.gz files).
        staged_dir: Destination directory, created if missing.
        parallelism: Max concurrent `cp` invocations (`xargs -P`).
    """
    staged_dir.mkdir(parents=True, exist_ok=True)
    paths_txt = staged_dir.parent / "paths.txt"
    paths_txt.write_text("\n".join(filepaths) + "\n")
    logger.info(
        "Staging %d genome files into %s with -P %d",
        len(filepaths),
        staged_dir,
        parallelism,
    )
    subprocess.run(
        [
            "xargs",
            "-P",
            str(parallelism),
            "-n",
            "100",
            "-a",
            str(paths_txt),
            "cp",
            "-t",
            str(staged_dir),
        ],
        check=True,
    )


def extract_genome_ids(filepaths: list[str], staged_dir: Path) -> list[list[str]]:
    """Parse each `staged_dir/<basename(filepath)>` FASTA file and collect the
    genome IDs from sequence headers.
    Args:
        filepaths: Original filepaths (only basename is used to locate staged copy).
        staged_dir: Directory containing pre-fetched .fna.gz files.
    Returns:
        List of genome-ID lists, one inner list per input filepath, preserving order.
    """
    gid_lists: list[list[str]] = []
    for path in filepaths:
        gid_list: list[str] = []
        with gzip.open(staged_dir / os.path.basename(path), "rt") as inf:
            for title, _sequence in SimpleFastaParser(inf):
                genome_id, _name = title.split(" ", 1)
                gid_list.append(genome_id)
        gid_lists.append(gid_list)
    return gid_lists


def add_genome_ids(metadata_path: str, output_path: str, parallelism: int) -> None:
    """Read metadata, stage genomes locally, parse FASTA headers, and write
    expanded metadata with a `genome_id` column.
    Args:
        metadata_path: Input TSV with at minimum a `local_filename` column.
        output_path: Output TSV (gzip-compressed) with the added `genome_id` column.
        parallelism: Parallel `cp` workers for staging.
    """
    meta_db = pd.read_csv(metadata_path, sep="\t", dtype=str)
    filepaths = list(meta_db["local_filename"])
    staged_dir = Path("staged")
    stage_genomes_parallel(filepaths, staged_dir, parallelism)
    gid_lists = extract_genome_ids(filepaths, staged_dir)
    expanded = [(idx, value) for idx, vals in enumerate(gid_lists) for value in vals]
    indices, values = zip(*expanded, strict=True)
    meta_db_gid = meta_db.iloc[list(indices)].copy()
    meta_db_gid["genome_id"] = list(values)
    meta_db_gid.to_csv(output_path, sep="\t", index=False)
    logger.info("Wrote %d rows to %s", len(meta_db_gid), output_path)


########
# MAIN #
########


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "metadata",
        help="Input Genbank metadata TSV (must contain `local_filename` column).",
    )
    parser.add_argument(
        "output", help="Output gzipped TSV path (with added `genome_id` column)."
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="Number of concurrent cp workers for staging (default: 1).",
    )
    return parser.parse_args()


def main() -> None:
    start_time = time.time()
    logger.info("Starting add_genbank_genome_ids.")
    args = parse_arguments()
    add_genome_ids(args.metadata, args.output, args.parallelism)
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
