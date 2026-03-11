#!/usr/bin/env python
"""Prepare viral genome metadata from NCBI datasets CLI output for downstream
filtering and genome ID extraction.

Reads the merged metadata TSV (from CONCATENATE_TSVS), joins with the virus
taxonomy DB to add species_taxid, matches genome files to assembly accessions,
and outputs a metadata file compatible with filter_viral_genbank_metadata.py.
"""

###########
# IMPORTS #
###########

import argparse
import csv
import gzip
import logging
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(UTCFormatter("[%(asctime)s] %(message)s"))
logger.handlers.clear()
logger.addHandler(handler)

#############
# FUNCTIONS #
#############


@contextmanager
def open_by_suffix(path: str, mode: str = "r"):
    """Open a file, transparently handling .gz compression."""
    if path.endswith(".gz"):
        f = gzip.open(path, mode + "t")
    else:
        f = open(path, mode)
    try:
        yield f
    finally:
        f.close()


def build_species_taxid_map(virus_db_path: str) -> dict[str, str]:
    """Build taxid -> species_taxid mapping from virus taxonomy DB.

    Args:
        virus_db_path: Path to virus taxonomy DB TSV with 'taxid' and 'taxid_species' columns.

    Returns:
        Dictionary mapping taxid to species-level taxid.
    """
    result = {}
    with open(virus_db_path) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            result[row["taxid"]] = row["taxid_species"]
    logger.info("Read %d entries from virus DB", len(result))
    return result


def match_genomes_to_accessions(genome_dir: Path, accessions: list[str]) -> dict[str, str]:
    """Match genome FASTA files to assembly accessions by filename prefix.

    Args:
        genome_dir: Directory containing genome .fna.gz files.
        accessions: List of assembly accessions to match.

    Returns:
        Dictionary mapping assembly accession to genome filename.
    """
    genome_files = sorted(genome_dir.glob("*.fna.gz"))
    result = {}
    for accession in accessions:
        matches = [f for f in genome_files if f.name.startswith(accession)]
        if matches:
            result[accession] = matches[0].name
    return result


def symlink_genomes(accession_to_file: dict[str, str], genome_dir: Path, output_dir: Path) -> None:
    """Symlink matched genome files into the output directory.

    Args:
        accession_to_file: Mapping from accession to genome filename.
        genome_dir: Source directory containing the genome files.
        output_dir: Target directory for symlinks.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename in accession_to_file.values():
        os.symlink(os.path.abspath(genome_dir / filename), output_dir / filename)


def prepare_metadata(
    merged_metadata_path: str, virus_db_path: str, genomes_dir: str,
    output_metadata_path: str, output_genomes_dir: str,
) -> None:
    """Prepare viral genome metadata with species_taxid and local_filename.

    Args:
        merged_metadata_path: Path to merged metadata TSV (may be gzipped).
        virus_db_path: Path to virus taxonomy DB TSV.
        genomes_dir: Directory containing downloaded genome .fna.gz files.
        output_metadata_path: Output path for prepared metadata TSV.
        output_genomes_dir: Output directory for genome files.
    """
    taxid_to_species = build_species_taxid_map(virus_db_path)

    # Read metadata rows, collect unique accessions
    rows = []
    with open_by_suffix(merged_metadata_path) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rows.append(row)
    logger.info("Read %d metadata rows", len(rows))

    # Match genomes and create symlinks
    accessions = list({row["assembly_accession"] for row in rows})
    genome_dir = Path(genomes_dir)
    output_dir = Path(output_genomes_dir)
    accession_to_file = match_genomes_to_accessions(genome_dir, accessions)
    logger.info("Matched %d/%d accessions to genome files", len(accession_to_file), len(accessions))
    symlink_genomes(accession_to_file, genome_dir, output_dir)

    # Write output with added columns, dropping unmatched rows
    out_fields = list(rows[0].keys()) + ["species_taxid", "local_filename"]
    n_written = 0
    with open(output_metadata_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, delimiter="\t")
        writer.writeheader()
        for row in rows:
            if row["assembly_accession"] not in accession_to_file:
                continue
            row["species_taxid"] = taxid_to_species.get(row["taxid"], "")
            filename = accession_to_file[row["assembly_accession"]]
            row["local_filename"] = f"{output_genomes_dir}/{filename}"
            writer.writerow(row)
            n_written += 1
    logger.info("Wrote %d rows to %s (dropped %d unmatched)", n_written, output_metadata_path,
                len(rows) - n_written)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("merged_metadata", help="Path to merged metadata TSV.")
    parser.add_argument("virus_db", help="Path to virus taxonomy DB TSV.")
    parser.add_argument("genomes_dir", help="Directory containing .fna.gz files.")
    parser.add_argument("output_metadata", help="Output path for prepared metadata TSV.")
    parser.add_argument("output_genomes_dir", help="Output directory for genome files.")
    return parser.parse_args()


def main() -> None:
    start_time = time.time()
    logger.info("Starting prepare_viral_metadata.")
    args = parse_arguments()
    prepare_metadata(
        args.merged_metadata, args.virus_db, args.genomes_dir,
        args.output_metadata, args.output_genomes_dir,
    )
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
