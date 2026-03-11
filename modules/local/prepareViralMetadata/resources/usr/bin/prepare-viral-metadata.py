#!/usr/bin/env python
DESC = """
Prepare viral genome metadata from NCBI datasets CLI output for downstream
filtering and genome ID extraction.

Reads the merged metadata TSV (from CONCATENATE_TSVS), joins with the virus
taxonomy DB to add species_taxid, matches genome files to assembly accessions,
and outputs a metadata file compatible with filter-viral-genbank-metadata.py.
"""

###########
# IMPORTS #
###########

import argparse
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#################
# MAIN FUNCTION #
#################


def match_genomes_to_accessions(
    genome_dir: Path, accessions: list[str],
) -> dict[str, str]:
    """Match genome FASTA files to assembly accessions by filename prefix.

    Args:
        genome_dir: Directory containing genome .fna.gz files.
        accessions: List of assembly accessions to match.

    Returns:
        Dictionary mapping assembly accession to genome filename.
    """
    genome_files = sorted(genome_dir.glob("*.fna.gz"))
    accession_to_file = {}
    for accession in accessions:
        matches = [f for f in genome_files if f.name.startswith(accession)]
        if matches:
            accession_to_file[accession] = matches[0].name
    return accession_to_file


def prepare_metadata(
    merged_metadata_path: str,
    virus_db_path: str,
    genomes_dir: str,
    output_metadata_path: str,
    output_genomes_dir: str,
) -> None:
    """Prepare viral genome metadata with species_taxid and local_filename.

    Args:
        merged_metadata_path: Path to merged metadata TSV (may be gzipped).
        virus_db_path: Path to virus taxonomy DB TSV.
        genomes_dir: Directory containing downloaded genome .fna.gz files.
        output_metadata_path: Output path for prepared metadata TSV.
        output_genomes_dir: Output directory for genome files.
    """
    genomes_path = Path(genomes_dir)
    output_genomes = Path(output_genomes_dir)

    # Read merged metadata from datasets CLI output
    logger.info("Reading merged metadata from %s", merged_metadata_path)
    metadata = pd.read_csv(merged_metadata_path, sep="\t", dtype=str)
    logger.info("Read %d rows with columns: %s", len(metadata), metadata.columns.tolist())

    # Read virus taxonomy DB to get species_taxid mapping
    logger.info("Reading virus taxonomy DB from %s", virus_db_path)
    virus_db = pd.read_csv(virus_db_path, sep="\t", dtype=str)
    logger.info("Virus DB has %d rows with columns: %s", len(virus_db), virus_db.columns.tolist())

    # Map taxid to species-level taxid (taxid_species in virus_db)
    taxid_to_species = dict(zip(virus_db["taxid"], virus_db["taxid_species"], strict=False))
    metadata["species_taxid"] = metadata["taxid"].map(taxid_to_species)
    n_mapped = metadata["species_taxid"].notna().sum()
    logger.info(
        "Mapped %d/%d taxids to species_taxid", n_mapped, len(metadata),
    )

    # Match genome files to assembly accessions
    accessions = metadata["assembly_accession"].unique().tolist()
    accession_to_file = match_genomes_to_accessions(genomes_path, accessions)
    logger.info(
        "Matched %d/%d accessions to genome files",
        len(accession_to_file),
        len(accessions),
    )

    # Create output genomes directory and symlink files
    output_genomes.mkdir(parents=True, exist_ok=True)
    for filename in accession_to_file.values():
        src = genomes_path / filename
        dst = output_genomes / filename
        os.symlink(os.path.abspath(src), dst)

    # Add local_filename column (path relative to process working directory)
    metadata["local_filename"] = metadata["assembly_accession"].map(
        lambda acc: f"{output_genomes_dir}/{accession_to_file[acc]}"
        if acc in accession_to_file
        else None,
    )

    # Drop rows without matching genome files
    n_before = len(metadata)
    metadata = metadata.dropna(subset=["local_filename"])
    n_dropped = n_before - len(metadata)
    if n_dropped > 0:
        logger.info(
            "Dropped %d rows without matching genome files", n_dropped,
        )

    # Write output metadata
    logger.info("Writing %d rows to %s", len(metadata), output_metadata_path)
    metadata.to_csv(output_metadata_path, sep="\t", index=False)
    logger.info("Done.")


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument(
        "merged_metadata", help="Path to merged metadata TSV (may be gzipped).",
    )
    parser.add_argument(
        "virus_db", help="Path to virus taxonomy DB TSV.",
    )
    parser.add_argument(
        "genomes_dir", help="Directory containing downloaded genome .fna.gz files.",
    )
    parser.add_argument(
        "output_metadata", help="Output path for prepared metadata TSV.",
    )
    parser.add_argument(
        "output_genomes_dir", help="Output directory for genome files.",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for prepare-viral-metadata script."""
    start_time = time.time()
    logger.info("Starting prepare-viral-metadata.")
    args = parse_arguments()
    prepare_metadata(
        args.merged_metadata,
        args.virus_db,
        args.genomes_dir,
        args.output_metadata,
        args.output_genomes_dir,
    )
    elapsed = time.time() - start_time
    logger.info("Total time elapsed: %.2f seconds", elapsed)


if __name__ == "__main__":
    main()
