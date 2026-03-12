#!/usr/bin/env python
"""Prepare viral genome metadata from NCBI datasets CLI output for downstream
filtering and genome ID extraction. Reads merged metadata TSV, joins with the
virus taxonomy DB to add species_taxid, matches genome files to accessions, and
outputs metadata compatible with filter_viral_genbank_metadata.py.
"""

import argparse
import csv
import gzip
import logging
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path

class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(UTCFormatter("[%(asctime)s] %(message)s"))
logger.handlers.clear()
logger.addHandler(handler)

def open_by_suffix(path: str, mode: str = "r"):
    """Open a file, transparently handling .gz compression."""
    return gzip.open(path, mode + "t") if path.endswith(".gz") else open(path, mode)

def build_species_taxid_map(virus_db_path: str) -> dict[str, str]:
    """Build taxid -> species_taxid mapping from virus taxonomy DB.
    Args:
        virus_db_path: Path to TSV with 'taxid' and 'taxid_species' columns.
    Returns:
        Dictionary mapping taxid to species-level taxid.
    """
    with open(virus_db_path) as f:
        result = {row["taxid"]: row["taxid_species"] for row in csv.DictReader(f, delimiter="\t")}
    logger.info("Read %d entries from virus DB", len(result))
    return result

ACCESSION_RE = re.compile(r"^(GC[AF]_\d+\.\d+)")

def match_genomes_to_accessions(genome_dir: Path, accessions: list[str]) -> dict[str, str]:
    """Match genome .fna.gz files to assembly accessions by filename prefix.
    Args:
        genome_dir: Directory containing genome .fna.gz files.
        accessions: List of assembly accessions to match.
    Returns:
        Dictionary mapping assembly accession to genome filename.
    """
    acc_set = set(accessions)
    result = {}
    for f in sorted(genome_dir.glob("*.fna.gz")):
        m = ACCESSION_RE.match(f.name)
        if m and m.group(1) in acc_set and m.group(1) not in result:
            result[m.group(1)] = f.name
    return result

def prepare_metadata(
    merged_metadata_path: str, virus_db_path: str, genomes_dir: str,
    output_metadata_path: str, output_genomes_dir: str,
) -> None:
    """Read metadata + virus DB, add species_taxid and local_filename, symlink genomes.
    Args:
        merged_metadata_path: Path to merged metadata TSV (may be gzipped).
        virus_db_path: Path to virus taxonomy DB TSV.
        genomes_dir: Directory containing downloaded genome .fna.gz files.
        output_metadata_path: Output path for prepared metadata TSV.
        output_genomes_dir: Output directory for symlinked genome files.
    """
    taxid_to_species = build_species_taxid_map(virus_db_path)
    with open_by_suffix(merged_metadata_path) as f:
        reader = csv.DictReader(f, delimiter="\t")
        in_fields = reader.fieldnames or []
        rows = list(reader)
    logger.info("Read %d metadata rows", len(rows))
    genome_dir, output_dir = Path(genomes_dir), Path(output_genomes_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_fields = list(in_fields) + ["species_taxid", "local_filename"]
    if not rows:
        logger.info("No metadata rows to process. Writing header-only output file.")
        with open(output_metadata_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=out_fields, delimiter="\t")
            writer.writeheader()
        return
    accessions = sorted({r["assembly_accession"] for r in rows})
    acc_to_file = match_genomes_to_accessions(genome_dir, accessions)
    logger.info("Matched %d/%d accessions to genome files", len(acc_to_file), len(accessions))
    # Symlink matched genomes into output directory
    for filename in acc_to_file.values():
        os.symlink(os.path.abspath(genome_dir / filename), output_dir / filename)
    n_written = 0
    with open(output_metadata_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, delimiter="\t")
        writer.writeheader()
        for row in rows:
            if row["assembly_accession"] not in acc_to_file:
                continue
            row["species_taxid"] = taxid_to_species.get(row["taxid"], "")
            row["local_filename"] = f"{output_genomes_dir}/{acc_to_file[row['assembly_accession']]}"
            writer.writerow(row)
            n_written += 1
    logger.info("Wrote %d rows (dropped %d unmatched)", n_written, len(rows) - n_written)

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
    prepare_metadata(args.merged_metadata, args.virus_db, args.genomes_dir,
                     args.output_metadata, args.output_genomes_dir)
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)

if __name__ == "__main__":
    main()
