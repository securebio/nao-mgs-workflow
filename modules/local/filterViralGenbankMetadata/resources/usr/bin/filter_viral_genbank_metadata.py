#!/usr/bin/env python
"""Filter a viral metadata TSV (from `datasets summary`) by host infection
status and assembly status, then emit the filtered accessions in fixed-size
chunk files for parallel downstream download."""

#=======================================================================
# Preamble
#=======================================================================

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter('[%(asctime)s] %(message)s')
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#=======================================================================
# Filter
#=======================================================================

def filter_metadata(meta_db: pd.DataFrame, virus_db: pd.DataFrame, host_taxa: list[str]) -> pd.DataFrame:
    """Filter the viral metadata TSV to host-infecting, current assemblies.

    Args:
        meta_db: Viral metadata table from `datasets summary` (must include
            `taxid` and `assembly_status` columns).
        virus_db: Virus taxa table annotated with per-host `infection_status_*`
            columns and a `taxid_species` column for species-level rollup.
        host_taxa: Host taxon names to filter to (matching `infection_status_<name>`
            columns in `virus_db`).

    Returns:
        Filtered metadata: rows whose `taxid` (or rolled-up species-level
        taxid) matches a host-infecting virus and whose `assembly_status`
        is `current`. Other statuses ('previous', 'replaced', 'suppressed',
        etc., per NCBI's datasets OpenAPI enum) can introduce duplicate
        sequence IDs alongside the live record.
    """
    screen_cols = ["infection_status_" + t for t in host_taxa]
    screen_status = (virus_db[screen_cols] == "1").max(1)
    virus_taxids = virus_db[screen_status]["taxid"].reset_index(drop=True)
    # Roll the input taxid up to species level via virus_db so that strain-
    # level accessions whose taxid isn't directly host-infecting still match
    # if their species is. Replaces the prior reliance on a `species_taxid`
    # column in meta_db (which used to be added by PREPARE_VIRAL_METADATA
    # upstream of filtering; it now runs downstream of filtering).
    species_map = dict(zip(virus_db["taxid"], virus_db["taxid_species"], strict=True))
    species_taxid = meta_db["taxid"].map(species_map)
    host_infecting = meta_db.loc[meta_db["taxid"].isin(virus_taxids) | species_taxid.isin(virus_taxids)]
    before = len(host_infecting)
    current = host_infecting.loc[host_infecting["assembly_status"] == "current"]
    logger.info("Dropped %d non-current assemblies.", before - len(current))
    return current

#=======================================================================
# Chunking
#=======================================================================

def write_accession_chunks(accessions: pd.Series, chunk_dir: Path, chunk_size: int) -> int:
    """Write accessions to fixed-size chunk files for parallel download fan-out.

    Args:
        accessions: Series of assembly accessions to chunk.
        chunk_dir: Output directory for `chunk_NNNN.txt` files (created if absent).
        chunk_size: Maximum accessions per chunk (must be >= 1).

    Returns:
        Number of chunk files written. Always at least 1: an empty input
        produces a single empty chunk file so the downstream channel is
        well-defined (Nextflow can dispatch a no-op task rather than hang).
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
    chunk_dir.mkdir(parents=True, exist_ok=True)
    n = len(accessions)
    if n == 0:
        (chunk_dir / "chunk_0001.txt").write_text("")
        logger.info("No accessions passed filter; wrote 1 empty chunk file.")
        return 1
    n_chunks = (n + chunk_size - 1) // chunk_size
    for i in range(n_chunks):
        chunk = accessions.iloc[i * chunk_size:(i + 1) * chunk_size]
        chunk.to_csv(chunk_dir / f"chunk_{i + 1:04d}.txt", index=False, header=False)
    logger.info("Wrote %d accessions to %d chunk files (chunk_size=%d).", n, n_chunks, chunk_size)
    return n_chunks

#=======================================================================
# Main
#=======================================================================

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("meta_db", help="Path to metadata table from ENUMERATE_VIRAL_ACCESSIONS.")
    parser.add_argument("virus_db", help="Path to TSV of virus taxa, annotated with infection status.")
    parser.add_argument("host_taxa", help="Space-separated list of host taxon names to filter to.")
    parser.add_argument("output_db", help="Output path to filtered metadata TSV.")
    parser.add_argument("output_chunk_dir", help="Output directory for chunked accession lists.")
    parser.add_argument("chunk_size", type=int, help="Maximum accessions per chunk file.")
    return parser.parse_args()

def main() -> None:
    logger.info("Initializing script.")
    args = parse_arguments()
    logger.info("Importing input TSVs.")
    meta_db = pd.read_csv(args.meta_db, sep="\t", dtype=str)
    virus_db = pd.read_csv(args.virus_db, sep="\t", dtype=str)
    host_taxa = args.host_taxa.split(" ")
    logger.info("Filtering metadata table.")
    filtered = filter_metadata(meta_db, virus_db, host_taxa)
    logger.info("Writing output.")
    filtered.to_csv(args.output_db, sep="\t", index=False)
    write_accession_chunks(filtered["assembly_accession"], Path(args.output_chunk_dir), args.chunk_size)
    logger.info("Script complete.")

if __name__ == "__main__":
    main()
