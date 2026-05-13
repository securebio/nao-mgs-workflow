#!/usr/bin/env python
"""Filter a ncbi-genome-download viral metadata TSV by host infection status
and drop non-current assemblies."""

#=======================================================================
# Preamble
#=======================================================================

import argparse
import logging
from datetime import UTC, datetime

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
        meta_db: Viral metadata table from ncbi-genome-download (must include
            `taxid`, `species_taxid`, and `assembly_status` columns).
        virus_db: Virus taxa table annotated with per-host `infection_status_*`
            columns.
        host_taxa: Host taxon names to filter to (matching `infection_status_<name>`
            columns in `virus_db`).

    Returns:
        Filtered metadata: rows whose `taxid` or `species_taxid` matches a
        host-infecting virus and whose `assembly_status == "current"`. Other
        statuses ('previous', 'replaced', 'suppressed', etc., per NCBI's
        datasets OpenAPI enum) can introduce duplicate sequence IDs alongside
        the live record.
    """
    screen_cols = ["infection_status_" + t for t in host_taxa]
    screen_status = (virus_db[screen_cols] == "1").max(1)
    virus_taxids = virus_db[screen_status]["taxid"].reset_index(drop=True)
    host_infecting = meta_db.loc[(meta_db["taxid"].isin(virus_taxids)) | (meta_db["species_taxid"].isin(virus_taxids))]
    before = len(host_infecting)
    current = host_infecting.loc[host_infecting["assembly_status"] == "current"]
    logger.info("Dropped %d non-current assemblies.", before - len(current))
    return current

#=======================================================================
# Main
#=======================================================================

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("meta_db", help="Path to metadata table from ncbi-genome-download.")
    parser.add_argument("virus_db", help="Path to TSV of virus taxa, annotated with infection status.")
    parser.add_argument("host_taxa", help="Space-separated list of host taxon names to filter to.")
    parser.add_argument("output_db", help="Output path to filtered metadata TSV.")
    parser.add_argument("output_accessions", help="Output path to filtered list of genome accessions.")
    parser.add_argument("output_paths", help="Output path to filtered list of genome filepaths.")
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
    filtered["assembly_accession"].to_csv(args.output_accessions, index=False, header=False)
    filtered["local_filename"].to_csv(args.output_paths, index=False, header=False)
    logger.info("Script complete.")

if __name__ == "__main__":
    main()
