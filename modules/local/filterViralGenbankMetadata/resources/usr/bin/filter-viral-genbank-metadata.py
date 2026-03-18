#!/usr/bin/env python

#=======================================================================
# Preamble
#=======================================================================

# Import libraries
import pandas as pd
import argparse
import logging
from datetime import datetime, timezone

# Configure logging
class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter('[%(asctime)s] %(message)s')
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#=======================================================================
# Main function
#=======================================================================

def main() -> None:
    logger.info("Initializing script.")
    # Define argument parsing
    parser = argparse.ArgumentParser(description="Filter a ncbi-genome-download viral metadata TSV by host infection status.")
    parser.add_argument("meta_db", help="Path to metadata table from ncbi-genome-download.")
    parser.add_argument("virus_db", help="Path to TSV of virus taxa, annotated with infection status.")
    parser.add_argument("host_taxa", help="Space-separated list of host taxon names to filter to.")
    parser.add_argument("output_db", help="Output path to filtered metadata TSV.")
    parser.add_argument("output_accessions", help="Output path to filtered list of genome accessions.")
    parser.add_argument("output_paths", help="Output path to filtered list of genome filepaths.")
    args = parser.parse_args()
    # Import inputs
    logger.info("Importing input TSVs.")
    meta_db = pd.read_csv(args.meta_db, sep="\t", dtype=str)
    virus_db = pd.read_csv(args.virus_db, sep="\t", dtype=str)
    host_taxa = args.host_taxa.split(" ")
    # Get list of taxids to search for
    logger.info("Getting virus taxids with appropriate infection status.")
    screen_cols = ["infection_status_" + t for t in host_taxa]
    screen_status = (virus_db[screen_cols] == "1").max(1)
    virus_taxids = virus_db[screen_status]["taxid"].reset_index(drop=True)
    # Filter metadata DB by screening taxa
    logger.info("Filtering metadata table to those virus taxids.")
    meta_db_filtered = meta_db.loc[(meta_db["taxid"].isin(virus_taxids)) | (meta_db["species_taxid"].isin(virus_taxids))]
    # Write output
    logger.info("Writing output.")
    meta_db_filtered.to_csv(args.output_db, sep="\t", index=False)
    meta_db_filtered["assembly_accession"].to_csv(args.output_accessions, index=False, header=False)
    meta_db_filtered["local_filename"].to_csv(args.output_paths, index=False, header=False)
    logger.info("Script complete.")

if __name__ == "__main__":
    main()
