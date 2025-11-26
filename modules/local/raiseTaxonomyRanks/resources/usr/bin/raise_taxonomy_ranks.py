#!/usr/bin/env python

#=======================================================================
# Preamble
#=======================================================================

# Import libraries
import pandas as pd
import subprocess
from typing import Dict, Set, List
import argparse
from collections import defaultdict
import logging
from datetime import datetime, timezone

# Configure logging
class UTCFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter('[%(asctime)s] %(message)s')
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

# Define constants
RANKS = ["subspecies", "species", "subgenus", "genus", "subfamily", "family",
         "suborder", "order", "class", "subphylum", "phylum", "kingdom",
         "superkingdom", "acellular root"]

#=======================================================================
# Auxiliary functions
#=======================================================================

def raise_rank_single(taxids: pd.Series, target_rank: str,
                      db: pd.DataFrame) -> tuple[pd.Series, bool]:
    """
    Given a Series of taxids and a target rank, perform a single
    rank-raise operation. Taxids below the target rank are replaced
    with their parent taxid, while taxids above the target rank are
    returned unchanged.
    Args:
        taxids (pd.Series): Series of taxids to raise.
        target_rank (str): Target rank to raise to.
        db (pd.DataFrame): DataFrame containing taxid hierarchy, with
            taxids as the index and parent taxids as a column.
    Returns:
        tuple[pd.Series, bool]: Tuple containing a Series of taxids
            raised to the target rank, and a boolean indicating whether
            any taxids were raised.
    """
    # Get assigned ranks for each taxid
    ranks = db.loc[taxids, "rank"]
    # If all ranks are equal to or above target rank, return unmodified
    ranks_above = RANKS[RANKS.index(target_rank):]
    if ranks.isin(ranks_above).all():
        return taxids, False
    # Otherwise, return the parent taxid for each taxid below target
    # rank, and the taxid itself for each taxid above target rank
    parent_taxids = db.loc[taxids, "parent_taxid"]
    taxids_out = taxids.where(ranks.isin(ranks_above), parent_taxids)
    return taxids_out, True

def raise_rank(taxids: pd.Series, target_rank: str,
               db: pd.DataFrame) -> pd.Series:
    """
    Given a Series of taxids and a target rank, traverse the taxid
    hierarchy to find the corresponding taxids at the target rank.
    Args:
        taxids (pd.Series): Series of taxids to raise.
        target_rank (str): Target rank to raise to.
        db (pd.DataFrame): DataFrame containing taxid hierarchy, with
            taxids as the index and parent taxids as a column.
    Returns:
        pd.Series: Series of taxids raised to the target rank, or
            pd.NA if the input taxid has no ancestor at that rank
            (e.g. because it is already above that rank).
    """
    # Check that the target rank is valid
    assert target_rank in RANKS, "Invalid target rank."
    # Check that all taxids to raise are in the taxonomy db index
    assert taxids.isin(db.index).all(), "Some taxids not in taxonomy db."
    # Raise ranks until all taxids are at or above the target rank
    iter_next = True
    while iter_next:
        taxids, iter_next = raise_rank_single(taxids, target_rank, db)
    # Replace taxids above the target rank with pd.NA
    ranks_high = RANKS[RANKS.index(target_rank)+1:]
    ranks = db.loc[taxids, "rank"]
    taxids_out = taxids.where(~ranks.isin(ranks_high), pd.NA)
    return taxids_out

def raise_rank_db(db: pd.DataFrame, target_rank: str) -> pd.DataFrame:
    """
    Given a DataFrame of taxids and their parent taxids, add a column
    containing the corresponding taxids at the target taxonomic rank.
    Args:
        db (pd.DataFrame): DataFrame containing taxid hierarchy, with
            taxids as the index and parent taxids as a column.
        target_rank (str): Target rank to raise to.
    Returns:
        pd.DataFrame: DataFrame with an additional column "taxid_[RANK]"
            containing the taxids at the target rank.
    """
    new_col_name = "taxid_" + target_rank
    new_col_values = raise_rank(db.index, target_rank, db)
    db[new_col_name] = new_col_values
    return db

def raise_ranks_db(db: pd.DataFrame, target_ranks: List[str]) -> pd.DataFrame:
    """
    Given a DataFrame of taxids and their parent taxids, add columns
    containing the corresponding taxids at each target taxonomic rank.
    Args:
        db (pd.DataFrame): DataFrame containing taxid hierarchy, with
            taxids as the index and parent taxids as a column.
        target_ranks (List[str]): List of target ranks to raise to.
    Returns:
        pd.DataFrame: DataFrame with additional columns "taxid_[RANK]"
            containing the taxids at each target rank.
    """
    for target_rank in target_ranks:
        db = raise_rank_db(db, target_rank)
    return db

#=======================================================================
# Main function
#=======================================================================

def main():
    logger.info("Initializing script.")
    # Define argument parsing
    desc = "Given a TSV of taxids and their parent taxids, add columns " \
           "containing the corresponding taxids at each target taxonomic rank."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("taxonomy_db", help="Path to TSV of taxids, parent taxids and ranks.")
    parser.add_argument("target_ranks", help="Space-delimited list of target ranks to raise to.")
    parser.add_argument("output_db", help="Path to output TSV.")
    args = parser.parse_args()
    # Import TSV and set taxids as index (while keeping taxid column)
    logger.info("Importing input TSV.")
    taxonomy_db = pd.read_csv(args.taxonomy_db, sep="\t", dtype=str)
    taxonomy_db = taxonomy_db.set_index("taxid", drop=False)
    logger.info(f"Imported {taxonomy_db.shape[0]} taxids.")
    logger.info(f"Columns: {taxonomy_db.columns.tolist()}")
    dims = taxonomy_db.shape
    # Parse target ranks
    target_ranks = args.target_ranks.split()
    logger.info(f"Target ranks: {target_ranks}")
    # Add columns for each target rank
    logger.info("Raising ranks.")
    for target_rank in target_ranks:
        taxonomy_db = raise_rank_db(taxonomy_db, target_rank)
    logger.info("Ranks raised.")
    # New DB should have equal rows and extra columns
    assert taxonomy_db.shape[0] == dims[0]
    assert taxonomy_db.shape[1] == dims[1] + len(target_ranks)
    logger.info(f"Columns: {taxonomy_db.columns.tolist()}")
    # Write output
    logger.info("Writing output.")
    taxonomy_db.to_csv(args.output_db, sep="\t", index=False,
                       na_rep="NA", header=True)
    logger.info("Script complete.")

if __name__ == "__main__":
    main()
