#!/usr/bin/env python

# =======================================================================
# Preamble
# =======================================================================

# Import libraries
import pandas as pd
from pathlib import Path
import argparse
from collections import defaultdict
from typing import override
import logging
from logging import LogRecord
from datetime import datetime, timezone


# Configure logging
class UTCFormatter(logging.Formatter):
    @override
    def formatTime(self, record: LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

# Define viral host infection states
# See docs/annotation.md for more details
INCONSISTENT = 0
MATCH = 1
UNCLEAR = 2
CONSISTENT = 3
UNRESOLVED = -1
MAYBE_INCONSISTENT = -2

# =======================================================================
# Auxiliary functions
# =======================================================================


def get_virus_host_mapping(db_path: str) -> dict[str, set[str]]:
    """
    Import a TSV from Virus-Host-DB and extract virus/host info into
    a dictionary.
    Args:
        db_path (str): Path to the Virus-Host-DB TSV File.
    Returns:
        dict[str, set[str]]: A dictionary mapping virus taxids to
            sets of host taxids.
    """
    logger.info("Importing Virus-Host-DB.")
    df = pd.read_csv(db_path, sep="\t", dtype=str)
    logger.info("Generating mapping from Virus-Host-DB.")
    mapping: dict[str, set[str]] = df.groupby("virus tax id")["host tax id"].apply(set).to_dict()  # type: ignore[assignment]
    return mapping


def expand_taxid(taxid: str, nodes: pd.DataFrame) -> set[str]:
    """
    Given a starting taxid and an NCBI nodes DataFrame, return a set of
    all taxids descended from that starting taxid (including itself).
    Args:
        taxid (str): Ancestral taxid as a numeric string.
        nodes (pd.DataFrame): A DataFrame generated from an NCBI nodes DMP
            file giving the parent taxid of each taxid within the NCBI
    Returns:
        set[str]: set of descendant taxids, including the original
            ancestor.
    """
    taxids_old = {taxid}
    taxids_desc = nodes.loc[nodes["parent_taxid"].isin(taxids_old)]["taxid"]
    taxids_new = taxids_old.union(taxids_desc)
    while taxids_new > taxids_old:
        taxids_old = taxids_new
        taxids_desc = nodes.loc[nodes["parent_taxid"].isin(taxids_old)]["taxid"]
        taxids_new = taxids_old.union(taxids_desc)
    return taxids_new


def get_host_taxids(hosts: dict[str, str], nodes: pd.DataFrame) -> dict[str, set[str]]:
    """
    Given a dictionary mapping host group names to host ancestor taxids,
    return a dictionary instead mapping those host names to sets of
    descendant taxids.
    Args:
        hosts (dict[str, str]): A dictionary mapping string names for
            host taxa (e.g. "human", "vertebrate") to ancestral taxids
            as numeric strings (e.g. "9606", "7742").
        nodes (pd.DataFrame): A DataFrame generated from an NCBI nodes DMP
            file giving the parent taxid of each taxid within the NCBI
            taxonomy structure.
    Returns:
        dict[str, set[str]]: A dictionary mapping those same string
            names to sets of descendant taxids returned by
            expand_taxid().
    """
    logger.info("Generating host taxid dictionary from input TSV.")
    host_dict_out: dict[str, set[str]] = dict()
    for k in hosts.keys():
        host_dict_out[k] = expand_taxid(hosts[k], nodes)
        n_desc = len(host_dict_out[k])
        logger.info(
            f'\tFound {n_desc} descendant taxids for host group "{k}" (taxid {hosts[k]}).'
        )
    logger.info("Host taxid dictionary construction complete.")
    return host_dict_out


def build_virus_tree(viral_taxa_df: pd.DataFrame) -> dict[str, set[str]]:
    """
    Build a dictionary representing the viral taxonomic tree from a
    viral taxa DataFrame.
    Args:
        viral_taxa_df (pd.DataFrame): DataFrame containing viral
            taxa information. Must have taxid and parent_taxid
            columns.
    Returns:
        dict[str, set[str]]: A dictionary mapping virus taxids
            to sets of their child taxids.
    """
    logger.info("Generating viral parent-child tree from viral taxa DB.")
    tree: dict[str, set[str]] = defaultdict(set)
    for _, row in viral_taxa_df.iterrows():
        tree[row["parent_taxid"]].add(row["taxid"])
    logger.info("Viral parent-child tree generation complete.")
    return tree


def mark_direct_infections(
    virus_taxids: pd.Series,
    host_taxids: set[str],
    virus_host_mapping: dict[str, set[str]],
) -> pd.Series:
    """For a given set of host taxids, check whether each virus taxid in a
    series is directly marked as infecting any of those host taxids,
    according to Virus-Host-DB. Doesn't consider the infection status of
    ancestral or descendant taxids.
    Args:
        virus_taxids (pd.Series): Series of virus taxids to check.
        host_taxids (set[str]): Series of host taxids to check against.
            Should be expanded to include all descendant taxids using
            netch_descendants().
        virus_host_mapping (dict[str, set[str]]): Mapping of virus
            taxids to host taxids, generated using
            get_virus_host_mapping().
    Returns:
        pd.Series: Integer series giving direct infection status of each
            viral taxid in virus_taxids.
    """

    def check_direct_infection(virus_taxid: str) -> int:
        if virus_taxid in virus_host_mapping:
            # Return 1 if the virus's hosts overlap with the specified host taxids
            # otherwise return 0
            if bool(virus_host_mapping[virus_taxid] & host_taxids):
                return MATCH
            else:
                return INCONSISTENT
        return UNRESOLVED  # Indicates not in mapping; needs special handling downstream

    statuses = virus_taxids.apply(check_direct_infection)
    return pd.Series(statuses.values, index=virus_taxids)


def add_descendants(
    virus_tree: dict[str, set[str]], taxids_start: set[str]
) -> set[str]:
    """
    Given a viral taxonomy tree and a set of starting taxids, return a set
    containing those taxids and all their descendants.
    Args:
        virus_tree (dict[str, set[str]]): Viral taxonomic tree,
            generated from a viral DB using build_virus_tree().
        taxids_start (set[str]): Initial set of taxa for which to find
            descendants.
    Returns:
        set[str]: Expanded set of taxids including taxids_start and all
            their descendants.
    """

    def expand_taxids(taxids: set[str]) -> set[str]:
        new_taxids = set(taxids)
        for taxid in taxids:
            new_taxids.update(virus_tree.get(taxid, set()))
        return new_taxids

    taxids = taxids_start
    taxids_new = expand_taxids(taxids)
    while taxids_new > taxids:
        taxids = taxids_new
        taxids_new = expand_taxids(taxids)
    return taxids_new


def mark_descendant_infections(
    virus_tree: dict[str, set[str]], statuses: pd.Series
) -> pd.Series:
    """
    Given a series of infection statuses, propagate statuses
    from each taxid to its descendants and return a new status series.
    Args:
        virus_tree (dict[str, set[str]]): Viral taxonomic tree,
            generated from a viral DB using build_virus_tree().
        statuses (pd.Series): Integer series of infection statuses
            for virus_taxids.
    Returns:
        pd.Series: Integer series of updated infection statuses.
    """
    # All descendants of taxids marked MATCH should be marked MATCH
    taxids_match = set(statuses.index[statuses == MATCH])
    taxids_match_expanded = add_descendants(virus_tree, taxids_match)
    statuses[statuses.index.isin(taxids_match_expanded)] = MATCH
    # All descendants of taxids marked INCONSISTENT should be marked INCONSISTENT
    taxids_inconsistent = set(statuses.index[statuses == INCONSISTENT])
    taxids_inconsistent_expanded = add_descendants(virus_tree, taxids_inconsistent)
    statuses[statuses.index.isin(taxids_inconsistent_expanded)] = INCONSISTENT
    # Descendants of taxids marked CONSISTENT should be marked CONSISTENT iff
    # they are not already marked MATCH
    taxids_consistent = set(statuses.index[statuses == CONSISTENT])
    taxids_consistent_expanded = add_descendants(virus_tree, taxids_consistent)
    assert (
        statuses[statuses.index.isin(taxids_consistent_expanded)]
        .isin([INCONSISTENT, UNCLEAR])
        .sum()
        == 0
    ), (
        "Some taxids marked as CONSISTENT have descendants marked as INCONSISTENT or UNCLEAR."
    )
    statuses[
        statuses.index.isin(taxids_consistent_expanded) & ~statuses.isin([MATCH])
    ] = CONSISTENT
    # Descendants of a taxid marked UNCLEAR should be marked UNCLEAR iff they are marked UNRESOLVED or MAYBE_INCONSISTENT
    # Do not overwrite descendants that are already marked INCONSISTENT
    taxids_unclear = set(statuses.index[statuses == UNCLEAR])
    taxids_unclear_expanded = add_descendants(virus_tree, taxids_unclear)
    statuses[
        statuses.index.isin(taxids_unclear_expanded)
        & statuses.isin([UNRESOLVED, MAYBE_INCONSISTENT])
    ] = UNCLEAR
    # After this, there should be no taxids marked UNRESOLVED
    assert statuses.isin([UNRESOLVED]).sum() == 0, "Some taxids are still unresolved."
    assert statuses.isin([MAYBE_INCONSISTENT]).sum() == 0, (
        "Some taxids are still maybe inconsistent."
    )
    return statuses


def exclude_infections(
    virus_tree: dict[str, set[str]], statuses: pd.Series, exclude_taxids: list[str]
) -> pd.Series:
    """
    Given a taxonomy tree, a series of infection status generated by
    mark_descendant_infections(), and a set of taxids to exclude,
    assign all taxids descended from those taxids a status of INCONSISTENT.
    Args:
        virus_tree (dict[str, set[str]]): Viral taxonomic tree,
            generated from a viral DB using build_virus_tree().
        statuses (pd.Series): Integer series of infection statuses
            for virus_taxids.
        exclude_taxids (list[str]): list of virus taxids to exclude from
            host annotation (i.e. to force to mark as non-infecting).
    Returns:
        pd.Series: Integer series of updated infection statuses.
    """
    exclude_taxids_expanded = add_descendants(virus_tree, set(exclude_taxids))
    statuses[statuses.index.isin(exclude_taxids_expanded)] = INCONSISTENT
    return statuses


def mark_ancestor_infections_single(
    virus_taxid: str, virus_df: pd.DataFrame, virus_tree: dict[str, set[str]]
) -> pd.DataFrame:
    """
    Given a single viral taxid of interest, a DataFrame giving current
    infection statuses for each of a set of viral taxids, and
    a mapping of each taxid to its children, conduct a recursive,
    memoized tree search to determine the infection of the focal taxid
    based on the infection statuses of its descendants.
    Args:
        virus_taxid (str): Specific viral taxid whose status to check.
        virus_df (pd.DataFrame): DataFrame with a column "taxid" giving
            a list of all viral taxids, a column "status" giving
            their currently-assigned infection status (0, 1 or 2),
            and a boolean column "checked" indicating whether its status
            has been checked and assigned by this function.
        virus_tree (dict[str, set[str]]): Viral taxonomic tree
            generated from a viral DB using build_virus_tree(),
            mapping each taxid to a set of its child taxids.
    Returns:
        pd.DataFrame: An updated DataFrame in the same format as
            virus_df.
    """

    # Define auxiliary functions
    def set_checked(taxid: str, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[taxid, "checked"] = True
        return df

    def set_status(taxid: str, df: pd.DataFrame, value: int) -> pd.DataFrame:
        df.loc[taxid, "status"] = value
        return set_checked(taxid, df)

    # Before passing to this function, already confirmed that taxid
    # has children and is unresolved, so start by getting children
    children = virus_df.loc[list(virus_tree[virus_taxid])]
    # If all children have been checked already, evaluate on the basis
    # of child statuses
    if children["checked"].all():
        child_statuses = children["status"]
        # If all children are marked UNRESOLVED, keep current status
        if (child_statuses == UNRESOLVED).all():
            return set_checked(virus_taxid, virus_df)
        # Otherwise, if all children have the same status, inherit that status
        elif (child_statuses == child_statuses.iloc[0]).all():
            return set_status(virus_taxid, virus_df, child_statuses.iloc[0])
        # If any child is marked UNCLEAR, mark this taxid as UNCLEAR
        elif child_statuses.isin([UNCLEAR]).any():
            return set_status(virus_taxid, virus_df, UNCLEAR)
        # If there are both children marked [INCONSISTENT or MAYBE_INCONSISTENT] and marked [MATCH or CONSISTENT],
        # mark this taxid as UNCLEAR
        elif (
            child_statuses.isin([INCONSISTENT, MAYBE_INCONSISTENT]).any()
            and child_statuses.isin([MATCH, CONSISTENT]).any()
        ):
            return set_status(virus_taxid, virus_df, UNCLEAR)
        # If any child is marked [INCONSISTENT or MAYBE_INCONSISTENT], retain any existing mark of INCONSISTENT
        # on the parent node, otherwise mark MAYBE_INCONSISTENT
        elif (child_statuses.isin([INCONSISTENT, MAYBE_INCONSISTENT])).any():
            # If parent is marked INCONSISTENT, keep current status
            if virus_df.loc[virus_taxid]["status"] == INCONSISTENT:
                return set_checked(virus_taxid, virus_df)
            # Otherwise, mark as MAYBE_INCONSISTENT. The parent must be UNRESOLVED since:
            # - MATCH nodes are already checked and excluded from this function
            # - CONSISTENT is not an initial state (only UNRESOLVED, MATCH, or INCONSISTENT are)
            # - We only pass UNRESOLVED and INCONSISTENT nodes to this function
            return set_status(virus_taxid, virus_df, MAYBE_INCONSISTENT)
        # Otherwise, mark as CONSISTENT
        # This covers all mixtures of MATCH, UNRESOLVED and CONSISTENT
        else:
            return set_status(virus_taxid, virus_df, CONSISTENT)
    # Otherwise, run this function for each unchecked child, then repeat
    children_unchecked = children.loc[~children["checked"]]
    for child in children_unchecked.index:
        virus_df = mark_ancestor_infections_single(child, virus_df, virus_tree)
    return mark_ancestor_infections_single(virus_taxid, virus_df, virus_tree)


def mark_ancestor_infections(
    virus_tree: dict[str, set[str]], statuses: pd.Series
) -> pd.Series:
    """
    Given a series of infection statuses, propagate statuses
    to ancestor taxa and return a new status series.
    Args:
        virus_tree (dict[str, set[str]]): Viral taxonomic tree
            generated from a viral DB using build_virus_tree(),
            mapping each taxid to a set of its child taxids.
        statuses (pd.Series): Integer series of direct infection statuses
            for virus_taxids.
    Returns:
        pd.Series: Integer series of updated infection statuses.
    """
    # Create a dataframe of taxids and current statuses
    df = pd.DataFrame({"status": statuses})
    # Mark taxids maked MATCH as checked
    df["checked"] = df["status"] == MATCH
    # Get number of children and mark childless nodes as checked
    df["n_children"] = df.index.map(lambda x: len(virus_tree[x]))
    df["checked"] = df["checked"] | (df["n_children"] == 0)
    # For nodes with children, count how many have been checked
    # Iterate over unchecked rows until all rows have been checked
    while not df["checked"].all():
        for taxid in df.index:
            if df.loc[taxid]["checked"]:
                continue
            df = mark_ancestor_infections_single(taxid, df, virus_tree)
    return df["status"]


def check_infection(
    virus_taxids: pd.Series,
    host_taxids: set[str],
    virus_tree: dict[str, set[str]],
    virus_host_mapping: dict[str, set[str]],
    hard_exclude_taxids: list[str],
) -> pd.Series:
    """
    For a single set of host taxids, classify each virus taxid in a series
    according to its infection status for that host set and return an integer
    series of classifications (see docs/annotation.md for more details):
        - An infection status of MATCH (1) designates taxa that are affirmatively
          marked as infecting that host group. It includes taxa that are directly
          marked in Virus-Host DB as infecting that host group; taxa that are
          descended from such taxa; and taxa whose descendants are all marked MATCH.
        - An infection status of INCONSISTENT (0) designates taxa that are marked
          as not infecting that host group. It includes taxa that: are included in
          Virus-Host DB; are not marked in Virus-Host DB as infecting that host group;
          and have no MATCH-marked descendants. It also includes taxa that are
          descended from such taxa, and taxa whose descendants are all marked INCONSISTENT.
        - An infection status of CONSISTENT (3) designates taxa that are likely to
          infect that host group, but cannot be affirmatively marked as such. It
          includes taxa that are not included in Virus-Host DB, have at least one
          MATCH-marked descendant, and no INCONSISTENT-marked descendants. It also
          includes all taxa that are descended from such taxa and cannot be marked
          MATCH according to the rules above, as well as taxa whose descendants are
          all marked CONSISTENT.
        - Finally, an infection status of UNCLEAR (2) designates taxa of uncertain
          infection status. It includes all other taxa: those that are not included
          in Virus-Host DB and have both INCONSISTENT- and MATCH-marked descendants;
          those descended from a UNCLEAR-marked taxon that are not given another
          status by the rules above; and those whose descendants are all marked UNCLEAR.
    If a taxon is included in hard_exclude_taxids, it and all descendants
    are marked 0 regardless of Virus-Host-DB; this is intended to account
    for misannotated taxa.
    Args:
        virus_taxids (pd.Series): Series of virus taxids to check.
        host_taxids (set[str]): Series of host taxids to check against.
            Should be expanded to include all descendant taxids using
            expand_taxid().
        virus_tree (dict[str, set[str]]): Viral taxonomic tree,
            generated from a viral DB using build_virus_tree().
        virus_host_mapping (dict[str, set[str]]): Mapping of virus
            taxids to host taxids, generated using
            get_virus_host_mapping().
        hard_exclude_taxids (list[str]): list of virus taxids to hard-exclude
            from host annotation (i.e. to force to mark as non-infecting).
    Returns:
        pd.Series: Four-state integer series of infection statuses for each viral
            taxid in virus_taxids.
    """
    # Start by marking direct infections
    logger.info("\tMarking direct infection status from Virus-Host-DB.")
    statuses = mark_direct_infections(virus_taxids, host_taxids, virus_host_mapping)
    # If no direct infections, mark all as INCONSISTENT
    if statuses.isin([MATCH]).sum() == 0:
        logger.info("\tNo direct infections found, marking all as INCONSISTENT.")
        return pd.Series(INCONSISTENT, index=statuses.index)
    # Exclude hard-excluded taxa
    logger.info("\tExclude hardcoded non-infecting taxids.")
    statuses = exclude_infections(virus_tree, statuses, hard_exclude_taxids)
    # Expand to ancestors
    logger.info("\tPropagating infection status to ancestor taxids.")
    statuses = mark_ancestor_infections(virus_tree, statuses)
    # Expand to descendants
    logger.info("\tPropagating infection status to descendant taxids.")
    statuses = mark_descendant_infections(virus_tree, statuses)
    logger.info("\tInfection status inference complete.")
    return statuses


def annotate_virus_db_single(
    virus_db: pd.DataFrame,
    host_name: str,
    host_taxids: set[str],
    virus_tree: dict[str, set[str]],
    virus_host_mapping: dict[str, set[str]],
    hard_exclude_taxids: list[str],
) -> pd.DataFrame:
    """
    Annotate a DataFrame of virus taxa with infection status information
    for a specified host taxon.
    Args:
        virus_db (pd.DataFrame): DataFrame with columns indicating taxid
            and parent taxid for each virus.
        host_name (str): String name for host taxon of interest, used to
            label infection status column.
        host_taxids (set[str]): Expanded set of taxids within host taxon,
            generated with get_host_taxids().
        virus_tree (dict[str, set[str]]): dictionary giving viral
            taxonomic structure, generated with build_virus_tree().
        virus_host_mapping (dict[str, set[str]]): Mapping of virus
            taxids to host taxids, generated using
            get_virus_host_mapping().
        hard_exclude_taxids (list[str]): list of virus taxids to hard-exclude
            from host annotation (i.e. to force to mark as non-infecting).
    Returns:
        pd.DataFrame: Copy of virus_db with an additional column giving
            infection status for the specified host taxon.
    """

    # Get infection statuses
    logger.info(f"Inferring infection statuses for {host_name}-infecting viruses.")
    statuses = check_infection(
        virus_db["taxid"],
        host_taxids,
        virus_tree,
        virus_host_mapping,
        hard_exclude_taxids,
    )
    logger.info(f"Infection statuses for {host_name}-infecting viruses inferred.")
    # Annotate DB with infection statuses and return
    virus_db.set_index("taxid", inplace=True, drop=False)
    virus_db["infection_status_" + host_name] = statuses
    # virus_db.reset_index(inplace=True, drop=True)
    logger.info(f"Virus DB annotated with {host_name}-infection status.")
    return virus_db


def annotate_virus_db(
    virus_db: pd.DataFrame,
    host_mapping: dict[str, set[str]],
    virus_host_mapping: dict[str, set[str]],
    hard_exclude_taxids: list[str],
) -> pd.DataFrame:
    """
    Given a DataFrame of virus taxa (including taxids and parent taxids)
    and another of host taxa (including names and taxids) add a column
    to the former for each taxid in the latter indicating infection
    status for each virus.
    Args:
        virus_db (pd.DataFrame): DataFrame with columns indicating taxid
            and parent taxid for each virus.
        host_mapping (dict[str, set[str]]): Mapping of host names (e.g.
            "human", "bird") to sets of member taxids.
        virus_host_mapping (dict[str, set[str]]): Mapping of virus
            taxids to host taxids, generated using
            get_virus_host_mapping().
        hard_exclude_taxids (list[str]): list of virus taxids to hard-exclude
            from host annotation (i.e. to force to mark as non-infecting).
    Returns:
        pd.DataFrame: Copy of virus_db with additional columns giving
            infection status for each taxon in host_db.
    """
    # Get viral taxonomic tree
    virus_tree = build_virus_tree(virus_db)
    # Add annotations for each host group
    for k in host_mapping.keys():
        virus_db = annotate_virus_db_single(
            virus_db,
            k,
            host_mapping[k],
            virus_tree,
            virus_host_mapping,
            hard_exclude_taxids,
        )
    return virus_db


# =======================================================================
# Main function
# =======================================================================


def main() -> None:
    logger.info("Initializing script.")
    # Define argument parsing
    parser = argparse.ArgumentParser(
        description="Annotate a viral TSV with host infection status."
    )
    _ = parser.add_argument(
        "virus_db", type=Path, help="Path to TSV of virus taxids and tree structure."
    )
    _ = parser.add_argument(
        "host_db", type=Path, help="Path to TSV of target host taxids and names."
    )
    _ = parser.add_argument(
        "infection_db",
        type=Path,
        help="Path to TSV from Virus-Host DB giving host information.",
    )
    _ = parser.add_argument(
        "nodes_db", type=Path, help="Path to NCBI taxonomy nodes file."
    )
    _ = parser.add_argument(
        "hard_exclude_taxids",
        type=str,
        help="Space-delimited list of viral taxids to hard-exclude from host annotation.",
    )
    _ = parser.add_argument(
        "output_db", type=Path, help="Output path for host-annotated TSV."
    )
    args = parser.parse_args()
    # Import inputs
    logger.info("Importing input TSVs.")
    virus_db = pd.read_csv(args.virus_db, sep="\t", dtype=str)
    host_db = pd.read_csv(args.host_db, sep="\t", dtype=str)
    nodes_db = (
        pd.read_csv(args.nodes_db, sep="\t", dtype=str, header=None)
        .iloc[:, [0, 2]]
        .rename(columns={0: "taxid", 2: "parent_taxid"})
    )
    virus_host_mapping = get_virus_host_mapping(args.infection_db)
    hard_exclude_taxids = args.hard_exclude_taxids.split(" ")
    # Prepare dictionary of host taxids
    host_dict_single: dict[str, str] = host_db.set_index("name")["taxid"].to_dict()  # type: ignore[assignment]
    host_dict_full = get_host_taxids(host_dict_single, nodes_db)
    # Add annotations
    logger.info("Initializing infection-state annotation.")
    output_db = annotate_virus_db(
        virus_db, host_dict_full, virus_host_mapping, hard_exclude_taxids
    )
    # Write output
    logger.info("Writing output.")
    output_db.to_csv(args.output_db, sep="\t", index=False, na_rep="NA", header=True)
    logger.info("Script complete.")


if __name__ == "__main__":
    main()
