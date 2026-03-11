#!/usr/bin/env python

"""
Given a sorted TSV containing group, taxid, and score columns,
return a TSV with a single row per group containing the lowest common ancestor
taxid across all taxids in the group.
"""

#=======================================================================
# Preamble
#=======================================================================

# Import libraries
import logging
import argparse
from datetime import datetime, timezone
import gzip
from dataclasses import dataclass
from collections import defaultdict
from typing import IO, TextIO, cast
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

# Define constants
TAXID_ROOT = 1

#=======================================================================
# LCA functions
#=======================================================================

def path_to_root(
        taxid: int,
        child_to_parent: dict[int, int],
        path_cache: dict[int, list[int]],
        ) -> tuple[list[int], dict[int, list[int]]]:
    """
    Find the path from a taxid to the root of the taxonomy tree.
    Args:
        taxid (int): The starting taxid.
        child_to_parent (dict[int, int]): Dictionary mapping each taxid to its parent.
        path_cache (dict[int, list[int]]): Cache of precomputed paths to the root.
    Returns:
        tuple[list[int], dict[int, list[int]]]: Tuple containing the path to the root
            and the updated path cache.
    """
    logger.debug(f"Finding path to root for taxid: {taxid}")
    # Check input type
    assert isinstance(taxid, int), "Taxid must be an integer."
    # If path is already cached, return it
    if taxid in path_cache:
        logger.debug(f"Path to root for target taxid {taxid} already cached: {path_cache[taxid]}")
        return path_cache[taxid], path_cache
    # Initialize path
    path: list[int] = [taxid]
    # If taxid is not in child_to_parent, raise a warning and return the root
    if taxid not in child_to_parent:
        logger.warning(f"Taxid {taxid} not found in child_to_parent dictionary.")
        path.append(TAXID_ROOT)
    # Otherwise, traverse up the tree until the root is reached
    else:
        while path[-1] != TAXID_ROOT:
            parent = child_to_parent[path[-1]]
            # Should not encounter loops before reaching the root
            if parent == path[-1]:
                msg = f"Taxid {taxid} has a self-loop in the child_to_parent dictionary."
                logger.warning(msg)
                path.append(TAXID_ROOT)
                break
            # Check if path for parent is already cached
            if parent in path_cache:
                logger.debug(f"Path to root for ancestor taxid {parent} already cached: {path_cache[parent]}")
                path.extend(path_cache[parent])
            # Otherwise, add parent to path
            else:
                path.append(parent)
    # Check path includes taxid and root
    assert path[0] == taxid, "Path does not start with taxid."
    assert path[-1] == TAXID_ROOT, "Path does not end with root."
    # Add paths to cache for target taxid and all its parents
    for i in range(len(path)):
        # Each path is a suffix of the previous path
        path_cache[path[i]] = path[i:]
    # Return the path and cache
    logger.debug(f"Path to root for target taxid {taxid}: {path}")
    return path, path_cache

def find_lca_paths(
        path1: list[int],
        path2: list[int],
        ) -> int:
    """
    Given two paths from a taxid to the root, find the lowest common ancestor
    shared by both paths.
    Args:
        path1 (list[int]): Path from first taxid to root.
        path2 (list[int]): Path from second taxid to root.
    Returns:
        int: The LCA taxid.
    """
    # If paths are identical, return the first taxid
    if path1 == path2:
        return path1[0]
    # Otherwise, walk down paths from root until they diverge
    path1_copy = path1.copy()
    path2_copy = path2.copy()
    lca = TAXID_ROOT
    while path1_copy and path2_copy:
        ancestor1 = path1_copy.pop()
        ancestor2 = path2_copy.pop()
        if ancestor1 != ancestor2:
            break
        lca = ancestor1
    return lca

def find_lca_pair(
        taxid1: int,
        taxid2: int,
        child_to_parent: dict[int, int],
        path_cache: dict[int, list[int]],
        ) -> tuple[int, dict[int, list[int]]]:
    """
    Find the lowest common ancestor (LCA) of two taxids.
    Args:
        taxid1 (int): First taxid.
        taxid2 (int): Second taxid.
        child_to_parent (dict[int, int]): Dictionary mapping each taxid to its parent.
        path_cache (dict[int, list[int]]): Cache of precomputed paths to the root.
    Returns:
        tuple[int, dict[int, list[int]]]: Tuple containing the LCA taxid
            and the updated path cache.
    """
    logger.debug(f"Finding pairwise LCA for taxids: {taxid1} and {taxid2}")
    # Check types
    assert isinstance(taxid1, int), "Taxid 1 must be an integer."
    assert isinstance(taxid2, int), "Taxid 2 must be an integer."
    # If taxids are the same, return that value as LCA
    if taxid1 == taxid2:
        return taxid1, path_cache
    # Get paths to root
    path1, path_cache = path_to_root(taxid1, child_to_parent, path_cache)
    path2, path_cache = path_to_root(taxid2, child_to_parent, path_cache)
    # Find LCA by walking down paths until they diverge
    lca = find_lca_paths(path1, path2)
    logger.debug(f"Found LCA for taxids {taxid1} and {taxid2}: {lca}")
    return lca, path_cache

def find_lca_set(
        taxids: set[int],
        child_to_parent: dict[int, int],
        path_cache: dict[int, list[int]],
        ) -> tuple[int, dict[int, list[int]]]:
    """
    Find the lowest common ancestor (LCA) of a set of taxids.
    Args:
        taxids (set[int]): Set of taxids to find the LCA for.
        child_to_parent (dict[int, int]): Dictionary mapping each taxid to its parent.
        path_cache (dict[int, list[int]]): Cache of precomputed paths to the root.
    Returns:
        tuple[int, dict[int, list[int]]]: Tuple containing the LCA taxid
            and the updated path cache.
    """
    logger.debug(f"Finding LCA for taxids: {taxids}")
    taxids_start = taxids.copy()
    assert isinstance(taxids, set), f"Taxids must be a set, got {type(taxids)} ({taxids})."
    assert len(taxids) > 0, "Taxids must be non-empty."
    # If only one taxid, return it as LCA
    if len(taxids) == 1:
        taxid = taxids.pop()
        logger.debug(f"Only one taxid, returning it as LCA: {taxid}")
        return taxid, path_cache
    # If any taxid is not in the dictionary, raise a warning and return the root
    for taxid in taxids:
        if taxid not in child_to_parent:
            logger.warning(f"Taxid {taxid} not found in child_to_parent dictionary; returning root.")
            return TAXID_ROOT, path_cache
    # Otherwise, find iterated pairwise LCA
    lca, path_cache = find_lca_pair(taxids.pop(), taxids.pop(), child_to_parent, path_cache)
    while taxids:
        lca, path_cache = find_lca_pair(lca, taxids.pop(), child_to_parent, path_cache)
    # Return the LCA and the updated path cache
    logger.debug(f"Found LCA for taxids {taxids_start}: {lca}")
    return lca, path_cache

#=======================================================================
# Subgroup class and functions
#=======================================================================

@dataclass
class Subgroup:
    """
    A subgroup of a Group object, tracking information about a subset of
    entries, including taxids, scores, and the top taxid and LCA.
    """
    taxids: list[int]
    taxids_classified: list[bool]
    scores: list[float]
    summary: dict[str, int|float|bool|None] | None

def new_subgroup(
        taxid: int | None,
        score: float | None,
        unclassified_taxids: set[int],
    ) -> Subgroup:
    """
    Create a new Subgroup object from minimal input information.
    Args:
        taxid (int | None): Taxid of entry, if any.
        score (float | None): Score of entry, if any.
        unclassified_taxids (set[int]): Set of unclassified taxids.
    Returns:
        Subgroup: New Subgroup object.
    """
    return Subgroup(
        taxids = [taxid] if taxid is not None else [],
        taxids_classified = [taxid not in unclassified_taxids] if taxid is not None else [],
        scores = [score] if score is not None else [],
        summary = None,
    )

def update_subgroup(
        subgroup: Subgroup,
        taxid: int,
        score: float,
        unclassified_taxids: set[int],
    ) -> Subgroup:
    """
    Update a Subgroup object with new taxid and score information.
    Args:
        subgroup (Subgroup): Subgroup object to update.
        taxid (int): Taxid of new entry.
        score (float): Score of new entry.
        unclassified_taxids (set[int]): Set of unclassified taxids.
    Returns:
        Subgroup: Updated Subgroup object.
    """
    if len(subgroup.taxids) == 0:
        taxids = [taxid]
        taxids_classified = [taxid not in unclassified_taxids]
        scores = [score]
    else:
        taxids = subgroup.taxids + [taxid]
        taxids_classified = subgroup.taxids_classified + [taxid not in unclassified_taxids]
        scores = subgroup.scores + [score]
    # Return updated Subgroup object
    return Subgroup(
        taxids = taxids,
        taxids_classified = taxids_classified,
        scores = scores,
        summary = None,
    )

def summarize_subgroup(
        subgroup: Subgroup,
        child_to_parent: dict[int, int],
        path_cache: dict[int, list[int]],
) -> tuple[Subgroup, dict[int, list[int]]]:
    """
    Summarize a Subgroup object into a dictionary of statistics.
    Args:
        subgroup (Subgroup): Subgroup object to summarize.
        child_to_parent (dict[int, int]): Dictionary mapping each taxid to its parent.
        path_cache (dict[int, list[int]]): Cache of precomputed paths to the root.
    Returns:
        tuple[Subgroup, dict[int, list[int]]]: Tuple containing the summarized
            Subgroup object and the updated path cache.
    """
    if len(subgroup.taxids) == 0:
        subgroup.summary = {
            "min_score": None,
            "max_score": None,
            "mean_score": None,
            "n_entries": 0,
            "n_classified": 0,
            "top_taxid": None,
            "top_taxid_classified": None,
            "lca": None,
        }
        return subgroup, path_cache
    # Summary score statistics
    min_score = min(subgroup.scores)
    max_score = max(subgroup.scores)
    mean_score = sum(subgroup.scores) / len(subgroup.scores)
    # Summary taxid statistics
    n_entries = len(subgroup.taxids)
    n_classified = sum(subgroup.taxids_classified)
    # Top taxid:
    # Out of all taxids with a joint max score, choose the first classified one if any,
    # otherwise choose the first unclassified one
    is_top_taxid = [score == max_score for score in subgroup.scores]
    top_and_classified = [x and y for x, y in zip(is_top_taxid, subgroup.taxids_classified, strict=True)]
    top_taxid_classified = any(top_and_classified)
    if top_taxid_classified:
        top_taxid = subgroup.taxids[top_and_classified.index(True)]
    else:
        top_taxid = subgroup.taxids[is_top_taxid.index(True)]
    # LCA:
    # If top taxid is classified, exclude all unclassified taxids
    # Otherwise, exclude unclassified taxids that are not joint top in score
    taxids_lca = set()
    taxids_lca_zip = zip(subgroup.taxids, subgroup.taxids_classified, is_top_taxid)
    for taxid, classified, is_top in taxids_lca_zip:
        if top_taxid_classified and classified:
            taxids_lca.add(taxid)
        elif not top_taxid_classified:
            if classified or is_top:
                taxids_lca.add(taxid)
    lca, path_cache = find_lca_set(taxids_lca, child_to_parent, path_cache)
    # Return summarized statistics
    subgroup.summary = {
        "min_score": min_score,
        "max_score": max_score,
        "mean_score": mean_score,
        "n_entries": n_entries,
        "n_classified": n_classified,
        "top_taxid": top_taxid,
        "top_taxid_classified": top_taxid_classified,
        "lca": lca,
    }
    return subgroup, path_cache

def output_subgroup(
        subgroup: Subgroup,
        ) -> list[str]:
    """
    Process a Subgroup object into a list of fields for output.
    """
    assert subgroup.summary is not None, "Subgroup has not yet been summarized."
    return [
        output_subgroup_element(subgroup.summary["lca"]),
        output_subgroup_element(subgroup.summary["n_entries"]),
        output_subgroup_element(subgroup.summary["n_classified"]),
        output_subgroup_element(subgroup.summary["top_taxid"]),
        output_subgroup_element(subgroup.summary["top_taxid_classified"]),
        output_subgroup_element(subgroup.summary["min_score"]),
        output_subgroup_element(subgroup.summary["max_score"]),
        output_subgroup_element(subgroup.summary["mean_score"]),
    ]

def output_subgroup_element(
        element: int | float | str | None,
) -> str:
    """
    Convert an element to a string, replacing None with "NA".
    Args:
        element (int | float | str | None): Element to convert to string.
    Returns:
        str: String representation of element, or "NA" if None.
    """
    return str(element) if element is not None else "NA"

#=======================================================================
# Group class and functions
#=======================================================================

@dataclass
class Group:
    """
    A group of entries, including taxids, scores, and the top taxid and LCA,
    divided into natural and artificial subgroups.
    """
    group_id: str
    all: Subgroup
    natural: Subgroup
    artificial: Subgroup

def new_group(
        group_id: str,
        taxid: int,
        score: float,
        artificial_taxids: set[int],
        unclassified_taxids: set[int],
        ) -> Group:
    """
    Create a new Group object from minimal input information.
    Args:
        group_id (str): Group ID.
        taxid (int): Taxid of first entry.
        score (float): Score of first entry.
        artificial_taxids (set[int]): Set of taxids that represent artificial
            or engineered sequences.
        unclassified_taxids (set[int]): Set of taxids that represent unclassified
            sequences.
    Returns:
        Group: New Group object.
    """
    # Determine if first entry is artificial
    is_artificial = taxid in artificial_taxids
    # Initialize all, natural, and artificial subgroups
    subgroup_all = new_subgroup(taxid, score, unclassified_taxids)
    if is_artificial:
        subgroup_natural = new_subgroup(None, None, unclassified_taxids)
        subgroup_artificial = new_subgroup(taxid, score, unclassified_taxids)
    else:
        subgroup_natural = new_subgroup(taxid, score, unclassified_taxids)
        subgroup_artificial = new_subgroup(None, None, unclassified_taxids)
    # Return Group object
    return Group(
        group_id=group_id,
        all=subgroup_all,
        natural=subgroup_natural,
        artificial=subgroup_artificial,
    )

def update_group(
        group: Group,
        taxid: int,
        score: float,
        artificial_taxids: set[int],
        unclassified_taxids: set[int],
        ) -> Group:
    """
    Update a Group object with new taxid and score information.
    Args:
        group (Group): Group object to update.
        taxid (int): Taxid of new entry.
        score (float): Score of new entry.
        artificial_taxids (set[int]): Set of taxids that represent artificial or engineered sequences.
        unclassified_taxids (set[int]): Set of taxids that represent unclassified
            sequences.
    Returns:
        Group: Updated Group object.
    """
    # Determine if new entry is artificial
    is_artificial = taxid in artificial_taxids
    # Update all subgroup
    group.all = update_subgroup(group.all, taxid, score, unclassified_taxids)
    # Update natural and artificial subgroups as appropriate
    if is_artificial:
        group.artificial = update_subgroup(group.artificial, taxid, score, unclassified_taxids)
    else:
        group.natural = update_subgroup(group.natural, taxid, score, unclassified_taxids)
    # Return updated Group object
    return group

def summarize_group(
        group: Group,
        child_to_parent: dict[int, int],
        path_cache: dict[int, list[int]],
        ) -> tuple[Group, dict[int, list[int]]]:
    """
    Summarize each Subgroup object in a Group object into a dictionary of
    statistics.
    Args:
        group (Group): Group object to summarize.
        child_to_parent (dict[int, int]): Dictionary mapping each taxid to its parent.
        path_cache (dict[int, list[int]]): Cache of precomputed paths to the root.
    Returns:
        tuple[Group, dict[int, list[int]]]: Tuple containing the summarized
            Group object and the updated path cache.
    """
    # Summarize subgroups
    group.all, path_cache = summarize_subgroup(group.all, child_to_parent, path_cache)
    group.natural, path_cache = summarize_subgroup(group.natural, child_to_parent, path_cache)
    group.artificial, path_cache = summarize_subgroup(group.artificial, child_to_parent, path_cache)
    # Return summarized Group object
    return group, path_cache

def output_group(
        group: Group,
        ) -> list[str]:
    """
    Process a Group object into a list of fields for output.
    """
    fields_all = output_subgroup(group.all)
    fields_natural = output_subgroup(group.natural)
    fields_artificial = output_subgroup(group.artificial)
    return [group.group_id] + fields_all + fields_natural + fields_artificial

#=======================================================================
# Functions for processing input and output
#=======================================================================

def get_output_header(
        prefix: str,
        group_field: str,
        taxid_field: str,
        score_field: str,
        ) -> list[str]:
    """
    Get the header for the output TSV.
    Args:
        prefix (str): Prefix for output columns.
        group_field (str): Input column header for group field.
        taxid_field (str): Input column header for taxid field.
        score_field (str): Input column header for score field.
    Returns:
        list[str]: Header for the output TSV.
    """
    fields_base = [
        taxid_field + "_lca",
        "n_assignments",
        "n_assignments_classified",
        taxid_field + "_top",
        taxid_field + "_top_classified",
        score_field + "_min",
        score_field + "_max",
        score_field + "_mean",
    ]
    if prefix == "":
        fields_prefixed = fields_base
    else:
        fields_prefixed = [prefix + "_" + f for f in fields_base]
    # Natural fields will be default used by user so we don't add prefix to make the output more readable
    fields_all = [f + "_combined" for f in fields_prefixed]
    fields_artificial = [f + "_artificial" for f in fields_prefixed]
    return [group_field] + fields_all + fields_prefixed + fields_artificial

def write_output_line(
        fields: list[str],
        headers: list[str],
        output_file: IO[str],
        ) -> None:
    """
    Write a line to the output file.
    """
    assert len(fields) == len(headers), \
        f"Number of fields ({len(fields)}) does not match number of headers ({len(headers)})."
    line = "\t".join(fields) + "\n"
    output_file.write(line)

def parse_input_header(
        header_fields: list[str],
        group_field: str,
        taxid_field: str,
        score_field: str,
        ) -> tuple[int, int, int]:
    """
    Parse the header of an input TSV and return the indices of the group,
    taxid, and score fields.
    Args:
        header_fields (list[str]): List of fields from the header of an input TSV.
        group_field (str): Column header for group field.
        taxid_field (str): Column header for taxid field.
        score_field (str): Column header for score field.
    Returns:
        tuple[int, int, int]: Tuple containing the indices of the group,
            taxid, and score fields.
    """
    # Confirm fields are present
    assert group_field in header_fields, f"Group field not found in header: {group_field}"
    assert taxid_field in header_fields, f"Taxid field not found in header: {taxid_field}"
    assert score_field in header_fields, f"Score field not found in header: {score_field}"
    # Return indices
    group_idx = header_fields.index(group_field)
    taxid_idx = header_fields.index(taxid_field)
    score_idx = header_fields.index(score_field)
    return group_idx, taxid_idx, score_idx

def process_input_line(
        fields: list[str],
        group_idx: int,
        taxid_idx: int,
        score_idx: int,
        group_info: Group | None,
        artificial_taxids: set[int],
        unclassified_taxids: set[int],
        ) -> Group:
    """
    Process a single line of an input TSV and return an updated Group object
    containing information about the corresponding entry group.
    Args:
        fields (list[str]): List of fields from an input line.
        group_idx (int): Index of the group field in the input line.
        taxid_idx (int): Index of the taxid field in the input line.
        score_idx (int): Index of the score field in the input line.
        group_info (Group | None): Group object containing information about the
            corresponding entry group.
        artificial_taxids (set[int]): Set of taxids that represent artificial or
            engineered sequences.
    Returns:
        Group: Updated Group object containing information about the corresponding
            entry group.
    """
    logger.debug(f"Processing input line: {fields}")
    logger.debug(f"Group info: {group_info}")
    # Get group ID and confirm match with Group object
    group_id = fields[group_idx]
    if group_info is not None:
        assert group_id == group_info.group_id, \
            f"Group ID mismatch: {group_id} != {group_info.group_id}"
    # Get taxid and score
    taxid = int(fields[taxid_idx])
    score = float(fields[score_idx])
    # If no Group object, create one
    if group_info is None:
        return new_group(group_id, taxid, score, artificial_taxids,
                         unclassified_taxids)
    # Otherwise, update Group object
    return update_group(group_info, taxid, score, artificial_taxids,
                        unclassified_taxids)

def parse_input_tsv(
        input_path: str,
        output_path: str,
        group_field: str,
        taxid_field: str,
        score_field: str,
        child_to_parent: dict[int, int],
        artificial_taxids: set[int],
        unclassified_taxids: set[int],
        prefix: str,
        ) -> None:
    """
    Iterate linewise over input TSV, calculating the LCA for each group
    of entries and writing the result to the output file.
    Args:
        input_path (str): Path to input TSV.
        output_path (str): Path to output TSV.
        group_field (str): Column header for group field.
        taxid_field (str): Column header for taxid field.
        score_field (str): Column header for score field.
        child_to_parent (dict[int, int]): Dictionary mapping each taxid to its parent.
        artificial_taxids (set[int]): Set of taxids that represent artificial or
            engineered sequences.
        unclassified_taxids (set[int]): Set of taxids that represent unclassified
            sequences.
        prefix (str): Prefix for output columns.
    """
    with open_by_suffix(input_path) as inf, open_by_suffix(output_path, "w") as outf:
        # Read header from input file
        header = inf.readline().strip().split("\t")
        logger.debug(f"Header: {header}")
        if len(header) == 1 and header[0] == "":
            logger.warning("Input file is empty (header only). Returning header only.")
            return
        # Write header to output file
        output_header = get_output_header(prefix, group_field, taxid_field, score_field)
        write_output_line(output_header, output_header, outf)
        # Get indices of fields
        group_idx, taxid_idx, score_idx = parse_input_header(
            header, group_field, taxid_field, score_field)
        def parse_line(fields: list[str], group_info: Group | None) -> Group:
            return process_input_line(fields, group_idx, taxid_idx, score_idx, group_info,
                                      artificial_taxids, unclassified_taxids)
        # Check for empty file
        fields = inf.readline().strip().split("\t")
        if len(fields) == 1 and fields[0] == "":
            logger.warning("Input file is empty (header only). Returning header only.")
            return
        # Process first line
        group_id = fields[group_idx]
        group_info = parse_line(fields, None)
        # Iterate over input file
        n_entries = 1
        n_groups = 1
        path_cache: dict[int, list[int]] = {}
        for line in inf:
            # Parse fields
            fields = line.strip().split("\t")
            # Get group ID and check sorting
            group_id_new = fields[group_idx]
            assert group_id_new >= group_id, \
                f"Group ID out of order: {group_id_new} < {group_id}"
            # If group ID is unchanged, process line into existing Group object
            if group_id_new == group_id:
                group_info = parse_line(fields, group_info)
            # Otherwise, write previous Group object to output and start new one
            else:
                group_info, path_cache = summarize_group(group_info, child_to_parent, path_cache)
                out_line = output_group(group_info)
                write_output_line(out_line, output_header, outf)
                group_info = parse_line(fields, None)
                group_id = group_id_new
                n_groups += 1
            n_entries += 1
        # Write last Group object to output
        group_info, path_cache = summarize_group(group_info, child_to_parent, path_cache)
        out_line = output_group(group_info)
        write_output_line(out_line, output_header, outf)
        logger.info(f"Processed {n_entries} entries in {n_groups} groups.")

#=======================================================================
# Taxonomy DB functions
#=======================================================================

def parse_nodes_db(path: str, artificial_taxid: int
                      ) -> tuple[dict[int, int], dict[int, set[int]]]:
    """
    Parse taxonomy DB into two dictionaries: one mapping each taxid
    to its parent taxid, and one mapping each taxid to its children taxids.
    Args:
        path (str): Path to taxonomy DB.
        artificial_taxid (int): Taxid of artificial parent.
    Returns:
        tuple[dict[int, int], dict[int, set[int]]]: Tuple containing the
            child-to-parent dictionary and the parent-to-children dictionary.
    """
    # Define dictionaries
    child_to_parent: dict[int, int] = {}
    parent_to_children: dict[int, set[int]] = defaultdict(set)
    # Read file line by line and parse into dictionaries
    with open_by_suffix(path) as f:
        for line in f:
            fields = line.strip().split("\t")
            taxid = int(fields[0])
            parent_taxid = int(fields[2])
            child_to_parent[taxid] = parent_taxid
            parent_to_children[parent_taxid].add(taxid)
    # Check that DB contains root as the topmost taxid
    assert TAXID_ROOT in child_to_parent and TAXID_ROOT in parent_to_children, \
        "Taxonomy DB does not contain root."
    assert child_to_parent[TAXID_ROOT] == TAXID_ROOT, "Root taxid has a parent."
    assert TAXID_ROOT in parent_to_children[TAXID_ROOT], "Root taxid must be its own child."
    # Check that DB contains artificial parent taxid
    assert artificial_taxid in child_to_parent, \
        f"Artificial parent taxid not found in child-to-parent DB: {artificial_taxid}, {child_to_parent}"
    assert artificial_taxid in parent_to_children, \
        f"Artificial parent taxid not found in parent-to-children DB: {artificial_taxid}, {parent_to_children}"
    # Return dictionaries
    return child_to_parent, parent_to_children

def get_descendants(taxids: set[int], parent_to_children: dict[int, set[int]]) -> set[int]:
    """
    Get a set of all descendants of a taxid.
    Args:
        taxids (set[int]): Set of taxids to get descendants of.
        parent_to_children (dict[int, set[int]]): Dictionary mapping each taxid
            to its children taxids.
    Returns:
        set[int]: Set of all descendants of the taxid, including the taxid itself.
    """
    descendants = taxids
    descendants_new = set()
    for taxid in descendants:
        descendants_new.update(parent_to_children[taxid])
    while descendants_new:
        descendants.update(descendants_new)
        descendants_newer = set()
        for taxid in descendants_new:
            descendants_newer.update(parent_to_children[taxid])
        # Remove any children already in set
        descendants_newer = descendants_newer - descendants
        # Update set of children
        descendants_new = descendants_newer
    # Return set of descendants
    return descendants

def parse_names_db(path: str) -> dict[int, set[str]]:
    """
    Parse taxonomy names DB into a dictionary mapping each taxid to a
    set of names.
    Args:
        path (str): Path to taxonomy names DB.
    Returns:
        dict[int, set[str]]: Dictionary mapping each taxid to a set of names.
    """
    names_db: dict[int, set[str]] = defaultdict(set)
    with open_by_suffix(path) as f:
        for line in f:
            fields = line.strip().split("\t")
            taxid = int(fields[0])
            name = fields[2]
            names_db[taxid].add(name)
    # Return dictionary
    return names_db

def get_unclassified_taxids(names_db: dict[int, set[str]]) -> set[int]:
    """
    Get a set of taxids that are unclassified in the taxonomy names DB.
    Args:
        names_db (dict[int, set[str]]): Dictionary mapping each taxid to a
            set of names.
    Returns:
        set[int]: Set of taxids that are unclassified (i.e. map to a name
            containing "unclassified" or "sp.")
    """
    unclassified_taxids = set()
    for taxid, names in names_db.items():
        for name in names:
            name_lower = name.lower()
            if "unclassified" in name_lower or " sp." in name_lower:
                unclassified_taxids.add(taxid)
                break
    # Return set of unclassified taxids
    return unclassified_taxids

#=======================================================================
# I/O functions
#=======================================================================

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    # Create parser
    desc = "Given a sorted TSV containing group, taxid, and score columns, " \
           "return a TSV with a single row per group containing the lowest " \
           "common ancestor taxid across all taxids in the group."
    parser = argparse.ArgumentParser(description=desc)
    # Add arguments
    parser.add_argument("--input", "-i", help="Path to input TSV.")
    parser.add_argument("--output", "-o", help="Path to output TSV.")
    parser.add_argument("--nodes-db", "-d", help="Path to taxonomy nodes DB (raw NCBI nodes.dmp file).")
    parser.add_argument("--names-db", "-n", help="Path to taxonomy names DB (raw NCBI names.dmp file).")
    parser.add_argument("--group", "-g", help="Column header for group field.")
    parser.add_argument("--taxid", "-t", help="Column header for taxid field.")
    parser.add_argument("--score", "-s", help="Column header for score field.")
    parser.add_argument("--artificial", "-a",
                        help="Parent taxid for artificial sequences (to be handled separately).")
    parser.add_argument("--prefix", "-p", default="", help="Column prefix for output columns.")
    # Return parsed arguments
    return parser.parse_args()

def open_by_suffix(filename: str, mode: str = "r", debug: bool = False) -> IO[str]:
    """Parse the suffix of a filename to determine the right open method
    to use, then open the file. Can handle .gz and uncompressed files."""
    if filename.endswith('.gz'):
        return cast(IO[str], gzip.open(filename, mode + 't'))
    else:
        return open(filename, mode)

#=======================================================================
# Main function
#=======================================================================

def main() -> None:
    logger.info("Initializing script.")
    # Parse arguments
    logger.info("Parsing arguments.")
    args = parse_args()
    # Import taxonomy DB and process into dictionaries
    logger.info("Parsing taxonomy DB.")
    child_to_parent, parent_to_children = parse_nodes_db(args.nodes_db, int(args.artificial))
    logger.info(f"Parsed taxonomy information for {len(child_to_parent)} taxids.")
    # Parse names DB and get set of unclassified taxids
    logger.info("Parsing taxonomy names DB.")
    names_db = parse_names_db(args.names_db)
    unclassified_taxids = get_unclassified_taxids(names_db)
    logger.info(f"Found {len(unclassified_taxids)} unclassified taxids.")
    # Get taxids descended from unclassified taxids
    logger.info("Getting taxids descended from unclassified taxids.")
    unclassified_taxids_descendants = get_descendants(unclassified_taxids, parent_to_children)
    logger.info(f"Found {len(unclassified_taxids_descendants)} taxids descended from unclassified taxids.")
    # Get set of artificial taxids
    logger.info("Getting set of artificial taxids.")
    artificial_taxids = get_descendants(set([int(args.artificial)]), parent_to_children)
    logger.info(f"Found {len(artificial_taxids)} artificial taxids.")
    # Parse input TSV and write LCA information to output TSV
    logger.info("Parsing input TSV.")
    parse_input_tsv(args.input, args.output, args.group, args.taxid, args.score,
                    child_to_parent, artificial_taxids, unclassified_taxids_descendants,
                    args.prefix)
    # Log completion
    logger.info("Script complete.")

if __name__ == "__main__":
    main()
