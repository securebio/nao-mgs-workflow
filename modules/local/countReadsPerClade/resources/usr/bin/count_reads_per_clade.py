#!/usr/bin/env python3
"""Generate clade counts for a taxonomic tree.

Take a table of reads with LCA assignments and a table of (child, parent) taxid pairs
and output a table of taxids with counts of reads that are directly assigned to
the taxid and all reads that are assigned to the clade descended from the taxid.
Output both deduplicated and total (non-deduplicated) counts.
"""

import argparse
import bz2
import csv
import gzip
import sys
from collections import Counter, defaultdict
from collections.abc import Iterator
from typing import IO, cast

TaxId = int
# NCBI taxonomy root node - has itself as parent
ROOT: TaxId = 1
# Tree as adjacency list mapping parents to children
Tree = defaultdict[TaxId, set[TaxId]]


def open_by_suffix(filename: str, mode: str = "r") -> IO[str]:
    """Parse the suffix of a filename to determine the open method, then open the file.

    Can handle .gz, .bz2, and uncompressed files.
    """
    if filename.endswith(".gz"):
        return cast(IO[str], gzip.open(filename, mode + "t"))
    if filename.endswith(".bz2"):
        return cast(IO[str], bz2.open(filename, mode + "t"))
    return open(filename, mode)


def read_tsv(file_path: str) -> Iterator[dict[str, str]]:
    """Read a TSV file and yield rows one at a time.

    Args:
        file_path (str): Path to the TSV file

    Yields:
        dict: Dictionary representing each row

    """
    with open_by_suffix(file_path, mode="rt") as file:
        reader = csv.DictReader(file, delimiter="\t")
        yield from reader


def is_duplicate(read: dict[str, str]) -> bool:
    """Check if a read is a duplicate.

    A duplicate read is one where sequence ID != primary alignment exemplar.

    Args:
        read: Dictionary representing a read record
              Must contain 'seq_id' and 'prim_align_dup_exemplar' fields

    Returns:
        True if the read is a duplicate (seq_id differs from prim_align_dup_exemplar)
        False otherwise

    Raises:
        KeyError: if 'seq_id' or 'prim_align_dup_exemplar' fields are missing

    """
    return read["seq_id"] != read["prim_align_dup_exemplar"]


def count_direct_reads_per_taxid(
    data: Iterator[dict[str, str]],
    group: str,
    taxid_field: str = "aligner_taxid_lca",
    group_field: str = "group",
) -> tuple[Counter[TaxId], Counter[TaxId]]:
    """Count total and deduplicated reads per taxonomic ID, validating group.

    These are reads assigned directly to the tax ID, not including descendent counts.

    Args:
        data: Iterator of read records as dictionaries
        group: Expected group identifier for validation
        taxid_field: Field name containing the taxonomic ID
        group_field: Field name containing the group

    Returns:
        Tuple of (total_counts, deduplicated_counts) as Counters

    """
    total: Counter[TaxId] = Counter()
    dedup: Counter[TaxId] = Counter()
    for read in data:
        read_group = read[group_field]
        assert read_group == group, f"Expected group '{group}', found '{read_group}'"
        taxid = int(read[taxid_field])
        total[taxid] += 1
        if not is_duplicate(read):
            dedup[taxid] += 1
    return total, dedup


def build_tree(
    tax_data: Iterator[dict[str, str]],
    child_field: str = "taxid",
    parent_field: str = "parent_taxid",
) -> Tree:
    """Build a taxonomic tree from taxonomy data.

    Args:
        tax_data: Iterator of taxonomy records as dictionaries
        child_field: Field name containing child taxonomic ID
        parent_field: Field name containing parent taxonomic ID

    Returns:
        Dictionary mapping parent IDs to sets of child IDs

    """
    tree = defaultdict(set)
    children = set()
    for taxon in tax_data:
        child = int(taxon[child_field])
        parent = int(taxon[parent_field])
        if child in children:
            msg = f"Child taxid {child} appears multiple times in taxdb"
            raise ValueError(msg)
        children.add(child)
        # Handle NCBI root: ROOT has itself as parent, don't add it as a child of itself
        if not (child == ROOT and parent == ROOT):
            tree[parent].add(child)
    if detect_cycle(tree):
        msg = "Cycle detected in taxdb"
        raise ValueError(msg)
    return tree


def detect_cycle(tree: Tree) -> bool:
    """Return True if the tree contains a cycle, False otherwise."""
    visited: set[TaxId] = set()
    current_path: set[TaxId] = set()

    # Visit all the nodes, using depth-first search
    def dfs(node: TaxId) -> bool:
        if node in current_path:
            return True
        if node in visited:
            return False
        visited.add(node)
        current_path.add(node)
        if node in tree:
            for child in tree[node]:
                if dfs(child):
                    return True
        current_path.remove(node)
        return False

    return any(dfs(node) for node in tree)


def parents(tree: Tree) -> set[TaxId]:
    """Get all parent nodes from a tree."""
    return set(tree.keys())


def children(tree: Tree) -> set[TaxId]:
    """Get all child nodes from a tree."""
    return set.union(*tree.values()) if tree else set()


def nodes(tree: Tree) -> set[TaxId]:
    """Get all nodes (parents and children) from a tree."""
    return parents(tree) | children(tree)


def roots(tree: Tree) -> set[TaxId]:
    """Find root nodes of a tree (parent nodes that are not also child nodes)."""
    return parents(tree) - children(tree)


def get_clade_counts(direct_counts: Counter[TaxId], tree: Tree) -> Counter[TaxId]:
    """Aggregate read counts for each clade (node and all its descendants).

    Args:
        direct_counts: Counter of directly-assigned reads per taxonomic ID
        tree: Taxonomic tree structure

    Returns:
        Counter mapping taxonomic IDs to their clade counts

    """
    clade_counts: Counter[TaxId] = Counter()

    # Depth-first search of the tree, store results as you go
    def dfs(node: TaxId) -> int:
        # Start with this node's own count
        total = direct_counts[node]

        # Add counts from all descendants
        for child in tree[node]:
            total += dfs(child)

        clade_counts[node] = total
        return total

    for root in roots(tree):
        dfs(root)

    return clade_counts


def write_output_tsv(
    output_path: str,
    group: str,
    tree: Tree,
    direct_counts_total: Counter[TaxId],
    direct_counts_dedup: Counter[TaxId],
    clade_counts_total: Counter[TaxId],
    clade_counts_dedup: Counter[TaxId],
) -> None:
    """Write taxonomic read counts to a TSV file.

    Args:
        output_path: Path to output TSV file
        group: Group identifier to include in output
        tree: Taxonomic tree structure
        direct_counts_total: Total directly assigned read counts per taxonomic ID
        direct_counts_dedup: Deduplicated directly assigned read counts per taxonomic ID
        clade_counts_total: Total clade counts per taxonomic ID
        clade_counts_dedup: Deduplicated clade counts per taxonomic ID

    """
    with open_by_suffix(output_path, "w") as outfile:
        fieldnames = [
            "group",
            "taxid",
            "parent_taxid",
            "reads_direct_total",
            "reads_direct_dedup",
            "reads_clade_total",
            "reads_clade_dedup",
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()

        # Write rows in depth-first order
        # If a node does not have a parent, set it to be ROOT: the root of the
        # NCBI taxonomy
        def dfs(node: TaxId, parent: TaxId = ROOT) -> None:
            clade_total = clade_counts_total[node]
            row: dict[str, str | int] = {
                "group": group,
                "taxid": node,
                "parent_taxid": parent,
                "reads_direct_total": direct_counts_total[node],
                "reads_direct_dedup": direct_counts_dedup[node],
                "reads_clade_total": clade_total,
                "reads_clade_dedup": clade_counts_dedup[node],
            }
            # Only print clades that have some reads
            if clade_total > 0:
                writer.writerow(row)
            for child in sorted(tree[node]):
                dfs(child, parent=node)

        for root in sorted(roots(tree)):
            dfs(root)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reads", help="Path to read TSV with LCA assignments.")
    parser.add_argument(
        "--taxdb",
        help="Path to taxonomy database with taxid and parent_taxid.",
    )
    parser.add_argument("--output", help="Path to output TSV.")
    parser.add_argument(
        "--group",
        help=(
            "Group identifier. "
            "The `group` column of the read TSV must match this in every row."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        direct_counts_total, direct_counts_dedup = count_direct_reads_per_taxid(
            read_tsv(args.reads), args.group
        )
    except KeyError as e:
        missing_column = e.args[0]
        print(
            f"Error: Missing required column '{missing_column}' "
            f"in reads file: {args.reads}",
            file=sys.stderr,
        )
        print(
            "Required columns for reads file: "
            "seq_id, prim_align_dup_exemplar, aligner_taxid_lca, group",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        tree = build_tree(read_tsv(args.taxdb))
    except KeyError as e:
        missing_column = e.args[0]
        print(
            f"Error: Missing required column '{missing_column}' "
            f"in taxonomy file: {args.taxdb}",
            file=sys.stderr,
        )
        print(
            "Required columns for taxonomy file: taxid, parent_taxid", file=sys.stderr
        )
        sys.exit(1)

    clade_counts_total = get_clade_counts(direct_counts_total, tree)
    clade_counts_dedup = get_clade_counts(direct_counts_dedup, tree)
    write_output_tsv(
        args.output,
        args.group,
        tree,
        direct_counts_total,
        direct_counts_dedup,
        clade_counts_total,
        clade_counts_dedup,
    )


if __name__ == "__main__":
    main()
