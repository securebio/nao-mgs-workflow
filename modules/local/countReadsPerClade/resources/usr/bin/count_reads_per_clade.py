#!/usr/bin/env python3
"""Generate clade counts for a taxonomic tree.

Take a table of reads with LCA assignments and a table of (child, parent) taxid pairs
and output a table of taxids with counts of reads that are directly assigned to
the taxid and all reads that are assigned to the clade descended from the taxid.
Output both deduplicated and total (non-deduplicated) counts.

Clade counts are computed by propagating each directly-assigned taxid's counts
*upward* through its ancestor chain, so the work scales with the number of taxids
the sample actually hit rather than the size of the taxonomy (which is ~2.5M nodes
for the full NCBI taxonomy).
"""

import argparse
import csv
import gzip
import sys
from collections import Counter, defaultdict
from collections.abc import Iterator
from typing import IO, cast

TaxId = int
# NCBI taxonomy root node - has itself as parent
ROOT: TaxId = 1
# Mapping from each taxid to its parent taxid
ParentMap = dict[TaxId, TaxId]
# Sparse parent -> children adjacency, holding only nodes touched by the sample
SparseTree = defaultdict[TaxId, set[TaxId]]


def open_by_suffix(filename: str, mode: str = "r") -> IO[str]:
    """Parse the suffix of a filename to determine the open method, then open the file.

    Can handle .gz and uncompressed files.
    """
    if filename.endswith(".gz"):
        return cast(IO[str], gzip.open(filename, mode + "t"))
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


def build_parent_map(
    tax_data: Iterator[dict[str, str]],
    child_field: str = "taxid",
    parent_field: str = "parent_taxid",
) -> ParentMap:
    """Build a child -> parent lookup from taxonomy data.

    Unlike a parent -> children adjacency, this flat map lets us reconstruct any
    taxid's lineage on demand by following parent pointers, which is all the clade
    counting needs. It is cheap to build and trivially serializable.

    Args:
        tax_data: Iterator of taxonomy records as dictionaries
        child_field: Field name containing child taxonomic ID
        parent_field: Field name containing parent taxonomic ID

    Returns:
        Dictionary mapping each taxid to its parent taxid

    Raises:
        ValueError: if a child taxid appears more than once
        KeyError: if a required column is missing

    """
    parent_map: ParentMap = {}
    for taxon in tax_data:
        child = int(taxon[child_field])
        parent = int(taxon[parent_field])
        if child in parent_map:
            msg = f"Child taxid {child} appears multiple times in taxdb"
            raise ValueError(msg)
        parent_map[child] = parent
    return parent_map


def ancestors(taxid: TaxId, parent_map: ParentMap) -> Iterator[TaxId]:
    """Yield a taxid followed by each of its ancestors up to (and including) its root.

    The walk stops at a root, identified either as a node that is its own parent
    (the NCBI convention, e.g. taxid 1) or a node with no parent entry of its own.
    Cycle-safety comes for free: revisiting a node mid-walk means the lineage loops.

    Args:
        taxid: Taxid to start from. Must be present in the taxonomy (a key or a
            value of parent_map); callers should drop off-tree taxids beforehand.
        parent_map: Child -> parent lookup

    Yields:
        The taxid itself, then its parent, grandparent, ... up to the root

    Raises:
        ValueError: if a cycle is detected in the lineage

    """
    node = taxid
    seen: set[TaxId] = set()
    while True:
        yield node
        seen.add(node)
        parent = parent_map.get(node)
        # A self-parent (e.g. ROOT) or a node with no parent row is a root: stop.
        if parent is None or parent == node:
            return
        if parent in seen:
            msg = "Cycle detected in taxdb"
            raise ValueError(msg)
        node = parent


def accumulate_clade_counts(
    direct_total: Counter[TaxId],
    direct_dedup: Counter[TaxId],
    parent_map: ParentMap,
) -> tuple[Counter[TaxId], Counter[TaxId], SparseTree, int]:
    """Propagate direct per-taxid counts upward into clade counts.

    For each directly-assigned taxid, walk its ancestor chain and add its counts to
    every node on the way to the root. This touches only nodes on a path from an
    observed taxid up to a root, so cost scales with the data, not the taxonomy.

    Reads assigned to taxids absent from the taxonomy are dropped (and tallied),
    matching the documented behaviour of the module.

    Args:
        direct_total: Total directly-assigned read counts per taxid
        direct_dedup: Deduplicated directly-assigned read counts per taxid
        parent_map: Child -> parent lookup

    Returns:
        Tuple of (clade_total, clade_dedup, sparse_tree, dropped_reads) where
        sparse_tree is the parent -> children adjacency restricted to touched nodes
        and dropped_reads is the number of (total) reads on off-tree taxids.

    """
    valid_nodes = set(parent_map.keys()) | set(parent_map.values())
    clade_total: Counter[TaxId] = Counter()
    clade_dedup: Counter[TaxId] = Counter()
    sparse_tree: SparseTree = defaultdict(set)
    dropped_reads = 0

    for taxid, n_total in direct_total.items():
        if taxid not in valid_nodes:
            dropped_reads += n_total
            continue
        n_dedup = direct_dedup.get(taxid, 0)
        child: TaxId | None = None
        for node in ancestors(taxid, parent_map):
            clade_total[node] += n_total
            clade_dedup[node] += n_dedup
            if child is not None:
                # `node` is the parent of the previously-yielded `child`
                sparse_tree[node].add(child)
            child = node

    return clade_total, clade_dedup, sparse_tree, dropped_reads


def write_output_tsv(
    output_path: str,
    group: str,
    parent_map: ParentMap,
    sparse_tree: SparseTree,
    direct_counts_total: Counter[TaxId],
    direct_counts_dedup: Counter[TaxId],
    clade_counts_total: Counter[TaxId],
    clade_counts_dedup: Counter[TaxId],
) -> None:
    """Write taxonomic read counts to a TSV file.

    Rows are emitted in sorted pre-order over the sparse tree of touched nodes,
    which reproduces the ordering of a sorted pre-order traversal of the full
    taxonomy restricted to nodes with a non-zero clade count.

    Args:
        output_path: Path to output TSV file
        group: Group identifier to include in output
        parent_map: Child -> parent lookup
        sparse_tree: Parent -> children adjacency restricted to touched nodes
        direct_counts_total: Total directly assigned read counts per taxonomic ID
        direct_counts_dedup: Deduplicated directly assigned read counts per taxonomic ID
        clade_counts_total: Total clade counts per taxonomic ID
        clade_counts_dedup: Deduplicated clade counts per taxonomic ID

    """
    # Every touched node has a positive clade total, and the roots of the sparse
    # tree are the touched nodes whose parent is a root (self-parent or absent).
    touched = set(clade_counts_total)
    sparse_roots = {
        node
        for node in touched
        if parent_map.get(node) is None or parent_map[node] == node
    }

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

        # Write rows in depth-first order.
        # If a node has no parent in the taxonomy, report it as ROOT.
        def dfs(node: TaxId) -> None:
            row: dict[str, str | int] = {
                "group": group,
                "taxid": node,
                "parent_taxid": parent_map.get(node, ROOT),
                "reads_direct_total": direct_counts_total[node],
                "reads_direct_dedup": direct_counts_dedup[node],
                "reads_clade_total": clade_counts_total[node],
                "reads_clade_dedup": clade_counts_dedup[node],
            }
            writer.writerow(row)
            for child in sorted(sparse_tree[node]):
                dfs(child)

        for root in sorted(sparse_roots):
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
        parent_map = build_parent_map(read_tsv(args.taxdb))
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

    clade_counts_total, clade_counts_dedup, sparse_tree, dropped_reads = (
        accumulate_clade_counts(direct_counts_total, direct_counts_dedup, parent_map)
    )
    if dropped_reads:
        print(
            f"Warning: {dropped_reads} read(s) assigned to taxids not present in the "
            "taxonomy were not counted.",
            file=sys.stderr,
        )

    write_output_tsv(
        args.output,
        args.group,
        parent_map,
        sparse_tree,
        direct_counts_total,
        direct_counts_dedup,
        clade_counts_total,
        clade_counts_dedup,
    )


if __name__ == "__main__":
    main()
