#!/usr/bin/env python3
DESC = """
Partition the taxonomic subtree rooted at a given taxid into a set of
disjoint subtrees, each containing no more than --max-size taxa.

The partition is computed top-down: starting at the root, if the subtree
size is at or below the threshold the root is emitted as a single segment,
otherwise the algorithm recurses into each child. The result is the
shallowest (and unique) set of subtree roots covering the input subtree
under the size constraint.

The number of taxa in a subtree is used as a proxy for the number of NCBI
genome assemblies it contains. The proxy is conservative for viruses
because strain-level variants typically have their own taxids
(e.g. ~141k Influenza A assemblies are spread across thousands of subtype
and strain taxids under 2955291).

If the input root has no children in nodes.dmp it is emitted unchanged,
matching the previous behavior of `enumerate_child_taxa.py`.
"""

###########
# IMPORTS #
###########

import argparse
import logging
import time
from collections import defaultdict
from datetime import UTC, datetime

###########
# LOGGING #
###########

class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
        return datetime.fromtimestamp(record.created, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(UTCFormatter("[%(asctime)s] %(message)s"))
logger.handlers.clear()
logger.addHandler(handler)

#############
# PARTITION #
#############

def build_child_map(nodes_path: str) -> dict[str, list[str]]:
    """Read nodes.dmp and return a parent -> [children] mapping.
    Args:
        nodes_path: Path to NCBI taxonomy nodes.dmp file.
    Returns:
        Mapping from parent taxid to list of direct child taxids,
        with self-references (root) omitted.
    """
    children: dict[str, list[str]] = defaultdict(list)
    with open(nodes_path) as f:
        for line in f:
            fields = line.strip().split("\t|\t")
            if len(fields) < 2:
                continue
            child_id = fields[0].strip()
            parent_id = fields[1].strip()
            if child_id != parent_id:
                children[parent_id].append(child_id)
    return children

def compute_subtree_sizes(children: dict[str, list[str]], root: str) -> dict[str, int]:
    """Compute the number of taxa in each subtree rooted at a node within
    the subtree of `root` (inclusive of the node itself).
    Iterative post-order traversal to avoid Python recursion limits on
    deep taxonomies.
    Args:
        children: Parent -> [children] mapping.
        root: Root taxid of the subtree to size.
    Returns:
        Mapping from taxid to size of its subtree (>=1).
    """
    sizes: dict[str, int] = {}
    # (node, expanded?) — when first popped, push back as expanded
    # then push children; on the second pop, all children are sized.
    stack: list[tuple[str, bool]] = [(root, False)]
    while stack:
        node, expanded = stack.pop()
        if expanded:
            sizes[node] = 1 + sum(sizes[c] for c in children.get(node, []))
        else:
            stack.append((node, True))
            for c in children.get(node, []):
                stack.append((c, False))
    return sizes

def partition_subtree(children: dict[str, list[str]], sizes: dict[str, int],
                      root: str, max_size: int) -> list[str]:
    """Find the shallowest set of subtree roots covering `root` such that
    every emitted subtree has at most `max_size` taxa.
    Args:
        children: Parent -> [children] mapping.
        sizes: Subtree sizes from compute_subtree_sizes.
        root: Root taxid to partition.
        max_size: Maximum subtree size per emitted segment.
    Returns:
        List of taxids; each is the root of a segment whose subtree size
        does not exceed max_size.
    """
    segments: list[str] = []
    stack: list[str] = [root]
    while stack:
        node = stack.pop()
        if sizes[node] <= max_size:
            segments.append(node)
        else:
            stack.extend(children.get(node, []))
    return segments

def partition(nodes_path: str, root: str, max_size: int) -> list[str]:
    """Top-level entry point: read nodes.dmp and partition the subtree.
    Args:
        nodes_path: Path to NCBI taxonomy nodes.dmp file.
        root: Root taxid of the subtree to partition.
        max_size: Maximum subtree size per emitted segment.
    Returns:
        List of segment-root taxids. If the root has no entries in
        nodes.dmp, returns [root] as a single segment.
    """
    children = build_child_map(nodes_path)
    if root not in children and not any(root in v for v in children.values()):
        return [root]
    sizes = compute_subtree_sizes(children, root)
    return partition_subtree(children, sizes, root, max_size)

##########
# DRIVER #
##########

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument("nodes_dmp", help="Path to NCBI taxonomy nodes.dmp file.")
    parser.add_argument("root_taxid", help="Root taxid to partition.")
    parser.add_argument("output", help="Output file path (one taxid per line).")
    parser.add_argument("--max-size", type=int, required=True,
                        help="Maximum subtree size (taxa count) per emitted segment.")
    return parser.parse_args()

def main() -> None:
    start_time = time.time()
    logger.info("Starting partition_taxon_subtree.")
    args = parse_arguments()
    if args.max_size < 1:
        raise ValueError(f"--max-size must be >= 1 (got {args.max_size})")
    segments = partition(args.nodes_dmp, args.root_taxid, args.max_size)
    logger.info("Partitioned subtree of %s into %d segments (max-size=%d).",
                args.root_taxid, len(segments), args.max_size)
    with open(args.output, "w") as f:
        for taxid in segments:
            f.write(taxid + "\n")
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)

if __name__ == "__main__":
    main()
