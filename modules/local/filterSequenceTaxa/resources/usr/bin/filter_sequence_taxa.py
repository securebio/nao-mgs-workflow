#!/usr/bin/env python
"""Drop rows whose taxid falls under an excluded taxonomic clade from a viral
genome metadata TSV.

Used to exclude influenza (Orthomyxoviridae, taxid 11308 by default) from the
sequence-sourcing branch of ENUMERATE_VIRAL_ACCESSIONS: NCBI keeps flu on
grouped genome assemblies (which the assembly branch captures), so the sequence
branch would otherwise re-add thousands of ungrouped flu segments. Exclusion is
by taxonomic descent from a configurable root, computed from the NCBI nodes.dmp.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import logging
import time
from collections import deque
from datetime import UTC, datetime
from typing import IO, cast

import pandas as pd

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created, UTC).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(UTCFormatter("[%(asctime)s] %(message)s"))
logger.handlers.clear()
logger.addHandler(handler)

#############
# FUNCTIONS #
#############


def open_by_suffix(path: str, mode: str = "r") -> IO[str]:
    """Open a file, transparently handling .gz compression (text mode)."""
    if path.endswith(".gz"):
        return cast(IO[str], gzip.open(path, mode + "t"))
    return cast(IO[str], open(path, mode))


def read_parent_map(nodes_path: str) -> dict[str, str]:
    """Read an NCBI nodes.dmp into a taxid -> parent_taxid mapping.

    Args:
        nodes_path: Path to NCBI `nodes.dmp`. Fields are separated by `\\t|\\t`,
            so tab-splitting places taxid at column 0 and parent_taxid at
            column 2 (the raw positional convention used by BUILD_VIRUS_TAXID_DB).

    Returns:
        Dictionary mapping each taxid to its parent taxid.
    """
    nodes = (
        pd.read_csv(nodes_path, sep="\t", dtype=str, header=None)
        .iloc[:, [0, 2]]
        .rename(columns={0: "taxid", 2: "parent_taxid"})
    )
    return dict(zip(nodes["taxid"], nodes["parent_taxid"], strict=True))


def descendant_taxids(parent_of: dict[str, str], root: str) -> set[str]:
    """Return the inclusive set of taxids descending from `root`.

    Builds a parent -> children adjacency from the taxid -> parent map and
    breadth-first-searches from `root`. Cycle-safe: a self-parent edge (as NCBI
    gives the tree root, taxid 1) is skipped so it cannot loop.

    Args:
        parent_of: Mapping of taxid to parent taxid.
        root: Taxid whose clade (itself plus all descendants) to collect.

    Returns:
        Set of taxids in the clade rooted at `root` (includes `root`).
    """
    children: dict[str, list[str]] = {}
    for taxid, parent in parent_of.items():
        if taxid == parent:
            continue  # skip self-parent edge (e.g. NCBI root 1) to avoid cycles
        children.setdefault(parent, []).append(taxid)
    result: set[str] = set()
    queue: deque[str] = deque([root])
    while queue:
        taxid = queue.popleft()
        if taxid in result:
            continue
        result.add(taxid)
        queue.extend(children.get(taxid, []))
    return result


def filter_sequence_taxa(
    meta_db: pd.DataFrame, exclude_taxids: set[str]
) -> pd.DataFrame:
    """Drop metadata rows whose `taxid` is in the excluded clade.

    Args:
        meta_db: Viral metadata table (must include a `taxid` column).
        exclude_taxids: Taxids to drop (a clade's inclusive descendant set).

    Returns:
        Metadata with excluded-clade rows removed, column order preserved.
    """
    before = len(meta_db)
    kept = meta_db.loc[~meta_db["taxid"].isin(exclude_taxids)]
    logger.info(
        "Dropped %d rows under the excluded clade (%d -> %d).",
        before - len(kept),
        before,
        len(kept),
    )
    return kept


########
# MAIN #
########


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metadata", help="Path to viral metadata TSV (may be .gz).")
    parser.add_argument("nodes_dmp", help="Path to NCBI nodes.dmp.")
    parser.add_argument(
        "exclude_taxid", help="Root taxid of the clade to exclude (e.g. 11308 = flu)."
    )
    parser.add_argument("output", help="Output path for the filtered metadata TSV.")
    return parser.parse_args()


def main() -> None:
    start_time = time.time()
    logger.info("Starting filter_sequence_taxa.")
    args = parse_arguments()
    parent_of = read_parent_map(args.nodes_dmp)
    # Fail loudly on a bad exclude_taxid: if it isn't a known taxid, the clade
    # would be a silent no-op (dropping nothing) despite a plausible log line.
    known_taxids = set(parent_of) | set(parent_of.values())
    if args.exclude_taxid not in known_taxids:
        raise ValueError(
            f"exclude_taxid {args.exclude_taxid!r} not found in nodes.dmp; "
            "refusing to run a no-op exclusion (check the taxid)."
        )
    exclude_taxids = descendant_taxids(parent_of, args.exclude_taxid)
    logger.info(
        "Excluding %d taxids in the clade rooted at %s.",
        len(exclude_taxids),
        args.exclude_taxid,
    )
    with open_by_suffix(args.metadata) as f:
        meta_db = pd.read_csv(f, sep="\t", dtype=str)
    kept = filter_sequence_taxa(meta_db, exclude_taxids)
    kept.to_csv(args.output, sep="\t", index=False)
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
