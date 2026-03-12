#!/usr/bin/env python
"""Enumerate direct child taxa of a given parent taxid from an NCBI taxonomy
nodes.dmp file. If the parent taxid is a leaf (no children), the parent
taxid itself is emitted.
"""

import argparse
import logging
import time
from datetime import UTC, datetime

class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(UTCFormatter("[%(asctime)s] %(message)s"))
logger.handlers.clear()
logger.addHandler(handler)

def enumerate_children(nodes_path: str, parent_taxid: str) -> list[str]:
    """Find direct child taxids of a given parent in nodes.dmp.
    Args:
        nodes_path: Path to NCBI taxonomy nodes.dmp file.
        parent_taxid: The parent taxid to find children for.
    Returns:
        List of child taxids. If the parent is a leaf taxon (no children),
        returns a list containing only the parent taxid.
    """
    children = []
    with open(nodes_path) as f:
        for line in f:
            fields = line.strip().split("\t|\t")
            child_id = fields[0].strip()
            parent_id = fields[1].strip()
            if parent_id == parent_taxid and child_id != parent_taxid:
                children.append(child_id)
    if not children:
        children = [parent_taxid]
    return children

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("nodes_dmp", help="Path to NCBI taxonomy nodes.dmp file.")
    parser.add_argument("parent_taxid", help="Parent taxid to find children for.")
    parser.add_argument("output", help="Output file path for child taxids (one per line).")
    return parser.parse_args()

def main() -> None:
    start_time = time.time()
    logger.info("Starting enumerate_child_taxa.")
    args = parse_arguments()
    children = enumerate_children(args.nodes_dmp, args.parent_taxid)
    logger.info("Found %d child taxa for parent %s", len(children), args.parent_taxid)
    with open(args.output, "w") as f:
        for taxid in children:
            f.write(taxid + "\n")
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)

if __name__ == "__main__":
    main()
