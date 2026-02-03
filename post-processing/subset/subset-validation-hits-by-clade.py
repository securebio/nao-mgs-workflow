#!/usr/bin/env python3

"""
Given validation_hits.tsv on stdin, filter to records with LCA assignments
within one of the provided target clades.

Usage:
   subset-validation-hits-by-clade.py \
      <in> <out> <index> <taxid1> [taxid2 [taxid3 ...]]

Example, limiting to Polio 1, 2, and 3:

    ./subset-validation-hits-by-clade.py \
        123456_validation_hits.tsv.gz | \
        123456_validation_hits_filtered.tsv.gz | \
        ~/index/index.20250825.taxonomy-nodes.dmp \
        12080 12083 12086
"""

import sys
import gzip
from collections import defaultdict

def parse_taxonomy_file(index_fname):
    """Parse the taxonomy nodes file and build parent-child mappings."""
    children = defaultdict(set)  # parent_id -> {child_ids}

    with open(index_fname, 'r') as f:
        for line in f:
            parts = [p.strip() for p in line.split('|')]
            node_id = parts[0]
            parent_id = parts[1]

            children[parent_id].add(node_id)

    return children

def get_descendants(node_id, children, descendants):
    """Build list of all taxids at or below the provided level"""
    descendants.add(node_id)
    for child_id in children[node_id]:
        get_descendants(child_id, children, descendants)

def start(in_fname, out_fname, index_fname, *taxids):
    children = parse_taxonomy_file(index_fname)
    descendants = set()
    for taxid in taxids:
        get_descendants(taxid, children, descendants)

    aligner_taxid_lca_column_index = None
    with gzip.open(in_fname, "rt") as inf, \
         gzip.open(out_fname, "wt") as outf:
         for line in inf:
             if aligner_taxid_lca_column_index is None:
                 aligner_taxid_lca_column_index = line.split(
                     "\t").index("aligner_taxid_lca")
                 outf.write(line)
                 continue

             if line.split("\t")[
                     aligner_taxid_lca_column_index] in descendants:
                 outf.write(line)

if __name__ == "__main__":
    start(*sys.argv[1:])
