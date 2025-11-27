#!/usr/bin/env python3
"""
Combination duplicate marking, adding similarity-based detection on top of the
alignment-based grouping we already have.  See README.
"""

import csv
import sys
import gzip
from pathlib import Path
from typing import Dict, Set, Tuple
from collections import defaultdict

import networkx as nx

# Add deps to path so we can import nao_dedup
sys.path.insert(0, str(Path(__file__).parent.parent / "deps"))

from nao_dedup.dedup import (
    ReadPair,
    DedupParams,
    MinimizerParams,
    deduplicate_read_pairs,
    _read_pairs_equivalent,
    _select_exemplar_by_centrality,
)


def read_dedup_columns(
        input_path: str) -> Tuple[Dict[str, ReadPair], Dict[str, str]]:
    """
    First pass: read only the columns needed for deduplication.

    Returns:
        Tuple of (read_pairs, prim_align_exemplars) where:
        - read_pairs: Dict mapping seq_id to ReadPair
        - prim_align_exemplars: Dict mapping seq_id to prim_align_dup_exemplar
    """
    read_pairs = {}
    prim_align_exemplars = {}

    with gzip.open(input_path, 'rt') as f:
        reader = csv.DictReader(f, delimiter='\t')

        for row in reader:
            seq_id = row['seq_id']
            read_pairs[seq_id] = ReadPair(
                read_id=seq_id,
                fwd_seq=row['query_seq'],
                rev_seq=row['query_seq_rev'],
                fwd_qual=row['query_qual'],
                rev_qual=row['query_qual_rev'],
            )
            prim_align_exemplars[seq_id] = row['prim_align_dup_exemplar']

    return read_pairs, prim_align_exemplars


def validate_exemplars(
    read_pairs: Dict[str, ReadPair],
    prim_align_exemplars: Dict[str, str]
) -> None:
    """Validate that all prim_align_dup_exemplar values point to existing
    seq_ids."""
    seq_ids = set(read_pairs.keys())

    for seq_id, exemplar in prim_align_exemplars.items():
        if exemplar not in seq_ids:
            raise ValueError(
                f"prim_align_dup_exemplar '{exemplar}' for read '{seq_id}' "
                f"not found in file"
            )


def run_similarity_dedup(read_pairs: Dict[str, ReadPair]) -> Dict[str, str]:
    """
    Run similarity-based deduplication using nao_dedup on ALL reads.

    Returns:
        Dict mapping seq_id to similarity_exemplar_id
    """
    # Convert dict to list for deduplication
    read_pair_list = [rp for rp in sorted(
        read_pairs.values(), key=lambda x: x.read_id)]

    deduplicate_read_pairs(read_pair_list, verbose=True)

    similarity_exemplars = {}
    for rp in read_pair_list:
        # If exemplar_id is None, read is not a duplicate
        similarity_exemplars[rp.read_id] = rp.exemplar_id or rp.read_id

    return similarity_exemplars


def build_prim_align_groups(
        prim_align_exemplars: Dict[str, str]) -> Dict[str, Set[str]]:
    """
    Group reads by their prim_align_dup_exemplar.

    Returns:
        Dict mapping exemplar_id to set of seq_ids in that group
    """
    groups = defaultdict(set)

    for seq_id, exemplar in prim_align_exemplars.items():
        groups[exemplar].add(seq_id)

    return dict(groups)


def merge_groups_by_similarity(
    prim_align_groups: Dict[str, Set[str]],
    similarity_exemplars: Dict[str, str]
) -> Dict[str, Set[str]]:
    """
    Merge prim_align groups based on similarity results.

    If any member of group A shares a similarity exemplar with any member of
    group B, merge A and B into one group.

    Returns:
        Dict mapping a representative prim_align_exemplar to all seq_ids in the
        merged group
    """
    # Build a graph where nodes are prim_align exemplars
    # Add edges when any members of two groups share a similarity exemplar
    graph = nx.Graph()

    # Add all prim_align exemplars as nodes
    for exemplar in prim_align_groups.keys():
        graph.add_node(exemplar)

    # Add edges based on similarity
    exemplar_list = list(prim_align_groups.keys())
    for i, exemplar_a in enumerate(exemplar_list):
        for exemplar_b in exemplar_list[i+1:]:
            # Get similarity exemplars for both groups
            sim_exemplars_a = {
                similarity_exemplars[seq_id]
                for seq_id in prim_align_groups[exemplar_a]
            }
            sim_exemplars_b = {
                similarity_exemplars[seq_id]
                for seq_id in prim_align_groups[exemplar_b]
            }

            # If they share any similarity exemplar, merge the groups
            if sim_exemplars_a & sim_exemplars_b:
                graph.add_edge(exemplar_a, exemplar_b)

    # Find connected components
    components = list(nx.connected_components(graph))

    # Build merged groups
    merged_groups = {}
    for component in components:
        # Collect all seq_ids in this component
        all_seq_ids = set()
        for exemplar in component:
            all_seq_ids.update(prim_align_groups[exemplar])

        # Pick representative (lexicographically smallest)
        representative = min(component)
        merged_groups[representative] = all_seq_ids

    return merged_groups


def select_final_exemplars(
    merged_groups: Dict[str, Set[str]],
    read_pairs: Dict[str, ReadPair],
    similarity_exemplars: Dict[str, str]
) -> Dict[str, str]:
    """
    Select final exemplar for each merged group using dedup.py's
    centrality logic.

    Since merged groups can contain reads that aren't all similar to
    each other (they're merged because they share alignment groups with
    similar reads), we use the similarity exemplars already computed to
    pick the best representative.

    Returns:
        Dict mapping seq_id to combined_dup_exemplar
    """
    final_mapping = {}

    for representative, seq_ids in merged_groups.items():
        if len(seq_ids) == 1:
            # Singleton group - read is its own exemplar
            seq_id = list(seq_ids)[0]
            final_mapping[seq_id] = seq_id
            continue

        # Get all unique similarity exemplars in this merged group
        sim_exemplars_in_group = list(set(
            similarity_exemplars[seq_id] for seq_id in seq_ids
        ))

        if len(sim_exemplars_in_group) == 1:
            # All reads have the same similarity exemplar
            chosen_exemplar = sim_exemplars_in_group[0]
        else:
            # Multiple similarity clusters merged - pick best exemplar
            # Get ReadPairs for the similarity exemplars
            exemplar_pairs = [
                read_pairs[exemplar_id]
                for exemplar_id in sorted(sim_exemplars_in_group)
            ]

            # Build graph of similarities among exemplars
            graph = nx.Graph()
            graph.add_nodes_from(range(len(exemplar_pairs)))

            params = DedupParams()
            for i in range(len(exemplar_pairs)):
                for j in range(i + 1, len(exemplar_pairs)):
                    if _read_pairs_equivalent(
                            exemplar_pairs[i], exemplar_pairs[j], params):
                        graph.add_edge(i, j)

            # Handle potentially disconnected graph by finding
            # connected components and picking best from largest
            components = list(nx.connected_components(graph))
            largest_component = max(components, key=len)

            # Select exemplar from largest component
            cluster = {
                idx: exemplar_pairs[idx] for idx in largest_component
            }
            chosen_exemplar = _select_exemplar_by_centrality(
                cluster, graph)

        # Map all reads in merged group to the chosen exemplar
        for seq_id in seq_ids:
            final_mapping[seq_id] = chosen_exemplar

    return final_mapping


def write_output_with_combined_column(
    input_path: str,
    output_path: str,
    combined_exemplars: Dict[str, str]
) -> None:
    """
    Second pass: read input TSV and write output with
    combined_dup_exemplar column.
    """
    with gzip.open(input_path, 'rt') as f_in, \
            gzip.open(output_path, 'wt') as f_out:
        reader = csv.DictReader(f_in, delimiter='\t')

        # Get fieldnames and add new column
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError("Input file has no header")

        fieldnames = list(fieldnames) + ['combined_dup_exemplar']

        writer = csv.DictWriter(
            f_out, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()

        for row in reader:
            seq_id = row.get('seq_id')
            if not seq_id:
                raise ValueError("Row missing seq_id")

            # Add combined_dup_exemplar
            row['combined_dup_exemplar'] = combined_exemplars.get(
                seq_id, seq_id)
            writer.writerow(row)


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.tsv.gz> <output.tsv.gz>",
              file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    print("Pass 1: Reading deduplication columns...")
    read_pairs, prim_align_exemplars = read_dedup_columns(input_path)
    print(f"  Read {len(read_pairs)} reads")

    print("Validating prim_align_dup_exemplar values...")
    validate_exemplars(read_pairs, prim_align_exemplars)

    print("Building prim_align groups...")
    prim_align_groups = build_prim_align_groups(prim_align_exemplars)
    print(f"  Found {len(prim_align_groups)} prim_align groups")

    print("Running similarity-based deduplication...")
    similarity_exemplars = run_similarity_dedup(read_pairs)

    print("Merging groups based on similarity...")
    merged_groups = merge_groups_by_similarity(
        prim_align_groups, similarity_exemplars)
    print(f"  Merged into {len(merged_groups)} final groups")

    print("Selecting final exemplars using centrality logic...")
    combined_exemplars = select_final_exemplars(
        merged_groups, read_pairs, similarity_exemplars)

    print("Pass 2: Writing output with combined_dup_exemplar column...")
    write_output_with_combined_column(
        input_path, output_path, combined_exemplars)

    print("Done!")


if __name__ == '__main__':
    main()
