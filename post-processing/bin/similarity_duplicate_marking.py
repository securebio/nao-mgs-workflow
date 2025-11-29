#!/usr/bin/env python3
"""
Similarity-based duplicate marking for alignment-unique reads.

Only runs similarity deduplication on reads where prim_align_dup_exemplar ==
seq_id (i.e., reads that are unique according to alignment-based
deduplication).

See README for details.
"""

import os
import csv
import sys
import gzip
import time
from typing import Dict
from pathlib import Path

# Add deps to path so we can import nao_dedup
sys.path.insert(0, str(Path(__file__).parent.parent / "deps"))

from nao_dedup.dedup import (
    ReadPair,
    DedupParams,
    MinimizerParams,
    deduplicate_read_pairs_streaming,
)


def read_alignment_unique_reads(input_path: str):
    """
    Generator that yields ReadPairs for reads where prim_align_dup_exemplar ==
    seq_id.

    These are reads that are unique according to alignment-based deduplication,
    and are the only ones we need to check for similarity-based duplicates.

    Yields:
        ReadPair objects for alignment-unique reads
    """

    with gzip.open(input_path, 'rt') as f:
        reader = csv.DictReader(f, delimiter='\t')

        for row in reader:
            seq_id = row['seq_id']
            prim_align_exemplar = row['prim_align_dup_exemplar']

            # Only process reads that are alignment-unique
            if prim_align_exemplar == seq_id:
                yield ReadPair(
                    read_id=seq_id,
                    fwd_seq=row['query_seq'],
                    rev_seq=row['query_seq_rev'],
                    fwd_qual=row['query_qual'],
                    rev_qual=row['query_qual_rev'],
                )


def write_output_with_sim_column(
    input_path: str,
    output_path: str,
    similarity_exemplars: Dict[str, str]
) -> (int, int, int):
    """
    Second pass: read input TSV and write output with sim_dup_exemplar column.

    For reads where prim_align_dup_exemplar == seq_id, use the similarity
    exemplar; otherwise use 'NA'.

    Returns:
       n_reads, n_prim_align_dups, n_sim_dups
    """

    n_reads = 0
    n_prim_align_dups = 0
    n_sim_dups = 0

    with gzip.open(input_path, 'rt') as inf, \
            gzip.open(output_path, 'wt') as outf:
        reader = csv.DictReader(inf, delimiter='\t')

        fieldnames = list(reader.fieldnames) + ['sim_dup_exemplar']

        writer = csv.DictWriter(
            outf, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()

        for row in reader:
            seq_id = row['seq_id']
            n_reads += 1

            # Only alignment-unique reads get similarity dedup results
            if row['prim_align_dup_exemplar'] == seq_id:
                sim_dup_exemplar = similarity_exemplars.get(seq_id, seq_id)
                if sim_dup_exemplar != seq_id:
                    n_sim_dups += 1
                row['sim_dup_exemplar'] = sim_dup_exemplar
            else:
                n_prim_align_dups += 1
                row['sim_dup_exemplar'] = 'NA'

            writer.writerow(row)

    return n_reads, n_prim_align_dups, n_sim_dups

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.tsv.gz> <output.tsv.gz>",
              file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    ts_start = time.time()

    # Create deduplication parameters explicitly to ensure consistency
    # Note: the default kmer length of 7 is much too loose for our situation,
    # as there are only 4^7 (~16k) possible sequences and we have 10-100 times
    # that many reads.
    params = DedupParams()
    minimizer_params = MinimizerParams(kmer_len=15, window_len=25, num_windows=4)

    n_reads, n_prim_align_dups, n_sim_dups = write_output_with_sim_column(
        input_path, output_path,
        deduplicate_read_pairs_streaming(
            read_alignment_unique_reads(input_path),
            dedup_params=params,
            minimizer_params=minimizer_params))

    ts_end = time.time()

    print(f"Marked similarity duplicates from {os.path.basename(input_path)} "
          f"processing {n_reads} reads in {(ts_end-ts_start):.0f}s, of which "
          f"{n_prim_align_dups} were already known to be duplicate and "
          f"{n_sim_dups} were additionally recognized as duplicate.")


if __name__ == '__main__':
    main()
