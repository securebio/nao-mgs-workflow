"""
Post-assembly read-pair deduplication with error-tolerant matching.

This module provides deduplication of read pairs using minimizer-based
bucket for efficiency. Deduplication is tolerant to small alignment
shifts and sequencing errors.
"""

import sys
from collections import defaultdict
from dataclasses import dataclass, field, InitVar
from itertools import combinations
from typing import Literal, Optional
from zlib import crc32

import networkx as nx

# No Bio import; we represent sequences as strings rather than Bio.Seq:
# - Don't need any Bio machinery
# - Python string operations are faster than the corresponding Seq operations

EMPTY_KMER_SENTINEL_HASH = -1  # crc32 returns nonnegative integers, no collision
HashPair = tuple[int, int]  # hash from fwd mate, hash from rev mate

ORIENT_STRICT = "strict"
ORIENT_TOLERANT = "tolerant"


@dataclass
class DedupParams:
    """User-configurable deduplication parameters."""

    max_offset: int = 1  # Maximum alignment shift in bases
    max_error_frac: float = 0.01  # Maximum mismatch fraction (errors/overlap)
    orientation: Literal["strict", "tolerant"] = (
        ORIENT_TOLERANT  # Whether to check swapped mates
    )


@dataclass
class MinimizerParams:
    """Minimizer configuration (rarely needs changing)."""

    num_windows: int = 3  # Number of windows per read
    window_len: int = 25  # Base pairs per window
    kmer_len: int = 7  # K-mer size for minimizers

    def __post_init__(self):
        if self.kmer_len > self.window_len:
            raise ValueError(
                f"kmer_len ({self.kmer_len}) must be <= window_len ({self.window_len})"
            )


@dataclass(slots=True)
class ReadPair:
    """Container for a read pair with deduplication support."""

    read_id: str
    fwd_seq: str
    rev_seq: str
    fwd_qual: InitVar[str]  # Passed to init but not stored in the object
    rev_qual: InitVar[str]  # Passed to init but not stored in the object

    # Store the calculated score instead of the raw strings
    mean_q: float = field(init=False)
    exemplar_id: Optional[str] = field(default=None, init=False)

    def __post_init__(self, fwd_qual, rev_qual):
        """Ensure sequences are uppercase and pre-calculate quality."""
        self.fwd_seq = self.fwd_seq.upper()
        self.rev_seq = self.rev_seq.upper()

        # Calculate mean quality once and store it as a float (8 bytes)
        # instead of keeping the raw strings (~300+ bytes)
        quals = [ord(c) - 33 for c in fwd_qual + rev_qual]
        self.mean_q = sum(quals) / len(quals) if quals else 0.0

    def mean_qual(self) -> float:
        """Return the pre-calculated mean quality."""
        return self.mean_q


##
# Assign read pairs to buckets based on minimizers.
# Each read pair will be assigned to multiple buckets.
# With high probability, duplicate read pairs will be assigned to at least
# one bucket in common, so we only need to do all-against-all read pair comparisons
# within each bucket.
##

# Complement table for reverse complement (including N)
_COMPLEMENT = str.maketrans("ACGTN", "TGCAN")


def _reverse_complement(seq: str) -> str:
    """Return reverse complement of DNA sequence."""
    return seq.translate(_COMPLEMENT)[::-1]


def _canonical_kmer(kmer: str) -> str:
    """Return lexicographically smaller of kmer and its reverse complement."""
    rc = _reverse_complement(kmer)
    return min(kmer, rc)


def _hash_kmer(kmer: str) -> int:
    """Hash a kmer to an int. The actual hash used is an implementation detail,
    but the result must be stable run-to-run (so no default Python hash)."""
    return crc32(kmer.encode())


def _extract_minimizer(seq: str, window_idx: int, params: MinimizerParams) -> int:
    """
    Extract the minimizer hash from a specific window of the sequence.

    Args:
        seq: DNA sequence
        window_idx: Which window to process (0-based)
        params: Minimizer parameters

    Returns:
        Hash of the lexicographically smallest canonical k-mer in the window
    """
    start = window_idx * params.window_len
    end = min(len(seq), start + params.window_len)

    if end - start < params.kmer_len:
        # Window too short to contain a k-mer - return consistent hash
        return EMPTY_KMER_SENTINEL_HASH

    # Find minimizer (smallest hash) in this window
    bigger_than_hash = sys.maxsize + 1
    min_hash = bigger_than_hash
    for i in range(start, end - params.kmer_len + 1):
        kmer = seq[i : i + params.kmer_len]
        if "N" not in kmer:  # Skip k-mers with ambiguous bases
            canonical = _canonical_kmer(kmer)
            h = _hash_kmer(canonical)
            if h < min_hash:
                min_hash = h

    return min_hash if min_hash != bigger_than_hash else EMPTY_KMER_SENTINEL_HASH


def _get_bucket_keys(
    read_pair: ReadPair, params: MinimizerParams, orientation: str
) -> set[HashPair]:
    """
    Generate all bucket keys for a read pair based on minimizers.

    Returns set of (forward_hash, reverse_hash) tuples that serve as bucket keys.
    """
    # Extract minimizers from each window
    fwd_hashes = [
        _extract_minimizer(read_pair.fwd_seq, i, params) for i in range(params.num_windows)
    ]
    rev_hashes = [
        _extract_minimizer(read_pair.rev_seq, i, params) for i in range(params.num_windows)
    ]

    # Generate all hash pairs
    keys = {(fh, rh) for fh in fwd_hashes for rh in rev_hashes}

    # In tolerant mode, also consider swapped orientation
    if orientation == ORIENT_TOLERANT:
        keys |= {(rh, fh) for fh in fwd_hashes for rh in rev_hashes}

    return keys


def _assign_to_buckets(
    read_pairs: list[ReadPair], minimizer_params: MinimizerParams, orientation: str
) -> dict[HashPair, list[int]]:
    """Assign read pairs to buckets by minimizers. Return a Dict {bucket_key : indices}
    where bucket_key is a tuple of ints (kmer hashes) and indices are relative to the
    input list of read pairs."""
    buckets = defaultdict(list)
    for idx, rp in enumerate(read_pairs):
        keys = _get_bucket_keys(rp, minimizer_params, orientation)
        for key in keys:
            buckets[key].append(idx)
    return buckets


##
# Read pair comparisons: are a pair of read pairs dups of each other?
##


def _mismatch_count(s1: str, s2: str) -> int:
    """Count mismatches between two strings (compares up to shorter length)."""
    return sum(c1 != c2 for c1, c2 in zip(s1, s2))


def _sequences_match(seq1: str, seq2: str, params: DedupParams) -> bool:
    """
    Check if two sequences match within allowed offset and error tolerance.

    Tests alignments with seq1 shifted relative to seq2 by up to max_offset bases, counting
    each base of offset as a single base mismatch.
    """
    for offset in range(-params.max_offset, params.max_offset + 1):
        # Determine overlap region
        if offset >= 0:
            # seq1 shifted left: seq1[offset:] aligns with seq2[0:]
            mismatches = _mismatch_count(seq1[offset:], seq2)
            overlap_len = min(len(seq1) - offset, len(seq2))
        else:
            # seq1 shifted right: seq1[0:] aligns with seq2[-offset:]
            mismatches = _mismatch_count(seq1, seq2[-offset:])
            overlap_len = min(len(seq1), len(seq2) + offset)
        if overlap_len <= 0:
            continue

        if abs(offset) + mismatches <= params.max_error_frac * overlap_len:
            return True

    return False


def _read_pairs_equivalent(rp1: ReadPair, rp2: ReadPair, params: DedupParams) -> bool:
    """
    Test if two read pairs are equivalent (duplicates).

    In strict mode: F1-R1 must match F2-R2
    In tolerant mode: Also checks F1-R1 against R2-F2 (swapped orientation)
    """
    # Always check standard orientation
    if _sequences_match(rp1.fwd_seq, rp2.fwd_seq, params) and _sequences_match(
        rp1.rev_seq, rp2.rev_seq, params
    ):
        return True

    # In tolerant mode, also check swapped orientation
    if params.orientation == ORIENT_TOLERANT:
        if _sequences_match(rp1.fwd_seq, rp2.rev_seq, params) and _sequences_match(
            rp1.rev_seq, rp2.fwd_seq, params
        ):
            return True

    return False


##
# Graph based duplicate detection
##


def _select_exemplar_by_centrality(cluster: dict[int, ReadPair], graph: nx.Graph) -> str:
    """
    Select exemplar as the most central node in the cluster subgraph.

    Uses eccentricity (max distance to any other node) as the centrality measure.
    Ties are broken by mean quality, then total length, then read ID.

    Args:
        cluster: List of ReadPair objects in the cluster
        cluster_indices: Corresponding node indices in the graph
        graph: The full deduplication graph

    Returns:
        Read ID of the selected exemplar
    """
    if len(cluster) == 1:
        return list(cluster.values())[0].read_id

    subgraph = graph.subgraph(cluster.keys())

    # Calculate eccentricity for each node (lower is more central)
    # For disconnected subgraphs, this still works per component
    eccentricities = nx.eccentricity(subgraph)

    # Build selection criteria for each read
    candidates = []
    for idx, rp in cluster.items():
        eccentricity = eccentricities[idx]
        mean_qual = rp.mean_qual()
        total_len = len(rp.fwd_seq) + len(rp.rev_seq)

        # Lower values are better
        score = (eccentricity, -mean_qual, -total_len, rp.read_id)
        candidates.append((score, rp.read_id))

    # Select best candidate
    return min(candidates)[1]


def _build_graph(
    read_pairs: list[ReadPair],
    buckets: dict[HashPair, list[int]],
    dedup_params: DedupParams,
) -> tuple[nx.Graph, int]:
    """Build a graph over read pairs that have already been sorted into buckets. In the
    graph, vertices are read pairs and edges represent equivalence. Returns a tuple
    (graph, n_comparisons)."""
    graph = nx.Graph()
    graph.add_nodes_from(range(len(read_pairs)))

    # Compare reads within each bucket
    comparisons = set()  # don't repeat comparisons across buckets
    for bucket_indices in buckets.values():
        # Check all pairs in this bucket
        for i, j in combinations(sorted(bucket_indices), 2):
            # (i, j) in sorted order since we sorted bucket_indices
            if (i, j) in comparisons:
                continue
            if _read_pairs_equivalent(read_pairs[i], read_pairs[j], dedup_params):
                graph.add_edge(i, j)
            comparisons.add((i, j))

    return graph, len(comparisons)


def deduplicate_read_pairs(
    read_pairs: list[ReadPair],
    dedup_params: DedupParams = DedupParams(),
    minimizer_params: MinimizerParams = MinimizerParams(),
    verbose: bool = True,
) -> list[ReadPair]:
    """
    Deduplicate a list of read pairs from a single library.

    Args:
        read_pairs: List of ReadPair objects to deduplicate
        dedup_params: Parameters controlling deduplication behavior
        minimizer_params: Parameters for minimizer extraction
        verbose: Print some debug info

    Returns:
        Same list with exemplar_id field populated for each read
    """
    if len(read_pairs) == 0:
        return read_pairs

    # Step 1: Build equivalence graph
    buckets = _assign_to_buckets(read_pairs, minimizer_params, dedup_params.orientation)
    graph, comparisons = _build_graph(read_pairs, buckets, dedup_params)

    # Save bucket count for verbose output before freeing memory
    n_buckets = len(buckets)

    # Delete the buckets index immediately to free memory
    # before running the connected components logic.
    del buckets

    # Step 2: Find connected components and assign exemplars
    for component in nx.connected_components(graph):
        component_list = list(component)
        cluster = {idx: read_pairs[idx] for idx in component_list}
        exemplar_id = _select_exemplar_by_centrality(cluster, graph)

        # Mark all reads in cluster with their exemplar
        for rp in cluster.values():
            rp.exemplar_id = exemplar_id

    if verbose:
        n_components = nx.number_connected_components(graph)
        n_edges = graph.number_of_edges()
        print(
            f"Deduplication: {len(read_pairs)} reads, {n_buckets} buckets, "
            f"{comparisons} comparisons, {n_edges} edges, {n_components} components"
        )

    return read_pairs
