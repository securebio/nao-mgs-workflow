import random
import sys
from pathlib import Path

# Add parent directory to path so we can import dedup
sys.path.insert(0, str(Path(__file__).parent.parent))

import networkx as nx
import pytest

from dedup import (
    EMPTY_KMER_SENTINEL_HASH,
    ORIENT_STRICT,
    ORIENT_TOLERANT,
    DedupParams,
    MinimizerParams,
    ReadPair,
    _assign_to_buckets,
    _build_graph,
    _canonical_kmer,
    _extract_minimizer,
    _get_bucket_keys,
    _mismatch_count,
    _read_pairs_equivalent,
    _reverse_complement,
    _select_exemplar_by_centrality,
    _sequences_match,
    deduplicate_read_pairs,
    deduplicate_read_pairs_streaming,
)


def _random_seq(length: int, rng: random.Random) -> str:
    """Generate random DNA sequence of specified length."""
    return "".join(rng.choices(["A", "C", "G", "T"], k=length))


class TestHelperFunctions:
    """Test sequence manipulation helper functions."""

    def test_reverse_complement_standard_bases(self):
        assert _reverse_complement("ACGT") == "ACGT"
        assert _reverse_complement("AAAA") == "TTTT"
        assert _reverse_complement("TTTT") == "AAAA"
        assert _reverse_complement("GCGC") == "GCGC"

    def test_reverse_complement_with_n(self):
        assert _reverse_complement("ACGTN") == "NACGT"
        assert _reverse_complement("NNNNN") == "NNNNN"

    def test_reverse_complement_empty(self):
        assert _reverse_complement("") == ""

    def test_canonical_kmer_lexicographic_selection(self):
        assert _canonical_kmer("AAAA") == "AAAA"  # AAAA vs TTTT
        assert _canonical_kmer("TTTT") == "AAAA"  # Same result
        assert _canonical_kmer("ACGT") == "ACGT"  # ACGT vs ACGT (palindrome)
        assert _canonical_kmer("AAAC") == "AAAC"  # AAAC vs GTTT
        assert _canonical_kmer("GTTT") == "AAAC"  # Same result

    def test_mismatch_count_equal_length(self):
        assert _mismatch_count("AAAA", "AAAA") == 0
        assert _mismatch_count("AAAA", "TTTT") == 4
        assert _mismatch_count("AAAA", "AAAT") == 1
        assert _mismatch_count("ACGT", "TGCA") == 4

    def test_mismatch_count_unequal_length(self):
        # Truncates to shorter length
        assert _mismatch_count("AAAA", "AA") == 0  # Only compares first 2
        assert _mismatch_count("AA", "AAAA") == 0  # Only compares first 2
        assert _mismatch_count("AAAA", "TT") == 2  # Compares first 2, both differ
        assert _mismatch_count("AAAT", "TT") == 2  # AA vs TT


class TestMinimizerExtraction:
    """Test minimizer extraction functions."""

    def test_extract_minimizer_normal_window(self):
        params = MinimizerParams(num_windows=2, window_len=20, kmer_len=7)
        seq = "A" * 20 + "C" * 20  # 40bp sequence

        # Test first window (all A's - should give consistent result)
        hash1 = _extract_minimizer(seq, 0, params)
        hash2 = _extract_minimizer(seq, 0, params)
        assert hash1 == hash2  # Should be deterministic

        # Test second window (all C's - should give different result)
        hash3 = _extract_minimizer(seq, 1, params)
        assert hash1 != hash3  # Different sequences should give different hashes

    def test_extract_minimizer_with_n_bases(self):
        params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)
        seq_with_n = "AANAAAANAA"
        seq_without_n = "AAGAAAAGAA"

        hash_with_n = _extract_minimizer(seq_with_n, 0, params)
        hash_without_n = _extract_minimizer(seq_without_n, 0, params)

        # Should skip N-containing kmers and find valid ones
        assert hash_with_n != EMPTY_KMER_SENTINEL_HASH
        assert hash_without_n != EMPTY_KMER_SENTINEL_HASH

    def test_extract_minimizer_window_too_short(self):
        params = MinimizerParams(num_windows=1, window_len=10, kmer_len=7)
        seq = "AAAAA"  # 5bp sequence, need 7bp kmer

        hash_result = _extract_minimizer(seq, 0, params)
        assert hash_result == EMPTY_KMER_SENTINEL_HASH

    def test_extract_minimizer_sequence_too_short(self):
        "Collected windows longer than sequence, should succeed with a sentinel hash."
        params = MinimizerParams(num_windows=2, window_len=10, kmer_len=7)
        seq = "AAAAACCGGTT"  # 11bp sequence, second window is too short

        hash_result = _extract_minimizer(seq, 1, params)
        assert hash_result == EMPTY_KMER_SENTINEL_HASH

    def test_extract_minimizer_sequence_matches_window_matches_kmer(self):
        params = MinimizerParams(num_windows=1, window_len=11, kmer_len=11)
        seq = "AAAAACCGGTT"  # 11bp sequence

        hash_result = _extract_minimizer(seq, 0, params)
        assert hash_result != EMPTY_KMER_SENTINEL_HASH

    def test_extract_minimizer_all_N_window(self):
        params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)
        seq = "NNNNNNNNNN"

        hash_result = _extract_minimizer(seq, 0, params)
        assert hash_result == EMPTY_KMER_SENTINEL_HASH

    def test_get_bucket_keys(self):
        params = MinimizerParams(num_windows=2, window_len=20, kmer_len=7)
        rng = random.Random("hello")
        rp = ReadPair(
            "test", _random_seq(40, rng), _random_seq(40, rng), "I" * 40, "I" * 40
        )

        # Strict mode should generate num_windows² keys:
        # num_windows minimizers in the fwd seq x num_windows in the rev seq
        keys = _get_bucket_keys(rp, params, ORIENT_STRICT)
        assert len(keys) == 4
        assert all(isinstance(key, tuple) and len(key) == 2 for key in keys)

        # Tolerant mode should generate 2*num_windows² keys:
        # num_windows minimizers in the fwd seq x num_windows in the rev seq,
        # times 2 for swapping fwd/rev
        keys = _get_bucket_keys(rp, params, ORIENT_TOLERANT)
        assert len(keys) == 8
        assert all(isinstance(key, tuple) and len(key) == 2 for key in keys)


class TestSequenceMatching:
    """Test sequence matching functions."""

    def test_sequences_match_exact(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01)

        assert _sequences_match("AAAA", "AAAA", params)
        assert _sequences_match("ACGT", "ACGT", params)

    def test_sequences_match_with_offset_1(self):
        params = DedupParams(max_offset=1, max_error_frac=0.25)
        # need a large max_error_frac; for short test test seqs, an offset of
        # 1 is a large relative error

        # Left shift: XAAAA vs AAAA (X removed)
        assert _sequences_match("GAAAA", "AAAA", params)
        # Right shift: AAAA vs XAAAA (X added at start)
        assert _sequences_match("AAAA", "GAAAA", params)

    def test_sequences_match_no_match_large_offset(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01)

        # Should not match with offset > 1
        assert not _sequences_match("GGAAAA", "AAAA", params)
        assert not _sequences_match("AAAA", "GGAAAA", params)

    def test_sequences_error_threshold(self):
        params = DedupParams(max_offset=0, max_error_frac=0.1)  # 10% error allowed

        # 1 error in 10bp = 10% error rate
        assert _sequences_match("AAAAAAAAAA", "AAAAAAAAAG", params)
        # 2 errors in 10bp = 20% error rate (should fail)
        assert not _sequences_match("AAAAAAAAAA", "AAAAAAAGGG", params)

    def test_read_pairs_equivalent_standard_orientation(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01, orientation=ORIENT_STRICT)

        rp1 = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("read2", "AAAA", "TTTT", "IIII", "IIII")
        rp3 = ReadPair("read3", "AAAA", "CCCC", "IIII", "IIII")

        assert _read_pairs_equivalent(rp1, rp2, params)
        assert not _read_pairs_equivalent(rp1, rp3, params)

    def test_read_pairs_equivalent_swapped_tolerant(self):
        # In tolerant mode, should match F1-R1 vs R2-F2
        params = DedupParams(max_offset=1, max_error_frac=0.01, orientation=ORIENT_TOLERANT)

        rp1 = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("read2", "TTTT", "AAAA", "IIII", "IIII")  # Swapped F/R

        assert _read_pairs_equivalent(rp1, rp2, params)

    def test_read_pairs_equivalent_swapped_strict(self):
        # In strict mode, should NOT match swapped orientation
        params = DedupParams(max_offset=1, max_error_frac=0.01, orientation=ORIENT_STRICT)

        rp1 = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("read2", "TTTT", "AAAA", "IIII", "IIII")  # Swapped F/R

        assert not _read_pairs_equivalent(rp1, rp2, params)

    def test_read_pairs_equivalent_no_match(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01, orientation=ORIENT_TOLERANT)

        rp1 = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("read2", "GGGG", "CCCC", "IIII", "IIII")

        assert not _read_pairs_equivalent(rp1, rp2, params)


class TestBucketing:
    """Test bucketing functions."""

    def test_assign_to_buckets_correct_assignment(self):
        # Identical read pairs should go to same bucket
        params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)

        rp1 = ReadPair("read1", "A" * 10, "T" * 10, "I" * 10, "I" * 10)
        rp2 = ReadPair("read2", "A" * 10, "T" * 10, "I" * 10, "I" * 10)

        buckets = _assign_to_buckets([rp1, rp2], params, ORIENT_STRICT)

        # Should have at least one bucket containing both reads
        assert any(len(bucket_indices) >= 2 for bucket_indices in buckets.values())

    def test_assign_to_buckets_multiple_buckets(self):
        # Reads should appear in multiple buckets (since num_windows > 1)
        params = MinimizerParams(num_windows=2, window_len=10, kmer_len=3)
        rng = random.Random("hello")
        rp = ReadPair(
            "read1",
            _random_seq(20, rng),
            _random_seq(20, rng),
            "I" * 20,
            "I" * 20,
        )

        buckets = _assign_to_buckets([rp], params, ORIENT_STRICT)

        # Read should appear in multiple buckets (W² = 4 buckets)
        total_appearances = sum(0 in indices for indices in buckets.values())
        assert total_appearances >= 4

    def test_dups_share_buckets(self):
        """Test with realistic-ish data that two read pairs which are duplicates
        with a single base error in each of fwd/rev read share a bucket. This test
        is probabilistic, though with fixed random seed should be consistent."""
        rng = random.Random(1234)
        read_len = 150

        def _seq_with_error(seq):
            error_loc = rng.choice(range(read_len))
            if rng.random() < 0.5:  # sometimes offset
                start = 1
            else:
                start = 0
            return (
                seq[start:error_loc]
                + rng.choice(["A", "C", "G", "T"])
                + seq[error_loc + 1 :]
            )

        for rep in range(100):
            # create a pair of reads that are dups but not perfect dups
            rp1 = ReadPair(
                "pair1",
                _random_seq(read_len, rng),
                _random_seq(read_len, rng),
                "I" * read_len,
                "I" * read_len,
            )
            rp2_fwd = _seq_with_error(rp1.fwd_seq)
            rp2_rev = _seq_with_error(rp1.rev_seq)
            rp2 = ReadPair(
                "pair2", rp2_fwd, rp2_rev, "I" * len(rp2_fwd), "I" * len(rp2_rev)
            )
            buckets = _assign_to_buckets([rp1, rp2], MinimizerParams(), ORIENT_STRICT)
            # Should have at least one bucket containing both reads
            assert any(len(bucket_indices) == 2 for bucket_indices in buckets.values())


class TestGraphOperations:
    """Test graph-based operations."""

    def test_build_graph_no_duplicates(self):
        params = DedupParams(max_offset=0, max_error_frac=0.0)
        min_params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)

        rp1 = ReadPair("read1", "A" * 10, "T" * 10, "I" * 10, "I" * 10)
        rp2 = ReadPair("read2", "G" * 10, "C" * 10, "I" * 10, "I" * 10)
        read_pairs = [rp1, rp2]

        buckets = _assign_to_buckets(read_pairs, min_params, params.orientation)
        graph, comparisons = _build_graph(read_pairs, buckets, params)

        assert graph.number_of_edges() == 0

    def test_build_graph_with_duplicates(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01)
        min_params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)

        rp1 = ReadPair("read1", "A" * 10, "T" * 10, "I" * 10, "I" * 10)
        rp2 = ReadPair("read2", "A" * 10, "T" * 10, "I" * 10, "I" * 10)  # Identical
        read_pairs = [rp1, rp2]

        buckets = _assign_to_buckets(read_pairs, min_params, params.orientation)
        graph, comparisons = _build_graph(read_pairs, buckets, params)

        assert graph.number_of_edges() >= 1

    def test_build_graph_no_repeated_comparisons(self):
        # Ensure we don't compare the same pair twice across buckets
        params = DedupParams(max_offset=1, max_error_frac=0.01)
        min_params = MinimizerParams(num_windows=2, window_len=10, kmer_len=3)

        # Create reads that will appear in multiple buckets
        rp1 = ReadPair("read1", "A" * 20, "T" * 20, "I" * 20, "I" * 20)
        rp2 = ReadPair("read2", "A" * 20, "T" * 20, "I" * 20, "I" * 20)
        read_pairs = [rp1, rp2]

        buckets = _assign_to_buckets(read_pairs, min_params, params.orientation)
        graph, comparisons = _build_graph(read_pairs, buckets, params)

        # Should only compare once despite appearing in multiple buckets
        assert comparisons == 1

    def test_select_exemplar_single_node(self):
        graph = nx.Graph()
        graph.add_node(0)

        rp = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        cluster = {0: rp}

        exemplar = _select_exemplar_by_centrality(cluster, graph)
        assert exemplar == "read1"

    def test_select_exemplar_linear_graph(self):
        # A-B-C linear graph should choose B (most central)
        graph = nx.Graph()
        graph.add_edges_from([(0, 1), (1, 2)])

        rp0 = ReadPair("readA", "AAAA", "TTTT", "IIII", "IIII")
        rp1 = ReadPair("readB", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("readC", "AAAA", "TTTT", "IIII", "IIII")
        cluster = {0: rp0, 1: rp1, 2: rp2}

        exemplar = _select_exemplar_by_centrality(cluster, graph)
        assert exemplar == "readB"  # Should choose central node

    def test_select_exemplar_tie_breaking(self):
        # Tie-breaking by quality, then length, then ID
        graph = nx.Graph()
        graph.add_edges_from([(0, 1)])

        # Lower quality read
        rp0 = ReadPair("readZ", "AAAA", "TTTT", "!!!!", "!!!!")  # Lower quality scores
        # Higher quality read
        rp1 = ReadPair("readA", "AAAA", "TTTT", "IIII", "IIII")  # Higher quality scores
        cluster = {0: rp0, 1: rp1}

        exemplar = _select_exemplar_by_centrality(cluster, graph)
        assert exemplar == "readA"  # Should choose higher quality read


class TestReadPairClass:
    """Test ReadPair class functionality."""

    def test_post_init_sequences_uppercase(self):
        rp = ReadPair("test", "acgt", "tgca", "IIII", "IIII")
        assert rp.fwd_seq == "ACGT"
        assert rp.rev_seq == "TGCA"

    def test_mean_qual_calculation(self):
        # Phred 33: '!' = 0, 'I' = 40
        rp = ReadPair("test", "AAAA", "TTTT", "!!!!", "IIII")
        expected_mean = (0 + 0 + 0 + 0 + 40 + 40 + 40 + 40) / 8
        assert rp.mean_qual() == expected_mean

    def test_mean_qual_empty_qualities(self):
        rp = ReadPair("test", "AAAA", "TTTT", "", "")
        assert rp.mean_qual() == 0.0


class TestParameterValidation:
    """Test parameter validation."""

    def test_minimizer_params_kmer_too_large(self):
        with pytest.raises(ValueError, match="kmer_len .* must be <= window_len"):
            MinimizerParams(window_len=5, kmer_len=7)

    def test_minimizer_params_valid(self):
        params = MinimizerParams(window_len=10, kmer_len=5)
        assert params.window_len == 10
        assert params.kmer_len == 5
        # it's legal if surprising to have window and kmer length match
        params = MinimizerParams(window_len=47, kmer_len=47)
        assert params.window_len == 47
        assert params.kmer_len == 47


    def test_dedup_params_valid_orientation(self):
        params1 = DedupParams(orientation=ORIENT_STRICT)
        params2 = DedupParams(orientation=ORIENT_TOLERANT)

        assert params1.orientation == ORIENT_STRICT
        assert params2.orientation == ORIENT_TOLERANT


def _get_exemplar_mapping(result):
    """Helper to get exemplar mapping from either result type."""
    if isinstance(result, dict):
        # Streaming version returns dict
        return result
    else:
        # Graph version returns list
        return {rp.read_id: rp.exemplar_id for rp in result}


@pytest.mark.parametrize(
    "dedup_func",
    [deduplicate_read_pairs, deduplicate_read_pairs_streaming],
    ids=["graph", "streaming"]
)
class TestDeduplicateFunction:
    """End-to-end tests for both deduplication algorithms."""

    def test_empty_input_empty_output(self, dedup_func):
        result = dedup_func([], verbose=False)
        mapping = _get_exemplar_mapping(result)
        assert mapping == {}

    def test_all_identical_sequences_single_cluster(self, dedup_func):
        rng = random.Random(42)
        seq_f = _random_seq(100, rng)
        seq_r = _random_seq(100, rng)
        qual = "I" * 100

        read_pairs = [
            ReadPair("read1", seq_f, seq_r, qual, qual),
            ReadPair("read2", seq_f, seq_r, qual, qual),
            ReadPair("read3", seq_f, seq_r, qual, qual),
        ]

        result = dedup_func(read_pairs, verbose=False)
        mapping = _get_exemplar_mapping(result)

        exemplars = set(mapping.values())
        assert len(exemplars) == 1
        assert set(mapping.keys()) == {"read1", "read2", "read3"}

    def test_no_duplicates_all_singletons(self, dedup_func):
        rng = random.Random(42)
        qual = "I" * 100

        read_pairs = [
            ReadPair("read1", _random_seq(100, rng), _random_seq(100, rng), qual, qual),
            ReadPair("read2", _random_seq(100, rng), _random_seq(100, rng), qual, qual),
            ReadPair("read3", _random_seq(100, rng), _random_seq(100, rng), qual, qual),
        ]

        result = dedup_func(read_pairs, verbose=False)
        mapping = _get_exemplar_mapping(result)

        for read_id, exemplar_id in mapping.items():
            assert exemplar_id == read_id
        assert set(mapping.keys()) == {"read1", "read2", "read3"}

    def test_multiple_small_clusters(self, dedup_func):
        rng = random.Random(42)

        seq1_f = _random_seq(100, rng)
        seq1_r = _random_seq(100, rng)
        seq2_f = _random_seq(100, rng)
        seq2_r = _random_seq(100, rng)
        seq3_f = _random_seq(100, rng)
        seq3_r = _random_seq(100, rng)

        read_pairs = [
            ReadPair("read1", seq1_f, seq1_r, "I" * 100, "J" * 100),  # Lower quality
            ReadPair("read2", seq1_f, seq1_r, "K" * 100, "K" * 100),  # Higher quality
            ReadPair("read3", seq2_f, seq2_r, "I" * 100, "I" * 100),  # Singleton
            ReadPair("read4", seq3_f, seq3_r, "I" * 100, "I" * 100),  # Cluster 2
            ReadPair("read5", seq3_f, seq3_r, "I" * 100, "I" * 100),  # Cluster 2
        ]

        result = dedup_func(read_pairs, verbose=False)
        mapping = _get_exemplar_mapping(result)

        exemplars = set(mapping.values())
        assert len(exemplars) == 3

        assert set(mapping.keys()) == {"read1", "read2", "read3", "read4", "read5"}

        assert mapping["read1"] == "read2"  # Higher quality wins
        assert mapping["read2"] == "read2"
        assert mapping["read3"] == "read3"  # Singleton
        assert mapping["read4"] in ["read4", "read5"]
        assert mapping["read5"] == mapping["read4"]

    def test_realistic_reads_with_errors(self, dedup_func):
        rng = random.Random(42)

        base_seq_f = _random_seq(150, rng)
        base_seq_r = _random_seq(150, rng)
        qual = "I" * 150

        # Create reads with small differences
        sub_f = "G" if base_seq_f[50] != "G" else "A"
        base_seq_f_with_error = base_seq_f[:50] + sub_f + base_seq_f[51:]
        sub_r = "G" if base_seq_r[50] != "G" else "A"
        base_seq_r_with_error = base_seq_r[:50] + sub_r + base_seq_r[51:]

        read_pairs = [
            ReadPair("read1", base_seq_f, base_seq_r, qual, qual),
            ReadPair("read2", base_seq_f_with_error, base_seq_r, qual, qual),
            ReadPair("read3", base_seq_f, base_seq_r_with_error, qual, qual),
            ReadPair(
                "read4", _random_seq(150, rng), _random_seq(150, rng), qual, qual
            ),  # Different
        ]

        dedup_params = DedupParams(max_offset=1, max_error_frac=0.01)
        result = dedup_func(read_pairs, dedup_params, verbose=False)
        mapping = _get_exemplar_mapping(result)

        exemplar1 = mapping["read1"]
        assert mapping["read2"] == exemplar1
        assert mapping["read3"] == exemplar1

        assert mapping["read4"] == "read4"

        exemplars = set(mapping.values())
        assert len(exemplars) == 2


# ============================================================================
# C Implementation Tests
# ============================================================================

# Import C wrapper
try:
    from c_wrapper import deduplicate_read_pairs_c
except (ImportError, FileNotFoundError) as e:
    raise RuntimeError(
        "C library not found. Run 'make test-lib' to build it."
    ) from e

# Parametrize to run tests on both implementations
c_and_python_impls = [deduplicate_read_pairs_streaming, deduplicate_read_pairs_c]
impl_ids = ["python-streaming", "c"]


@pytest.mark.parametrize(
    "dedup_func",
    c_and_python_impls,
    ids=impl_ids
)
class TestCAndPythonDeduplication:
    """Tests that run on both C and Python implementations."""

    def test_empty_input(self, dedup_func):
        result = dedup_func([], verbose=False)
        mapping = _get_exemplar_mapping(result)
        assert mapping == {}

    def test_single_read(self, dedup_func):
        rp = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        result = dedup_func([rp], verbose=False)
        mapping = _get_exemplar_mapping(result)
        assert mapping == {"read1": "read1"}

    def test_identical_reads(self, dedup_func):
        rng = random.Random(42)
        seq_f = _random_seq(100, rng)
        seq_r = _random_seq(100, rng)
        qual = "I" * 100

        read_pairs = [
            ReadPair("read1", seq_f, seq_r, qual, qual),
            ReadPair("read2", seq_f, seq_r, qual, qual),
            ReadPair("read3", seq_f, seq_r, qual, qual),
        ]

        result = dedup_func(read_pairs, verbose=False)
        mapping = _get_exemplar_mapping(result)

        # All should map to same exemplar
        exemplars = set(mapping.values())
        assert len(exemplars) == 1

    def test_no_duplicates(self, dedup_func):
        rng = random.Random(42)
        qual = "I" * 100

        read_pairs = [
            ReadPair("read1", _random_seq(100, rng), _random_seq(100, rng), qual, qual),
            ReadPair("read2", _random_seq(100, rng), _random_seq(100, rng), qual, qual),
            ReadPair("read3", _random_seq(100, rng), _random_seq(100, rng), qual, qual),
        ]

        result = dedup_func(read_pairs, verbose=False)
        mapping = _get_exemplar_mapping(result)

        # Each should be its own exemplar
        for read_id in mapping:
            assert mapping[read_id] == read_id

    def test_approximate_match_parity(self, dedup_func):
        """Ensure C and Python handle mismatches identically."""
        seq = "A" * 100
        seq_error = "A" * 50 + "T" + "A" * 49  # 1 mismatch (1% error)

        # Should match with 2% error threshold
        rp1 = ReadPair("r1", seq, seq, "I"*100, "I"*100)
        rp2 = ReadPair("r2", seq_error, seq, "I"*100, "I"*100)

        # Explicit params to allow the error
        params = DedupParams(max_error_frac=0.02)
        result = dedup_func([rp1, rp2], dedup_params=params)
        mapping = _get_exemplar_mapping(result)

        # Should be clustered together
        assert mapping["r1"] == mapping["r2"]

    def test_approximate_match_threshold(self, dedup_func):
        """Ensure C and Python reject matches above error threshold."""
        seq = "A" * 100
        seq_error = "A" * 97 + "TTT"  # 3 mismatches (3% error)

        rp1 = ReadPair("r1", seq, seq, "I"*100, "I"*100)
        rp2 = ReadPair("r2", seq_error, seq, "I"*100, "I"*100)

        # With 2% threshold, should NOT match
        params = DedupParams(max_error_frac=0.02)
        result = dedup_func([rp1, rp2], dedup_params=params)
        mapping = _get_exemplar_mapping(result)

        # Should be separate clusters
        assert mapping["r1"] != mapping["r2"]
        assert mapping["r1"] == "r1"
        assert mapping["r2"] == "r2"

    def test_offset_alignment_left_shift(self, dedup_func):
        """Test that sequences match when one is shifted left by 1 base."""
        # Use longer sequences so they'll share minimizer buckets
        # Sequence 1: G + 99 A's (100 bases total)
        # Sequence 2: 99 A's (99 bases)
        # With offset=1, seq1[1:] aligns with seq2[0:], giving perfect match
        seq1 = "G" + "A" * 99
        seq2 = "A" * 99
        common = "T" * 99  # Same reverse read so they share buckets

        rp1 = ReadPair("r1", seq1, common, "I"*100, "I"*99)
        rp2 = ReadPair("r2", seq2, common, "I"*99, "I"*99)

        # With max_offset=1, these should match
        # Overlap is 99 bases, offset counts as 1 error, so 1/99 ≈ 0.0101 (1.01% error)
        params = DedupParams(max_offset=1, max_error_frac=0.02)
        result = dedup_func([rp1, rp2], dedup_params=params)
        mapping = _get_exemplar_mapping(result)

        # Should be clustered together
        assert mapping["r1"] == mapping["r2"]

    def test_offset_alignment_right_shift(self, dedup_func):
        """Test that sequences match when one is shifted right by 1 base."""
        # Use longer sequences so they'll share minimizer buckets
        # Sequence 1: 99 A's
        # Sequence 2: G + 99 A's (100 bases total)
        # With offset=-1, seq1[0:] aligns with seq2[1:], giving perfect match
        seq1 = "A" * 99
        seq2 = "G" + "A" * 99
        common = "T" * 99  # Same reverse read so they share buckets

        rp1 = ReadPair("r1", seq1, common, "I"*99, "I"*99)
        rp2 = ReadPair("r2", seq2, common, "I"*100, "I"*99)

        # With max_offset=1, these should match
        params = DedupParams(max_offset=1, max_error_frac=0.02)
        result = dedup_func([rp1, rp2], dedup_params=params)
        mapping = _get_exemplar_mapping(result)

        # Should be clustered together
        assert mapping["r1"] == mapping["r2"]

    def test_different_length_no_match_beyond_offset(self, dedup_func):
        """Test that sequences with length difference > max_offset don't match."""
        # Use longer sequences that share minimizer buckets, but differ in BOTH reads
        # Forward: Sequence 1: GG + 98 A's (100 bases) vs 98 A's
        # Reverse: Sequence 1: TT + 98 C's (100 bases) vs 98 C's
        # Both have difference of 2 bases, but max_offset=1, so should not match
        seq1_fwd = "GG" + "A" * 98
        seq2_fwd = "A" * 98
        seq1_rev = "TT" + "C" * 98
        seq2_rev = "C" * 98

        rp1 = ReadPair("r1", seq1_fwd, seq1_rev, "I"*100, "I"*100)
        rp2 = ReadPair("r2", seq2_fwd, seq2_rev, "I"*98, "I"*98)

        params = DedupParams(max_offset=1, max_error_frac=0.01)
        result = dedup_func([rp1, rp2], dedup_params=params)
        mapping = _get_exemplar_mapping(result)

        # Should be separate clusters
        assert mapping["r1"] == "r1"
        assert mapping["r2"] == "r2"

    def test_error_fraction_with_offset(self, dedup_func):
        """Test that offset counts toward error budget."""
        # Seq1: 200 A's
        # Seq2: G + 199 A's (shifted by 1)
        seq1 = "A" * 200
        seq2 = "G" + "A" * 199

        rp1 = ReadPair("r1", seq1, seq1, "I"*200, "I"*200)
        rp2 = ReadPair("r2", seq2, seq1, "I"*200, "I"*200)

        # With max_offset=1 and max_error_frac=0.004:
        # Offset=-1: overlap=199, offset counts as 1, total = 1/199 ≈ 0.00503
        # Should NOT match (0.00503 > 0.004)
        params = DedupParams(max_offset=1, max_error_frac=0.004)
        result = dedup_func([rp1, rp2], dedup_params=params)
        mapping = _get_exemplar_mapping(result)

        # Should be separate clusters
        assert mapping["r1"] == "r1"
        assert mapping["r2"] == "r2"

        # But with 0.006 threshold, should match  (0.00503 <= 0.006)
        params2 = DedupParams(max_offset=1, max_error_frac=0.006)
        result2 = dedup_func([rp1, rp2], dedup_params=params2)
        mapping2 = _get_exemplar_mapping(result2)

        # Should be clustered together
        assert mapping2["r1"] == mapping2["r2"]

    def test_cluster_lookup_after_leader_update(self, dedup_func):
        """
        Test that cluster lookups work correctly after the best_read_id is updated.

        This test triggers a bug where the cluster hash table uses the initial
        exemplar ID as its key, but lookups incorrectly compare against best_read_id,
        which can change during processing.

        Scenario:
        1. Read A (low quality) creates cluster keyed by "readA"
        2. Read B (high quality) matches A, updates best_read_id to "readB"
        3. Read C (low quality) matches A, should find the same cluster

        Bug behavior: Step 3 fails to find the cluster because it hashes "readA"
        but compares against best_read_id "readB", creating a duplicate cluster.
        """
        # Create three identical sequences but with different quality scores
        seq = "A" * 150

        # Read A: lowest quality - will be the initial exemplar
        rpA = ReadPair("readA", seq, seq, "!" * 150, "!" * 150)  # Q=0

        # Read B: highest quality - will become the best exemplar
        rpB = ReadPair("readB", seq, seq, "I" * 150, "I" * 150)  # Q=40

        # Read C: medium quality - should find the existing cluster
        rpC = ReadPair("readC", seq, seq, "5" * 150, "5" * 150)  # Q=20

        # Process in order A, B, C
        read_pairs = [rpA, rpB, rpC]
        result = dedup_func(read_pairs, verbose=False)
        mapping = _get_exemplar_mapping(result)

        # All three should map to the same cluster
        exemplars = set(mapping.values())
        assert len(exemplars) == 1, f"Expected 1 cluster, got {len(exemplars)}: {exemplars}"

        # The best exemplar should be readB (highest quality)
        assert mapping["readA"] == "readB"
        assert mapping["readB"] == "readB"
        assert mapping["readC"] == "readB"

    def test_cluster_lookup_multiple_updates(self, dedup_func):
        """
        Test cluster lookups with multiple leader updates.

        This extends the previous test with more reads to ensure the bug
        doesn't create multiple duplicate clusters.
        """
        seq = "G" * 150

        # Create 5 reads with varying quality, all identical sequences
        read_pairs = [
            ReadPair("read1", seq, seq, "!" * 150, "!" * 150),  # Q=0
            ReadPair("read2", seq, seq, "#" * 150, "#" * 150),  # Q=2
            ReadPair("read3", seq, seq, "I" * 150, "I" * 150),  # Q=40 - best
            ReadPair("read4", seq, seq, "5" * 150, "5" * 150),  # Q=20
            ReadPair("read5", seq, seq, "(" * 150, "(" * 150),  # Q=7
        ]

        result = dedup_func(read_pairs, verbose=False)
        mapping = _get_exemplar_mapping(result)

        # All should map to the same cluster
        exemplars = set(mapping.values())
        assert len(exemplars) == 1, f"Expected 1 cluster, got {len(exemplars)}: {exemplars}"

        # All should map to read3 (highest quality)
        for read_id in ["read1", "read2", "read3", "read4", "read5"]:
            assert mapping[read_id] == "read3", f"{read_id} mapped to {mapping[read_id]}, expected read3"
