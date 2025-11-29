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

    @pytest.mark.fast
    @pytest.mark.unit
    def test_reverse_complement_standard_bases(self):
        assert _reverse_complement("ACGT") == "ACGT"
        assert _reverse_complement("AAAA") == "TTTT"
        assert _reverse_complement("TTTT") == "AAAA"
        assert _reverse_complement("GCGC") == "GCGC"

    @pytest.mark.fast
    @pytest.mark.unit
    def test_reverse_complement_with_n(self):
        assert _reverse_complement("ACGTN") == "NACGT"
        assert _reverse_complement("NNNNN") == "NNNNN"

    @pytest.mark.fast
    @pytest.mark.unit
    def test_reverse_complement_empty(self):
        assert _reverse_complement("") == ""

    @pytest.mark.fast
    @pytest.mark.unit
    def test_canonical_kmer_lexicographic_selection(self):
        assert _canonical_kmer("AAAA") == "AAAA"  # AAAA vs TTTT
        assert _canonical_kmer("TTTT") == "AAAA"  # Same result
        assert _canonical_kmer("ACGT") == "ACGT"  # ACGT vs ACGT (palindrome)
        assert _canonical_kmer("AAAC") == "AAAC"  # AAAC vs GTTT
        assert _canonical_kmer("GTTT") == "AAAC"  # Same result

    @pytest.mark.fast
    @pytest.mark.unit
    def test_mismatch_count_equal_length(self):
        assert _mismatch_count("AAAA", "AAAA") == 0
        assert _mismatch_count("AAAA", "TTTT") == 4
        assert _mismatch_count("AAAA", "AAAT") == 1
        assert _mismatch_count("ACGT", "TGCA") == 4

    @pytest.mark.fast
    @pytest.mark.unit
    def test_mismatch_count_unequal_length(self):
        # Truncates to shorter length
        assert _mismatch_count("AAAA", "AA") == 0  # Only compares first 2
        assert _mismatch_count("AA", "AAAA") == 0  # Only compares first 2
        assert _mismatch_count("AAAA", "TT") == 2  # Compares first 2, both differ
        assert _mismatch_count("AAAT", "TT") == 2  # AA vs TT


class TestMinimizerExtraction:
    """Test minimizer extraction functions."""

    @pytest.mark.fast
    @pytest.mark.unit
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

    @pytest.mark.fast
    @pytest.mark.unit
    def test_extract_minimizer_with_n_bases(self):
        params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)
        seq_with_n = "AANAAAANAA"
        seq_without_n = "AAGAAAAGAA"

        hash_with_n = _extract_minimizer(seq_with_n, 0, params)
        hash_without_n = _extract_minimizer(seq_without_n, 0, params)

        # Should skip N-containing kmers and find valid ones
        assert hash_with_n != EMPTY_KMER_SENTINEL_HASH
        assert hash_without_n != EMPTY_KMER_SENTINEL_HASH

    @pytest.mark.fast
    @pytest.mark.unit
    def test_extract_minimizer_window_too_short(self):
        params = MinimizerParams(num_windows=1, window_len=10, kmer_len=7)
        seq = "AAAAA"  # 5bp sequence, need 7bp kmer

        hash_result = _extract_minimizer(seq, 0, params)
        assert hash_result == EMPTY_KMER_SENTINEL_HASH

    @pytest.mark.fast
    @pytest.mark.unit
    def test_extract_minimizer_sequence_too_short(self):
        "Collected windows longer than sequence, should succeed with a sentinel hash."
        params = MinimizerParams(num_windows=2, window_len=10, kmer_len=7)
        seq = "AAAAACCGGTT"  # 11bp sequence, second window is too short

        hash_result = _extract_minimizer(seq, 1, params)
        assert hash_result == EMPTY_KMER_SENTINEL_HASH

    @pytest.mark.fast
    @pytest.mark.unit
    def test_extract_minimizer_sequence_matches_window_matches_kmer(self):
        params = MinimizerParams(num_windows=1, window_len=11, kmer_len=11)
        seq = "AAAAACCGGTT"  # 11bp sequence

        hash_result = _extract_minimizer(seq, 0, params)
        assert hash_result != EMPTY_KMER_SENTINEL_HASH

    @pytest.mark.fast
    @pytest.mark.unit
    def test_extract_minimizer_all_N_window(self):
        params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)
        seq = "NNNNNNNNNN"

        hash_result = _extract_minimizer(seq, 0, params)
        assert hash_result == EMPTY_KMER_SENTINEL_HASH

    @pytest.mark.fast
    @pytest.mark.unit
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

    @pytest.mark.fast
    @pytest.mark.unit
    def test_sequences_match_exact(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01)

        assert _sequences_match("AAAA", "AAAA", params)
        assert _sequences_match("ACGT", "ACGT", params)

    @pytest.mark.fast
    @pytest.mark.unit
    def test_sequences_match_with_offset_1(self):
        params = DedupParams(max_offset=1, max_error_frac=0.25)
        # need a large max_error_frac; for short test test seqs, an offset of
        # 1 is a large relative error

        # Left shift: XAAAA vs AAAA (X removed)
        assert _sequences_match("GAAAA", "AAAA", params)
        # Right shift: AAAA vs XAAAA (X added at start)
        assert _sequences_match("AAAA", "GAAAA", params)

    @pytest.mark.fast
    @pytest.mark.unit
    def test_sequences_match_no_match_large_offset(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01)

        # Should not match with offset > 1
        assert not _sequences_match("GGAAAA", "AAAA", params)
        assert not _sequences_match("AAAA", "GGAAAA", params)

    @pytest.mark.fast
    @pytest.mark.unit
    def test_sequences_error_threshold(self):
        params = DedupParams(max_offset=0, max_error_frac=0.1)  # 10% error allowed

        # 1 error in 10bp = 10% error rate
        assert _sequences_match("AAAAAAAAAA", "AAAAAAAAAG", params)
        # 2 errors in 10bp = 20% error rate (should fail)
        assert not _sequences_match("AAAAAAAAAA", "AAAAAAAGGG", params)

    @pytest.mark.fast
    @pytest.mark.unit
    def test_read_pairs_equivalent_standard_orientation(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01, orientation=ORIENT_STRICT)

        rp1 = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("read2", "AAAA", "TTTT", "IIII", "IIII")
        rp3 = ReadPair("read3", "AAAA", "CCCC", "IIII", "IIII")

        assert _read_pairs_equivalent(rp1, rp2, params)
        assert not _read_pairs_equivalent(rp1, rp3, params)

    @pytest.mark.fast
    @pytest.mark.unit
    def test_read_pairs_equivalent_swapped_tolerant(self):
        # In tolerant mode, should match F1-R1 vs R2-F2
        params = DedupParams(max_offset=1, max_error_frac=0.01, orientation=ORIENT_TOLERANT)

        rp1 = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("read2", "TTTT", "AAAA", "IIII", "IIII")  # Swapped F/R

        assert _read_pairs_equivalent(rp1, rp2, params)

    @pytest.mark.fast
    @pytest.mark.unit
    def test_read_pairs_equivalent_swapped_strict(self):
        # In strict mode, should NOT match swapped orientation
        params = DedupParams(max_offset=1, max_error_frac=0.01, orientation=ORIENT_STRICT)

        rp1 = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("read2", "TTTT", "AAAA", "IIII", "IIII")  # Swapped F/R

        assert not _read_pairs_equivalent(rp1, rp2, params)

    @pytest.mark.fast
    @pytest.mark.unit
    def test_read_pairs_equivalent_no_match(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01, orientation=ORIENT_TOLERANT)

        rp1 = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        rp2 = ReadPair("read2", "GGGG", "CCCC", "IIII", "IIII")

        assert not _read_pairs_equivalent(rp1, rp2, params)


class TestBucketing:
    """Test bucketing functions."""

    @pytest.mark.fast
    @pytest.mark.unit
    def test_assign_to_buckets_correct_assignment(self):
        # Identical read pairs should go to same bucket
        params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)

        rp1 = ReadPair("read1", "A" * 10, "T" * 10, "I" * 10, "I" * 10)
        rp2 = ReadPair("read2", "A" * 10, "T" * 10, "I" * 10, "I" * 10)

        buckets = _assign_to_buckets([rp1, rp2], params, ORIENT_STRICT)

        # Should have at least one bucket containing both reads
        assert any(len(bucket_indices) >= 2 for bucket_indices in buckets.values())

    @pytest.mark.fast
    @pytest.mark.unit
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

    @pytest.mark.fast
    @pytest.mark.unit
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

    @pytest.mark.fast
    @pytest.mark.unit
    def test_build_graph_no_duplicates(self):
        params = DedupParams(max_offset=0, max_error_frac=0.0)
        min_params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)

        rp1 = ReadPair("read1", "A" * 10, "T" * 10, "I" * 10, "I" * 10)
        rp2 = ReadPair("read2", "G" * 10, "C" * 10, "I" * 10, "I" * 10)
        read_pairs = [rp1, rp2]

        buckets = _assign_to_buckets(read_pairs, min_params, params.orientation)
        graph, comparisons = _build_graph(read_pairs, buckets, params)

        assert graph.number_of_edges() == 0

    @pytest.mark.fast
    @pytest.mark.unit
    def test_build_graph_with_duplicates(self):
        params = DedupParams(max_offset=1, max_error_frac=0.01)
        min_params = MinimizerParams(num_windows=1, window_len=10, kmer_len=3)

        rp1 = ReadPair("read1", "A" * 10, "T" * 10, "I" * 10, "I" * 10)
        rp2 = ReadPair("read2", "A" * 10, "T" * 10, "I" * 10, "I" * 10)  # Identical
        read_pairs = [rp1, rp2]

        buckets = _assign_to_buckets(read_pairs, min_params, params.orientation)
        graph, comparisons = _build_graph(read_pairs, buckets, params)

        assert graph.number_of_edges() >= 1

    @pytest.mark.fast
    @pytest.mark.unit
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

    @pytest.mark.fast
    @pytest.mark.unit
    def test_select_exemplar_single_node(self):
        graph = nx.Graph()
        graph.add_node(0)

        rp = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        cluster = {0: rp}

        exemplar = _select_exemplar_by_centrality(cluster, graph)
        assert exemplar == "read1"

    @pytest.mark.fast
    @pytest.mark.unit
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

    @pytest.mark.fast
    @pytest.mark.unit
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

    @pytest.mark.fast
    @pytest.mark.unit
    def test_post_init_sequences_uppercase(self):
        rp = ReadPair("test", "acgt", "tgca", "IIII", "IIII")
        assert rp.fwd_seq == "ACGT"
        assert rp.rev_seq == "TGCA"

    @pytest.mark.fast
    @pytest.mark.unit
    def test_mean_qual_calculation(self):
        # Phred 33: '!' = 0, 'I' = 40
        rp = ReadPair("test", "AAAA", "TTTT", "!!!!", "IIII")
        expected_mean = (0 + 0 + 0 + 0 + 40 + 40 + 40 + 40) / 8
        assert rp.mean_qual() == expected_mean

    @pytest.mark.fast
    @pytest.mark.unit
    def test_mean_qual_empty_qualities(self):
        rp = ReadPair("test", "AAAA", "TTTT", "", "")
        assert rp.mean_qual() == 0.0


class TestParameterValidation:
    """Test parameter validation."""

    @pytest.mark.fast
    @pytest.mark.unit
    def test_minimizer_params_kmer_too_large(self):
        with pytest.raises(ValueError, match="kmer_len .* must be <= window_len"):
            MinimizerParams(window_len=5, kmer_len=7)

    @pytest.mark.fast
    @pytest.mark.unit
    def test_minimizer_params_valid(self):
        params = MinimizerParams(window_len=10, kmer_len=5)
        assert params.window_len == 10
        assert params.kmer_len == 5
        # it's legal if surprising to have window and kmer length match
        params = MinimizerParams(window_len=47, kmer_len=47)
        assert params.window_len == 47
        assert params.kmer_len == 47

    @pytest.mark.fast
    @pytest.mark.unit
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

    @pytest.mark.fast
    @pytest.mark.integration
    def test_empty_input_empty_output(self, dedup_func):
        result = dedup_func([], verbose=False)
        mapping = _get_exemplar_mapping(result)
        assert mapping == {}

    @pytest.mark.fast
    @pytest.mark.integration
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

    @pytest.mark.fast
    @pytest.mark.integration
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

    @pytest.mark.fast
    @pytest.mark.integration
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

    @pytest.mark.fast
    @pytest.mark.integration
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
