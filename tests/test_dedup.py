import sys
import gzip
import random
import pytest
import subprocess
import networkx as nx
from pathlib import Path

# Add parent directory to path so we can import dedup
sys.path.insert(0, str(Path(__file__).parent.parent))
from dedup import (
    EMPTY_KMER_SENTINEL_HASH,
    ORIENT_STRICT,
    ORIENT_TOLERANT,
    DedupParams,
    MinimizerParams,
    ReadPair,
    _assign_to_buckets,
    _build_graph,
    _extract_minimizer,
    _get_bucket_keys,
    _hash_kmer,
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


# ============================================================================
# Python-Only Tests (Helper Functions, Graph Operations, etc.)
# ============================================================================

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


# ============================================================================
# Rust-Specific Tests
# ============================================================================

class TestRustSpecific:
    """Tests specific to the Rust implementation."""

    def test_rust_kmer_len_validation(self):
        """Test that Rust validates kmer_len <= 32."""
        from nao_dedup_rust import deduplicate_read_pairs_rust

        # Create a minimal test case
        rp = ReadPair("test", "A" * 100, "T" * 100, "I" * 100, "I" * 100)

        # kmer_len > 32 should raise ValueError from Rust
        params = MinimizerParams(kmer_len=33, window_len=50, num_windows=3)
        with pytest.raises(ValueError, match="k-mer length must be <= 32"):
            deduplicate_read_pairs_rust([rp], minimizer_params=params)


# ============================================================================
# End-to-End Deduplication Tests (All Implementations)
# ============================================================================

# Import Rust wrapper - always rebuild to pick up any changes
rust_bindings_dir = Path(__file__).parent / "rust_bindings"

try:
    # Always run maturin develop to ensure we're testing the latest code
    # (maturin will skip rebuild if nothing changed)
    subprocess.run(
        ["maturin", "develop", "--quiet"],
        cwd=str(rust_bindings_dir),
        check=True
    )
    from nao_dedup_rust import deduplicate_read_pairs_rust
except (subprocess.CalledProcessError, FileNotFoundError) as e:
    raise RuntimeError(
        "Failed to build Rust library. "
        "Install Rust first, then:\n"
        "  pip install -r requirements-dev.txt\n"
        "  cd tests/rust_bindings && maturin develop"
    ) from e

# Parametrize to run tests on all three implementations
all_implementations = [
    deduplicate_read_pairs,
    deduplicate_read_pairs_streaming,
    deduplicate_read_pairs_rust
]
all_implementation_ids = ["graph", "python-streaming", "rust"]


@pytest.mark.parametrize(
    "dedup_func",
    all_implementations,
    ids=all_implementation_ids
)
class TestDeduplication:
    """End-to-end tests for all deduplication implementations."""

    def test_empty_input(self, dedup_func):
        mapping = dedup_func([], verbose=False)
        assert mapping == {}

    def test_single_read(self, dedup_func):
        rp = ReadPair("read1", "AAAA", "TTTT", "IIII", "IIII")
        mapping = dedup_func([rp], verbose=False)
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

        mapping = dedup_func(read_pairs, verbose=False)

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

        mapping = dedup_func(read_pairs, verbose=False)

        # Each should be its own exemplar
        for read_id in mapping:
            assert mapping[read_id] == read_id

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

        mapping = dedup_func(read_pairs, verbose=False)

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
        mapping = dedup_func(read_pairs, dedup_params, verbose=False)

        exemplar1 = mapping["read1"]
        assert mapping["read2"] == exemplar1
        assert mapping["read3"] == exemplar1

        assert mapping["read4"] == "read4"

        exemplars = set(mapping.values())
        assert len(exemplars) == 2

    def test_approximate_match_parity(self, dedup_func):
        """Ensure Rust and Python handle mismatches identically."""
        seq = "A" * 100
        seq_error = "A" * 50 + "T" + "A" * 49  # 1 mismatch (1% error)

        # Should match with 2% error threshold
        rp1 = ReadPair("r1", seq, seq, "I"*100, "I"*100)
        rp2 = ReadPair("r2", seq_error, seq, "I"*100, "I"*100)

        # Explicit params to allow the error
        params = DedupParams(max_error_frac=0.02)
        mapping = dedup_func([rp1, rp2], dedup_params=params)

        # Should be clustered together
        assert mapping["r1"] == mapping["r2"]

    def test_approximate_match_threshold(self, dedup_func):
        """Ensure Rust and Python reject matches above error threshold."""
        seq = "A" * 100
        seq_error = "A" * 97 + "TTT"  # 3 mismatches (3% error)

        rp1 = ReadPair("r1", seq, seq, "I"*100, "I"*100)
        rp2 = ReadPair("r2", seq_error, seq, "I"*100, "I"*100)

        # With 2% threshold, should NOT match
        params = DedupParams(max_error_frac=0.02)
        mapping = dedup_func([rp1, rp2], dedup_params=params)

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
        mapping = dedup_func([rp1, rp2], dedup_params=params)

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
        mapping = dedup_func([rp1, rp2], dedup_params=params)

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
        mapping = dedup_func([rp1, rp2], dedup_params=params)

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
        mapping = dedup_func([rp1, rp2], dedup_params=params)

        # Should be separate clusters
        assert mapping["r1"] == "r1"
        assert mapping["r2"] == "r2"

        # But with 0.006 threshold, should match  (0.00503 <= 0.006)
        params2 = DedupParams(max_offset=1, max_error_frac=0.006)
        result2 = dedup_func([rp1, rp2], dedup_params=params2)
        mapping2 = result2

        # Should be clustered together
        assert mapping2["r1"] == mapping2["r2"]

    def test_cluster_lookup_after_leader_update(self, dedup_func):
        """
        Test that cluster lookups work correctly after the best_read_id is updated.

        This is a regression test for a bug where the cluster hash table uses the
        initial exemplar ID as its key, but lookups incorrectly compared against
        best_read_id, which can change during processing.

        Scenario:
        1. Read A (low quality) creates cluster keyed by "readA"
        2. Read B (high quality) matches A, updates best_read_id to "readB"
        3. Read C (low quality) matches A, should find the same cluster

        The bug caused step 3 to fail because it hashed "readA" but compared
        against best_read_id "readB", creating a duplicate cluster instead.
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
        mapping = dedup_func(read_pairs, verbose=False)

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

        mapping = dedup_func(read_pairs, verbose=False)

        # All should map to the same cluster
        exemplars = set(mapping.values())
        assert len(exemplars) == 1, f"Expected 1 cluster, got {len(exemplars)}: {exemplars}"

        # All should map to read3 (highest quality)
        for read_id in ["read1", "read2", "read3", "read4", "read5"]:
            assert mapping[read_id] == "read3", f"{read_id} mapped to {mapping[read_id]}, expected read3"

    def test_best_read_selection_quality_length_tiebreaker(self, dedup_func):
        """
        Test that when reads have the same quality, the longer one is chosen as
        exemplar.

        This test verifies the scoring formula: score = mean_quality * 1000 +
        length.

        Scenario:
        1. Read A (shorter, Q=30) creates cluster
        2. Read B (longer, Q=30) matches A and should become the exemplar

        Since "better" is quality*1000 + length, B should become the exemplar.
        """
        # Create identical sequence content, but different lengths
        # Use same quality (Q=30, which is '?' in Phred+33) for both
        qual_char = "?"  # Q=30

        # Shorter read: 100 bases
        seq_short = "A" * 100
        rpA = ReadPair(
            "readA", seq_short, seq_short, qual_char * 100, qual_char * 100)

        # Longer read: 150 bases
        seq_long = "A" * 150
        rpB = ReadPair(
            "readB", seq_long, seq_long, qual_char * 150, qual_char * 150)

        # Verify they have the same mean quality
        assert abs(rpA.mean_qual() - rpB.mean_qual()) < 0.01, \
            f"Reads should have same quality: A={rpA.mean_qual()}, " \
            f"B={rpB.mean_qual()}"

        # Process shorter first, then longer
        read_pairs = [rpA, rpB]
        mapping = dedup_func(read_pairs, verbose=False)

        # Both should cluster together
        assert mapping["readA"] == mapping["readB"], \
            f"Reads should cluster together: A->{mapping['readA']}, "\
            f"B->{mapping['readB']}"

        # The longer read (readB) should be chosen as the exemplar
        assert mapping["readA"] == "readB", \
            f"Expected readB (longer) as exemplar, but got {mapping['readA']}"
        assert mapping["readB"] == "readB"

    def test_windows_with_all_ns(self, dedup_func):
        """
        Test that sequences with windows containing all N's are handled correctly.

        Edge case: When a window has all N's (or enough N's that no valid k-mer
        can be formed), Python's _extract_minimizer returns EMPTY_KMER_SENTINEL_HASH
        while Rust's extract_minimizers skips the window (doesn't add to vector).

        Both produce the same clustering behavior in this test because the reads
        share valid minimizers in other windows. However, in pathological cases
        where reads ONLY share N-windows (no valid minimizers in common), Python
        would cluster them (via shared sentinel bucket keys) while Rust would not
        (no bucket keys in common). This is an acceptable edge case difference.
        """
        # With default params: num_windows=3/4, window_len=25
        # Create sequences where middle window is all N's
        # Format: [good bases][all N's][good bases]
        seq_with_ns = "A" * 30 + "N" * 30 + "C" * 90

        # Two identical sequences with N windows
        rp1 = ReadPair("r1", seq_with_ns, seq_with_ns, "I" * 150, "I" * 150)
        rp2 = ReadPair("r2", seq_with_ns, seq_with_ns, "I" * 150, "I" * 150)

        mapping = dedup_func([rp1, rp2], verbose=False)

        # Should be clustered together despite N windows
        assert mapping["r1"] == mapping["r2"], \
            f"Sequences with N windows not clustered: r1->{mapping['r1']}, r2->{mapping['r2']}"

    def test_all_windows_ns(self, dedup_func):
        """
        Test sequences where ALL windows contain only N's.

        Edge case: Implementations behave differently for all-N reads:
        - Graph: All-N reads share bucket keys (with sentinel values) so they
          cluster together
        - Python streaming and Rust: All-N reads produce no valid comparisons,
          so each is separate. (Python creates sentinel bucket keys but the
          streaming algorithm filters them out; Rust skips N-windows entirely.)
        """
        # Create a sequence of all N's
        all_ns = "N" * 150

        # Two sequences of all N's
        rp1 = ReadPair("r1", all_ns, all_ns, "I" * 150, "I" * 150)
        rp2 = ReadPair("r2", all_ns, all_ns, "I" * 150, "I" * 150)

        # Also add a normal sequence
        normal_seq = "A" * 150
        rp3 = ReadPair("r3", normal_seq, normal_seq, "I" * 150, "I" * 150)

        mapping = dedup_func([rp1, rp2, rp3], verbose=False)

        # r3 should always be its own cluster
        assert mapping["r3"] == "r3"

        # All-N reads behave differently:
        if dedup_func == deduplicate_read_pairs:  # graph only
            # Graph: all-N reads share bucket keys, so they cluster together
            assert mapping["r1"] == mapping["r2"], "Graph should cluster all-N reads"
        else:  # Python streaming and Rust
            # Both produce no valid comparisons for all-N reads
            assert mapping["r1"] == "r1", "r1 should be its own exemplar"
            assert mapping["r2"] == "r2", "r2 should be its own exemplar"

    def test_adapter_orientation_swapped_deduplication(self, dedup_func):
        """
        Verify that the same DNA fragment with adapters in opposite orientations
        is correctly identified as a duplicate.

        When adapters attach to a double-stranded insert in opposite orientations,
        we get reads that are swapped but NOT reverse complemented:

        Orientation alpha (P5 on top strand):
            Forward read: beginning of top strand
            Reverse read: beginning of bottom strand (reported as-is by sequencer)

        Orientation beta (P5 on bottom strand):
            Forward read: beginning of bottom strand
            Reverse read: beginning of top strand (reported as-is by sequencer)

        This means: (F_alpha, R_alpha) should match (R_beta, F_beta)
        with NO reverse complement needed.

        Example with GATTACA insert (using longer sequences for realistic minimizers):
        """
        # Create a 150bp "insert" - this represents the actual DNA fragment
        # Use a pattern that's clearly directional (not palindromic)
        insert_top = "GATTACA" * 21 + "GAT"  # 150bp total
        insert_bottom = _reverse_complement(insert_top)

        qual = "I" * 150

        # Orientation alpha: P5 attached to top strand
        # Forward reads from top strand, reverse reads from bottom strand
        fwd_alpha = insert_top
        rev_alpha = insert_bottom

        # Orientation beta: P5 attached to bottom strand (insert rotated 180°)
        # Forward reads from bottom strand, reverse reads from top strand
        fwd_beta = insert_bottom
        rev_beta = insert_top

        rp1 = ReadPair("r1", fwd_alpha, rev_alpha, qual, qual)
        rp2 = ReadPair("r2", fwd_beta, rev_beta, qual, qual)

        # Verify they're actually swapped (not identical)
        assert fwd_alpha == rev_beta
        assert rev_alpha == fwd_beta
        assert fwd_alpha != fwd_beta  # Different orientations

        mapping = dedup_func([rp1, rp2], verbose=False)

        # They should be detected as duplicates (tolerant mode)
        assert mapping["r1"] == mapping["r2"], \
            f"Adapter-swapped orientations not deduplicated. " \
            f"R1: {mapping['r1']}, R2: {mapping['r2']}"

    def test_windowing_strategy_beginning_of_read(self, dedup_func):
        """
        Verify that windowing strategy anchors to the beginning of the read.

        Both implementations use adjacent windows starting from position 0,
        focusing on the most stable region of the read (since quality drops off
        as you get farther into a read, so more trimming is likely.

        With num_windows=3, window_len=25 on 150bp reads:
        Windows: [0-25], [25-50], [50-75]

        Case 1: Similarity only in the tail [75-150] → miss
        Case 2: Similarity in window 1 [25-50] → detect
        """
        # Use parameters that allow 9 mismatches in 150bp (6% error)
        m_params = MinimizerParams(num_windows=3, window_len=25, kmer_len=7)
        d_params = DedupParams(max_offset=0, max_error_frac=0.07)
        read_len = 150
        base_seq = "C" * read_len
        qual = "I" * read_len

        # --- Case 1: Both miss (tail-only similarity) ---
        # Break all k-mers in the first three windows [0-75]
        # but leave the tail [75-150] clean.
        read2_tail_only_list = list(base_seq)
        # 3 changes per 25bp window to disrupt all 7-mers
        for idx in [6, 13, 20, 31, 38, 45, 56, 63, 70]:
            read2_tail_only_list[idx] = "T"
        read2_tail_only = "".join(read2_tail_only_list)

        rp1 = ReadPair("r1", base_seq, base_seq, qual, qual)
        rp2_tail = ReadPair("r2_tail", read2_tail_only, read2_tail_only, qual, qual)

        # Both implementations: All windows contain mismatches -> No shared
        # minimizers
        mapping = dedup_func([rp1, rp2_tail], d_params, m_params, verbose=False)
        assert mapping["r1"] != mapping["r2_tail"], \
            "Should miss tail-only similarity (windows only cover first 75bp)"

        # --- Case 2: Both hit (window 1 has similarity) ---
        # Break k-mers in windows 0 and 2, but leave window 1 [25-50] clean
        read2_mid_match_list = list(base_seq)
        for idx in [6, 13, 20, 56, 63, 70]:
            read2_mid_match_list[idx] = "T"
        read2_mid_match = "".join(read2_mid_match_list)

        rp2_mid = ReadPair("r2_mid", read2_mid_match, read2_mid_match, qual, qual)

        # Both implementations: Window 1 [25-50] is intact -> Shares minimizer
        mapping2 = dedup_func([rp1, rp2_mid], d_params, m_params, verbose=False)
        assert mapping2["r1"] == mapping2["r2_mid"], \
            "Should detect similarity via shared minimizer in window 1"


# ============================================================================
# Rust Binary End-to-End Tests
# ============================================================================

class TestRustBinary:
    """End-to-end tests for the Rust dedup_interleaved_fastq binary."""

    @pytest.fixture(scope="class")
    def binary_path(self):
        """Build the Rust binary and return its path."""
        project_root = Path(__file__).parent.parent

        # Build the binary in release mode for faster execution
        print("\nBuilding Rust binary...")
        result = subprocess.run(
            ["cargo", "build", "--release", "--bin", "dedup_interleaved_fastq"],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            pytest.fail(f"Failed to build Rust binary:\n{result.stderr}")

        binary = project_root / "target" / "release" / "dedup_interleaved_fastq"
        if not binary.exists():
            pytest.fail(f"Binary not found at {binary}")

        return binary

    def _write_fastq_gz(self, path: Path, records: list):
        """Write FASTQ records to a gzipped file.

        Args:
            path: Output file path
            records: List of (header, sequence, quality) tuples
        """
        with gzip.open(path, 'wt') as f:
            for header, sequence, quality in records:
                f.write(f"@{header}\n")
                f.write(f"{sequence}\n")
                f.write("+\n")
                f.write(f"{quality}\n")

    def _read_fastq_gz(self, path: Path) -> list:
        """Read FASTQ records from a gzipped file.

        Returns:
            List of (header, sequence, quality) tuples
        """
        records = []
        with gzip.open(path, 'rt') as f:
            while True:
                header = f.readline().strip()
                if not header:
                    break
                sequence = f.readline().strip()
                plus = f.readline().strip()
                quality = f.readline().strip()
                records.append((header[1:], sequence, quality))  # Remove @ from header
        return records

    def test_binary_basic_deduplication(self, binary_path, tmp_path):
        """Test that the binary correctly deduplicates identical read pairs."""
        # Create synthetic interleaved FASTQ data
        # Format: R1, R2, R1, R2, ...
        seq1_r1 = "ACGT" * 25  # 100bp
        seq1_r2 = "TGCA" * 25  # 100bp
        seq2_r1 = "GGGG" * 25  # Different sequence
        seq2_r2 = "CCCC" * 25
        qual = "I" * 100  # High quality

        # Create input: 5 read pairs
        # Pairs 0, 1, 2 are identical (cluster 1)
        # Pairs 3, 4 are identical (cluster 2)
        records = [
            # Pair 0 (cluster 1)
            ("read0_R1", seq1_r1, qual),
            ("read0_R2", seq1_r2, qual),
            # Pair 1 (cluster 1, duplicate)
            ("read1_R1", seq1_r1, qual),
            ("read1_R2", seq1_r2, qual),
            # Pair 2 (cluster 1, duplicate)
            ("read2_R1", seq1_r1, qual),
            ("read2_R2", seq1_r2, qual),
            # Pair 3 (cluster 2)
            ("read3_R1", seq2_r1, qual),
            ("read3_R2", seq2_r2, qual),
            # Pair 4 (cluster 2, duplicate)
            ("read4_R1", seq2_r1, qual),
            ("read4_R2", seq2_r2, qual),
        ]

        input_file = tmp_path / "input.fastq.gz"
        output_file = tmp_path / "output.fastq.gz"

        self._write_fastq_gz(input_file, records)

        # Run the binary
        result = subprocess.run(
            [str(binary_path), str(input_file), str(output_file)],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            pytest.fail(f"Binary failed:\n{result.stderr}")

        # Read output
        output_records = self._read_fastq_gz(output_file)

        # Should have 2 exemplar pairs (4 records total: 2 R1s + 2 R2s)
        assert len(output_records) == 4, \
            f"Expected 4 records (2 pairs), got {len(output_records)}"

        # Extract pairs from output (alternating R1/R2)
        output_pairs = []
        for i in range(0, len(output_records), 2):
            r1 = output_records[i]
            r2 = output_records[i + 1]
            output_pairs.append((r1, r2))

        # Should have exactly 2 unique pairs
        assert len(output_pairs) == 2, \
            f"Expected 2 output pairs, got {len(output_pairs)}"

        # Verify the sequences match our two unique sequences
        output_seqs = {(r1[1], r2[1]) for r1, r2 in output_pairs}
        expected_seqs = {(seq1_r1, seq1_r2), (seq2_r1, seq2_r2)}
        assert output_seqs == expected_seqs, \
            f"Output sequences don't match expected unique sequences"

    def test_binary_all_unique(self, binary_path, tmp_path):
        """Test that the binary handles all unique reads correctly."""
        rng = random.Random(12345)

        # Create 3 unique read pairs
        records = []
        for i in range(3):
            r1_seq = _random_seq(100, rng)
            r2_seq = _random_seq(100, rng)
            qual = "I" * 100
            records.extend([
                (f"read{i}_R1", r1_seq, qual),
                (f"read{i}_R2", r2_seq, qual),
            ])

        input_file = tmp_path / "input.fastq.gz"
        output_file = tmp_path / "output.fastq.gz"

        self._write_fastq_gz(input_file, records)

        # Run the binary
        result = subprocess.run(
            [str(binary_path), str(input_file), str(output_file)],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, f"Binary failed:\n{result.stderr}"

        # Read output
        output_records = self._read_fastq_gz(output_file)

        # All 3 pairs should be in output (6 records)
        assert len(output_records) == 6, \
            f"Expected 6 records (3 unique pairs), got {len(output_records)}"

    def test_binary_quality_selection(self, binary_path, tmp_path):
        """Test that the binary selects the highest quality read as exemplar."""
        seq = "ACGT" * 25  # 100bp

        # Create 3 identical pairs with different quality scores
        records = [
            # Pair 0: low quality (Q=10, char '+'
            ("read0_R1", seq, "+" * 100),
            ("read0_R2", seq, "+" * 100),
            # Pair 1: high quality (Q=40, char 'I')
            ("read1_R1", seq, "I" * 100),
            ("read1_R2", seq, "I" * 100),
            # Pair 2: medium quality (Q=20, char '5')
            ("read2_R1", seq, "5" * 100),
            ("read2_R2", seq, "5" * 100),
        ]

        input_file = tmp_path / "input.fastq.gz"
        output_file = tmp_path / "output.fastq.gz"

        self._write_fastq_gz(input_file, records)

        # Run the binary
        result = subprocess.run(
            [str(binary_path), str(input_file), str(output_file)],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, f"Binary failed:\n{result.stderr}"

        # Read output
        output_records = self._read_fastq_gz(output_file)

        # Should have 1 exemplar pair (2 records)
        assert len(output_records) == 2, \
            f"Expected 2 records (1 pair), got {len(output_records)}"

        # The exemplar should have high quality (all 'I')
        assert output_records[0][2] == "I" * 100, \
            f"Expected highest quality read as exemplar, got quality: {output_records[0][2][:10]}..."
        assert output_records[1][2] == "I" * 100, \
            f"Expected highest quality read as exemplar, got quality: {output_records[1][2][:10]}..."

    def test_binary_with_custom_params(self, binary_path, tmp_path):
        """Test that the binary accepts custom deduplication parameters."""
        # Create two slightly different sequences
        seq1 = "A" * 100
        seq2 = "A" * 97 + "TTT"  # 3 mismatches (3% error)
        qual = "I" * 100

        records = [
            ("read0_R1", seq1, qual),
            ("read0_R2", seq1, qual),
            ("read1_R1", seq2, qual),
            ("read1_R2", seq1, qual),  # Only R1 differs
        ]

        input_file = tmp_path / "input.fastq.gz"
        output_file = tmp_path / "output.fastq.gz"

        self._write_fastq_gz(input_file, records)

        # Run with strict error threshold (should NOT deduplicate)
        result = subprocess.run(
            [
                str(binary_path),
                str(input_file),
                str(output_file),
                "--max-error-frac", "0.02",  # 2% threshold - sequences have 3% error
            ],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, f"Binary failed:\n{result.stderr}"

        output_records = self._read_fastq_gz(output_file)

        # Should have 2 pairs (4 records) since they don't match with strict threshold
        assert len(output_records) == 4, \
            f"Expected 4 records (2 pairs), got {len(output_records)}"

        # Run with looser error threshold (should deduplicate)
        output_file2 = tmp_path / "output2.fastq.gz"
        result2 = subprocess.run(
            [
                str(binary_path),
                str(input_file),
                str(output_file2),
                "--max-error-frac", "0.04",  # 4% threshold - sequences have 3% error
            ],
            capture_output=True,
            text=True
        )

        assert result2.returncode == 0, f"Binary failed:\n{result2.stderr}"

        output_records2 = self._read_fastq_gz(output_file2)

        # Should have 1 pair (2 records) since they match with looser threshold
        assert len(output_records2) == 2, \
            f"Expected 2 records (1 pair), got {len(output_records2)}"

    def test_binary_empty_input(self, binary_path, tmp_path):
        """Test that the binary handles empty input files gracefully."""
        input_file = tmp_path / "empty.fastq.gz"
        output_file = tmp_path / "output.fastq.gz"

        # Create an empty gzipped file
        with gzip.open(input_file, 'wt') as f:
            pass  # Write nothing

        # Run the binary
        result = subprocess.run(
            [str(binary_path), str(input_file), str(output_file)],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, f"Binary failed on empty input:\n{result.stderr}"

        # Verify no NaN in output
        assert "NaN" not in result.stderr, \
            f"Binary output contains NaN: {result.stderr}"

        # Verify warning message for empty input
        assert "Warning: No reads found in input file" in result.stderr, \
            f"Expected warning about no reads in output: {result.stderr}"

        # Output should be empty
        output_records = self._read_fastq_gz(output_file)
        assert len(output_records) == 0, \
            f"Expected empty output for empty input, got {len(output_records)} records"
