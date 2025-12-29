use ahash::{AHashMap, AHashSet};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

// ============================================================================
// Configuration Parameters
// ============================================================================

/// Parameters for deduplication matching.
///
/// Note: for simplicity, the Rust implementation always uses tolerant
/// orientation matching (allows swapped mate pairs). Unlike the Python
/// implementation, there is no strict orientation mode.
#[derive(Debug, Clone)]
pub struct DedupParams {
    pub max_offset: usize,
    pub max_error_frac: f64,
}

impl Default for DedupParams {
    fn default() -> Self {
        Self {
            max_offset: 1,
            max_error_frac: 0.01,
        }
    }
}

/// Parameters for minimizer extraction.
///
/// Note: The Rust defaults (kmer_len=15, num_windows=4) differ from Python
/// (kmer_len=7, num_windows=3) because Rust is expected to handle much larger
/// inputs where more selective minimizers reduce memory usage and comparisons.
#[derive(Debug, Clone)]
pub struct MinimizerParams {
    pub kmer_len: usize,
    pub window_len: usize,
    pub num_windows: usize,
}

impl MinimizerParams {
    /// Create new MinimizerParams with validation.
    pub fn new(kmer_len: usize, window_len: usize, num_windows: usize) -> Result<Self, String> {
        if kmer_len > 32 {
            return Err(format!(
                "k-mer length must be <= 32 for 2-bit encoding (fits in 64 bits), got {}",
                kmer_len
            ));
        }
        if kmer_len > window_len {
            return Err(format!(
                "kmer_len ({}) must be <= window_len ({})",
                kmer_len, window_len
            ));
        }
        Ok(Self {
            kmer_len,
            window_len,
            num_windows,
        })
    }
}

impl Default for MinimizerParams {
    fn default() -> Self {
        Self {
            kmer_len: 15,
            window_len: 25,
            num_windows: 4,
        }
    }
}

// ============================================================================
// Read Pair
// ============================================================================

#[derive(Debug, Clone)]
pub struct ReadPair {
    pub read_id: String,
    pub fwd_seq: String,
    pub rev_seq: String,
    pub fwd_qual: String,
    pub rev_qual: String,
}

impl ReadPair {
    pub fn mean_quality(&self) -> f64 {
        let total: u32 = self.fwd_qual.bytes().chain(self.rev_qual.bytes())
            .map(|b| (b - 33) as u32)
            .sum();
        let count = (self.fwd_qual.len() + self.rev_qual.len()) as f64;
        if count == 0.0 {
            return 0.0;
        }
        total as f64 / count
    }
}

/// Lightweight representation of an exemplar for similarity checking.
/// Only stores sequences (not quality strings) to reduce memory footprint.
#[derive(Clone)]
struct StoredExemplar {
    fwd_seq: String,
    rev_seq: String,
}

/// ID registry for interning read IDs to compact u32 indices.
/// Dramatically reduces memory usage and improves hash/comparison performance.
struct IDRegistry {
    id_to_index: AHashMap<String, u32>,
    index_to_id: Vec<String>,
}

impl IDRegistry {
    fn new() -> Self {
        Self {
            id_to_index: AHashMap::new(),
            index_to_id: Vec::new(),
        }
    }

    /// Get or create an index for a read ID
    fn get_or_create(&mut self, id: &str) -> u32 {
        if let Some(&idx) = self.id_to_index.get(id) {
            return idx;
        }
        let idx = self.index_to_id.len() as u32;
        self.index_to_id.push(id.to_string());
        self.id_to_index.insert(id.to_string(), idx);
        idx
    }

    /// Convert index back to read ID
    #[inline]
    fn get_id(&self, idx: u32) -> &str {
        &self.index_to_id[idx as usize]
    }
}

// ============================================================================
// Minimizer Extraction
//
// Strategy: Use 2-bit encoding (A=0,C=1,G=2,T=3) to pack k-mers into u64.
// This allows fast rolling hash computation and comparison.
// ============================================================================

// Lookup table for base encoding (faster than match statement)
// Maps ASCII byte values to 2-bit encodings: A/a=0, C/c=1, G/g=2, T/t=3
// Invalid bases (including N) are marked with u64::MAX
const ENCODE_LOOKUP: [u64; 256] = {
    let mut table = [u64::MAX; 256];
    table[b'A' as usize] = 0;
    table[b'a' as usize] = 0;
    table[b'C' as usize] = 1;
    table[b'c' as usize] = 1;
    table[b'G' as usize] = 2;
    table[b'g' as usize] = 2;
    table[b'T' as usize] = 3;
    table[b't' as usize] = 3;
    table
};

#[inline(always)]
fn encode_base(b: u8) -> Option<u64> {
    let encoded = ENCODE_LOOKUP[b as usize];
    if encoded == u64::MAX {
        None
    } else {
        Some(encoded)
    }
}

/// Extract one minimizer per window from a sequence.
///
/// Uses rolling hash to efficiently compute k-mer hashes.
/// Windows are adjacent starting from position 0, focusing on the most reliable
/// portion of the read (quality typically degrades toward the end).
fn extract_minimizers(seq: &str, params: &MinimizerParams) -> SmallVec<[u64; 8]> {
    let seq_bytes = seq.as_bytes();
    let seq_len = seq_bytes.len();

    if seq_len < params.kmer_len {
        return SmallVec::new();
    }

    // Mask to keep only the rightmost k*2 bits (each base uses 2 bits)
    let mask: u64 = if params.kmer_len == 32 {
        u64::MAX
    } else {
        (1u64 << (2 * params.kmer_len)) - 1
    };

    let mut minimizers = SmallVec::with_capacity(params.num_windows);


    // Use adjacent windows starting from the beginning of the read
    // This matches Python's strategy: window i covers
    // [i*window_len, (i+1)*window_len]
    for i in 0..params.num_windows {
        let window_start = i * params.window_len;

        if window_start >= seq_len {
            break;
        }

        let window_end = (window_start + params.window_len).min(seq_len);

        // Stop if this window would start beyond the sequence
        if window_end - window_start < params.kmer_len {
            break;
        }

        let mut min_hash = u64::MAX;
        let mut hash: u64 = 0;
        let mut valid_len: usize = 0;  // number of consecutive valid ACGT bases

        for pos in window_start..window_end {
            if let Some(encoded) = encode_base(seq_bytes[pos]) {
                // Update forward hash: shift left, add new base
                hash = ((hash << 2) | encoded) & mask;
                valid_len += 1;

                if valid_len >= params.kmer_len {
                    min_hash = min_hash.min(hash);
                }
            } else {
                // Non-ACGT base: reset
                hash = 0;
                valid_len = 0;
            }
        }

        if min_hash != u64::MAX {
            minimizers.push(min_hash);
        }
    }

    minimizers
}

// ============================================================================
// Similarity Checking
//
// Allow sequences to match with small alignment shifts (indels) and mismatches.
// The offset counts as error: e.g., 1bp offset + 1 mismatch = 2 errors total.
// ============================================================================

fn check_similarity(
    seq1: &str,
    seq2: &str,
    max_offset: usize,
    max_error_frac: f64,
) -> bool {
    let s1 = seq1.as_bytes();
    let s2 = seq2.as_bytes();

    // Optimized helper function with early exit for hot path performance
    #[inline]
    fn check_one_way(seqa: &[u8], seqb: &[u8], off: usize, max_error_frac: f64) -> bool {
        if off >= seqa.len() {
            return false;
        }
        let overlap_len = (seqa.len() - off).min(seqb.len());
        if overlap_len == 0 {
            return false;
        }

        // Pre-calculate error budget to avoid floating-point division in the loop
        let max_errors = (max_error_frac * overlap_len as f64).floor() as usize;
        if off > max_errors {
            return false; // Offset alone exceeds budget
        }

        let allowed_mismatches = max_errors - off;
        let mut mismatches = 0;

        let a_part = &seqa[off..off + overlap_len];
        let b_part = &seqb[..overlap_len];

        // Manual loop for early exit when error budget is exceeded
        for i in 0..overlap_len {
            if a_part[i] != b_part[i] {
                mismatches += 1;
                if mismatches > allowed_mismatches {
                    return false; // Short-circuit early!
                }
            }
        }
        true
    }

    for offset in 0..=max_offset {
        // Check with s1 shifted left relative to s2
        if check_one_way(s1, s2, offset, max_error_frac) {
            return true;
        }
        // Check with s2 shifted left relative to s1 (equivalent to s1 shifted
        // right)
        if offset > 0 && check_one_way(s2, s1, offset, max_error_frac) {
            return true;
        }
    }

    false
}

/// Check if two read pairs are similar enough to be duplicates.
///
/// Checks two orientations (matching Python's ORIENT_TOLERANT mode):
/// 1. Standard: (Fwd, Rev) vs (Fwd, Rev)
/// 2. Swapped: (Fwd, Rev) vs (Rev, Fwd)
///
/// The swapped check handles the case where adapters attached in the opposite
/// orientation, causing the same DNA fragment to be sequenced with forward/reverse
/// swapped. Note: Rust version always uses tolerant mode (no strict mode option).
fn reads_are_similar(
    rp: &ReadPair,
    exemplar: &StoredExemplar,
    dedup_params: &DedupParams,
) -> bool {
    if check_similarity(&rp.fwd_seq, &exemplar.fwd_seq, dedup_params.max_offset, dedup_params.max_error_frac)
        && check_similarity(&rp.rev_seq, &exemplar.rev_seq, dedup_params.max_offset, dedup_params.max_error_frac)
    {
        return true;
    }

    if check_similarity(&rp.fwd_seq, &exemplar.rev_seq, dedup_params.max_offset, dedup_params.max_error_frac)
        && check_similarity(&rp.rev_seq, &exemplar.fwd_seq, dedup_params.max_offset, dedup_params.max_error_frac)
    {
        return true;
    }

    false
}

// ============================================================================
// Deduplication Context
//
// Streaming algorithm: processes reads one at a time, storing only unique
// sequences (exemplars) rather than all reads. Memory usage scales with
// unique sequences, not total input size.
//
// Key invariant: Each cluster is identified by the read_id of its FIRST member
// (the initial exemplar). As we see better reads, we update best_read_id in
// ClusterStats, but the cluster key remains unchanged. This is crucial for
// lookups to work correctly.
// ============================================================================

#[derive(Debug, Clone)]
struct ClusterStats {
    best_read_idx: u32,  // Index of best read (can change as we see better reads)
    best_score: f64,
    count: usize,
}

pub struct DedupContext {
    dedup_params: DedupParams,
    minimizer_params: MinimizerParams,

    // ID interning: read IDs -> compact u32 indices for faster hashing/comparison
    id_registry: IDRegistry,

    // HashMap choices:
    // - FxHashMap for integer keys: u64/u32 keys are well-distributed
    //   integers, so we can use the ultra-fast FxHash (just a multiply + XOR)

    // minimizer -> list of read indices (instead of read IDs)
    buckets: FxHashMap<u64, Vec<u32>>,

    // read_idx -> read sequences (only for exemplars, quality strings omitted)
    exemplar_store: Vec<Option<StoredExemplar>>,

    // read_idx -> cluster_leader_idx (grows linearly with total reads)
    results: Vec<u32>,

    // cluster_leader_idx -> ClusterStats
    clusters: FxHashMap<u32, ClusterStats>,

    finalized: bool,
}

impl DedupContext {
    pub fn new(dedup_params: DedupParams, minimizer_params: MinimizerParams) -> Self {
        Self {
            dedup_params,
            minimizer_params,
            id_registry: IDRegistry::new(),
            buckets: FxHashMap::default(),
            exemplar_store: Vec::new(),
            results: Vec::new(),
            clusters: FxHashMap::default(),
            finalized: false,
        }
    }

    /// Process one read pair. Returns the cluster ID it was assigned to.
    ///
    /// Algorithm:
    /// 1. Extract minimizers and look up matching exemplars in buckets
    /// 2. Compare this read against candidates until we find a match
    /// 3a. If match found: add to existing cluster, potentially updating best_read_idx
    /// 3b. If no match: create new cluster with this read as initial exemplar
    pub fn process_read(&mut self, read_pair: ReadPair) -> String {
        // Intern the read ID to a compact u32 index
        let read_idx = self.id_registry.get_or_create(&read_pair.read_id);
        let mean_q = read_pair.mean_quality();

        // Calculate score: quality is primary (scaled by 1000), length is secondary
        let length = (read_pair.fwd_seq.len() + read_pair.rev_seq.len()) as f64;
        let score = mean_q * 1000.0 + length;

        let fwd_mins = extract_minimizers(&read_pair.fwd_seq, &self.minimizer_params);
        let rev_mins = extract_minimizers(&read_pair.rev_seq, &self.minimizer_params);

        let mut all_mins = fwd_mins;
        all_mins.extend(rev_mins);

        // Track which candidates we've already checked
        let mut checked_indices = AHashSet::new();
        let mut matching_cluster_idx: Option<u32> = None;

        'outer: for &min_hash in &all_mins {
            if let Some(bucket_reads) = self.buckets.get(&min_hash) {
                for &candidate_idx in bucket_reads {
                    if !checked_indices.insert(candidate_idx) {
                        continue;  // Already checked this candidate
                    }

                    if let Some(candidate) = self.exemplar_store.get(candidate_idx as usize).and_then(|opt| opt.as_ref()) {
                        if reads_are_similar(&read_pair, candidate, &self.dedup_params) {
                            // candidate_idx from buckets is always a cluster leader
                            matching_cluster_idx = Some(candidate_idx);
                            break 'outer;
                        }
                    }
                }
            }
        }

        let cluster_leader_idx = if let Some(cluster_idx) = matching_cluster_idx {
            // Found a match - add to existing cluster
            if let Some(cluster) = self.clusters.get_mut(&cluster_idx) {
                cluster.count += 1;
                // Update best read if this one is better
                if score > cluster.best_score {
                    cluster.best_read_idx = read_idx;
                    cluster.best_score = score;
                }
            }
            cluster_idx
        } else {
            // New unique sequence - create new cluster with this read as leader
            self.clusters.insert(
                read_idx,
                ClusterStats {
                    best_read_idx: read_idx,
                    best_score: score,
                    count: 1,
                },
            );

            // Ensure exemplar_store has space for this index
            if self.exemplar_store.len() <= read_idx as usize {
                self.exemplar_store.resize(read_idx as usize + 1, None);
            }

            // Store only sequences (not quality strings) to reduce memory footprint
            self.exemplar_store[read_idx as usize] = Some(StoredExemplar {
                fwd_seq: read_pair.fwd_seq,
                rev_seq: read_pair.rev_seq,
            });

            // Add read to minimizer buckets (only for new exemplars)
            for &min_hash in &all_mins {
                self.buckets.entry(min_hash).or_insert_with(Vec::new).push(read_idx);
            }

            read_idx
        };

        // Track this read's cluster assignment
        if self.results.len() <= read_idx as usize {
            self.results.resize(read_idx as usize + 1, 0);
        }
        self.results[read_idx as usize] = cluster_leader_idx;

        // Return the cluster leader's ID (as a String)
        self.id_registry.get_id(cluster_leader_idx).to_string()
    }

    /// Finalize results: resolve all reads to their cluster's best_read_idx.
    ///
    /// During streaming, reads point to the cluster leader index (first exemplar).
    /// After finalization, they point to the best exemplar found for that cluster.
    pub fn finalize(&mut self) {
        // Update each read's cluster assignment to point to the best exemplar
        for read_idx in 0..self.results.len() {
            let cluster_leader_idx = self.results[read_idx];
            if let Some(cluster) = self.clusters.get(&cluster_leader_idx) {
                self.results[read_idx] = cluster.best_read_idx;
            }
        }

        self.finalized = true;

        // Free memory for intermediate data structures
        self.buckets.clear();
        self.exemplar_store.clear();
    }

    pub fn get_cluster_id(&self, read_id: &str) -> String {
        if let Some(&read_idx) = self.id_registry.id_to_index.get(read_id) {
            if let Some(&cluster_idx) = self.results.get(read_idx as usize) {
                return self.id_registry.get_id(cluster_idx).to_string();
            }
        }
        read_id.to_string()
    }

    pub fn stats(&self) -> (usize, usize) {
        let total_reads = self.results.len();
        let unique_clusters = self.clusters.len();
        (total_reads, unique_clusters)
    }
}

// ============================================================================
// Public API
// ============================================================================

pub fn deduplicate_read_pairs(
    read_pairs: Vec<ReadPair>,
    dedup_params: Option<DedupParams>,
    minimizer_params: Option<MinimizerParams>,
) -> AHashMap<String, String> {
    let dedup_params = dedup_params.unwrap_or_default();
    let minimizer_params = minimizer_params.unwrap_or_default();

    let mut ctx = DedupContext::new(dedup_params, minimizer_params);

    for rp in read_pairs {
        ctx.process_read(rp);
    }

    ctx.finalize();

    // Convert internal index-based results to HashMap<String, String> for public API
    let mut result_map = AHashMap::with_capacity(ctx.results.len());
    for read_idx in 0..ctx.results.len() {
        let cluster_idx = ctx.results[read_idx];
        let read_id = ctx.id_registry.get_id(read_idx as u32);
        let cluster_id = ctx.id_registry.get_id(cluster_idx);
        result_map.insert(read_id.to_string(), cluster_id.to_string());
    }

    result_map
}
