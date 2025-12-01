use ahash::AHashMap;
use std::collections::HashMap;

// ============================================================================
// Configuration Parameters
// ============================================================================

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

#[derive(Debug, Clone)]
pub struct MinimizerParams {
    pub kmer_len: usize,
    pub window_len: usize,
    pub num_windows: usize,
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
        total as f64 / count
    }
}

// ============================================================================
// Minimizer Extraction
// ============================================================================

fn compute_kmer_hash(seq: &[u8], kmer_len: usize) -> u64 {
    let mut hash: u64 = 0;
    for &base in &seq[..kmer_len] {
        hash = hash.wrapping_mul(31).wrapping_add(base as u64);
    }
    hash
}

fn extract_minimizers(seq: &str, params: &MinimizerParams) -> Vec<u64> {
    let seq_bytes = seq.as_bytes();
    let seq_len = seq_bytes.len();

    if seq_len < params.kmer_len {
        return vec![];
    }

    let mut minimizers = Vec::with_capacity(params.num_windows);
    let step = if params.num_windows > 1 {
        (seq_len.saturating_sub(params.window_len)) / (params.num_windows - 1)
    } else {
        0
    };

    for i in 0..params.num_windows {
        let window_start = (i * step).min(seq_len.saturating_sub(params.window_len));
        let window_end = (window_start + params.window_len).min(seq_len);

        if window_end - window_start < params.kmer_len {
            break;
        }

        let mut min_hash = u64::MAX;
        for pos in window_start..=(window_end - params.kmer_len) {
            let hash = compute_kmer_hash(&seq_bytes[pos..], params.kmer_len);
            min_hash = min_hash.min(hash);
        }

        if min_hash != u64::MAX {
            minimizers.push(min_hash);
        }
    }

    minimizers
}

// ============================================================================
// Similarity Checking
// ============================================================================

fn check_similarity(
    seq1: &str,
    seq2: &str,
    max_offset: usize,
    max_error_frac: f64,
) -> bool {
    let s1 = seq1.as_bytes();
    let s2 = seq2.as_bytes();

    // Try different offsets
    for offset in 0..=max_offset {
        // Try s1 ahead of s2
        if offset < s1.len() {
            let overlap_len = s1.len().min(s2.len() + offset) - offset;
            if overlap_len > 0 {
                let mismatches = s1[offset..offset + overlap_len]
                    .iter()
                    .zip(&s2[..overlap_len])
                    .filter(|(&a, &b)| a != b)
                    .count();

                // Offset counts as errors
                let total_errors = mismatches + offset;
                if (total_errors as f64) / (overlap_len as f64) <= max_error_frac {
                    return true;
                }
            }
        }

        // Try s2 ahead of s1
        if offset > 0 && offset < s2.len() {
            let overlap_len = s2.len().min(s1.len() + offset) - offset;
            if overlap_len > 0 {
                let mismatches = s2[offset..offset + overlap_len]
                    .iter()
                    .zip(&s1[..overlap_len])
                    .filter(|(&a, &b)| a != b)
                    .count();

                // Offset counts as errors
                let total_errors = mismatches + offset;
                if (total_errors as f64) / (overlap_len as f64) <= max_error_frac {
                    return true;
                }
            }
        }
    }

    false
}

fn reads_are_similar(
    rp1: &ReadPair,
    rp2: &ReadPair,
    dedup_params: &DedupParams,
) -> bool {
    // 1. Standard Orientation (Fwd-Fwd, Rev-Rev)
    if check_similarity(&rp1.fwd_seq, &rp2.fwd_seq, dedup_params.max_offset, dedup_params.max_error_frac)
        && check_similarity(&rp1.rev_seq, &rp2.rev_seq, dedup_params.max_offset, dedup_params.max_error_frac)
    {
        return true;
    }

    // 2. Tolerant/Swapped Orientation (Fwd-Rev, Rev-Fwd)
    // Matches Python's ORIENT_TOLERANT
    if check_similarity(&rp1.fwd_seq, &rp2.rev_seq, dedup_params.max_offset, dedup_params.max_error_frac)
        && check_similarity(&rp1.rev_seq, &rp2.fwd_seq, dedup_params.max_offset, dedup_params.max_error_frac)
    {
        return true;
    }

    false
}

// ============================================================================
// Cluster Stats
// ============================================================================

#[derive(Debug, Clone)]
struct ClusterStats {
    #[allow(dead_code)]
    key: String,           // Immutable: initial exemplar ID (used as hash key)
    best_read_id: String,  // Mutable: current best read ID
    best_score: f64,
    count: usize,
}

// ============================================================================
// Deduplication Context
// ============================================================================

pub struct DedupContext {
    dedup_params: DedupParams,
    minimizer_params: MinimizerParams,

    // Minimizer buckets: minimizer -> list of exemplar IDs
    buckets: AHashMap<u64, Vec<String>>,

    // Exemplar storage: Only store ReadPair data for exemplars
    exemplar_store: HashMap<String, ReadPair>,

    // Result mapping: read_id -> exemplar_id (grows linearly with total reads)
    results: HashMap<String, String>,

    // Cluster stats: initial_exemplar_id -> ClusterStats
    clusters: HashMap<String, ClusterStats>,

    // Finalized flag
    finalized: bool,
}

impl DedupContext {
    pub fn new(dedup_params: DedupParams, minimizer_params: MinimizerParams) -> Self {
        Self {
            dedup_params,
            minimizer_params,
            buckets: AHashMap::new(),
            exemplar_store: HashMap::new(),
            results: HashMap::new(),
            clusters: HashMap::new(),
            finalized: false,
        }
    }

    pub fn process_read(&mut self, read_pair: ReadPair) -> String {
        let read_id = read_pair.read_id.clone();
        let mean_q = read_pair.mean_quality();

        // Extract minimizers from both forward and reverse
        let fwd_mins = extract_minimizers(&read_pair.fwd_seq, &self.minimizer_params);
        let rev_mins = extract_minimizers(&read_pair.rev_seq, &self.minimizer_params);

        // Combine minimizers
        let mut all_mins = fwd_mins;
        all_mins.extend(rev_mins);

        // Find matching exemplar by checking buckets (use FIRST match found)
        let mut matching_exemplar: Option<String> = None;

        'outer: for &min_hash in &all_mins {
            if let Some(bucket_reads) = self.buckets.get(&min_hash) {
                for candidate_id in bucket_reads {
                    // Look up candidate in exemplar_store (not all reads)
                    if let Some(candidate) = self.exemplar_store.get(candidate_id) {
                        // Check if reads are similar
                        if reads_are_similar(&read_pair, candidate, &self.dedup_params) {
                            // Get the exemplar for this candidate
                            let candidate_exemplar = self.results.get(candidate_id)
                                .cloned()
                                .unwrap_or_else(|| candidate_id.clone());

                            matching_exemplar = Some(candidate_exemplar);
                            break 'outer;  // Use first match found
                        }
                    }
                }
            }
        }

        let exemplar_id = if let Some(exemplar) = matching_exemplar {
            // Found a match - add to existing cluster
            if let Some(cluster) = self.clusters.get_mut(&exemplar) {
                cluster.count += 1;
                // Update best read if this one is better
                if mean_q > cluster.best_score {
                    cluster.best_read_id = read_id.clone();
                    cluster.best_score = mean_q;
                }
            }
            exemplar
        } else {
            // New unique sequence - create new cluster
            self.clusters.insert(
                read_id.clone(),
                ClusterStats {
                    key: read_id.clone(),
                    best_read_id: read_id.clone(),
                    best_score: mean_q,
                    count: 1,
                },
            );

            // Store the actual ReadPair data (only for new exemplars)
            self.exemplar_store.insert(read_id.clone(), read_pair);

            // Add read to minimizer buckets (only for new exemplars)
            for &min_hash in &all_mins {
                self.buckets.entry(min_hash).or_insert_with(Vec::new).push(read_id.clone());
            }

            read_id.clone()
        };

        // Record the result mapping
        self.results.insert(read_id.clone(), exemplar_id.clone());

        exemplar_id
    }

    pub fn finalize(&mut self) {
        // Resolve exemplars to cluster best reads
        let mut final_results = HashMap::new();

        for read_id in self.results.keys() {
            // Get the cluster this read was assigned to
            let cluster_key = self.results.get(read_id)
                .cloned()
                .unwrap_or_else(|| read_id.clone());

            // Get the best read from that cluster
            let final_exemplar = if let Some(cluster) = self.clusters.get(&cluster_key) {
                cluster.best_read_id.clone()
            } else {
                // No cluster found, use the key itself
                cluster_key
            };

            final_results.insert(read_id.clone(), final_exemplar);
        }

        self.results = final_results;
        self.finalized = true;

        // Clear buckets and exemplar store to free memory (no longer needed)
        self.buckets.clear();
        self.exemplar_store.clear();
    }

    pub fn get_final_exemplar(&self, read_id: &str) -> String {
        self.results.get(read_id)
            .cloned()
            .unwrap_or_else(|| read_id.to_string())
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
) -> HashMap<String, String> {
    let dedup_params = dedup_params.unwrap_or_default();
    let minimizer_params = minimizer_params.unwrap_or_default();

    let mut ctx = DedupContext::new(dedup_params, minimizer_params);

    // Process all reads
    for rp in read_pairs {
        ctx.process_read(rp);
    }

    // Finalize
    ctx.finalize();

    // Return the results (already built during processing)
    ctx.results.clone()
}
