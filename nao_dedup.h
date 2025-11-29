/*
 * nao_dedup - High-performance sequence deduplication library
 *
 * Provides similarity-based deduplication using minimizer hashing.
 */

#ifndef NAO_DEDUP_H
#define NAO_DEDUP_H

#include <stdint.h>
#include <stddef.h>

// Opaque handle to the deduplication engine
typedef struct NaoDedupContext NaoDedupContext;

// Configuration parameters
typedef struct {
    int kmer_len;           // K-mer length for minimizer hashing
    int window_len;         // Window length for minimizer extraction
    int num_windows;        // Number of windows to process per read
    int max_offset;         // Maximum alignment shift in bases (default: 1)
    double max_error_frac;  // Maximum mismatch fraction (default: 0.01)
    size_t expected_reads;  // Expected number of reads (for hash table sizing)
} NaoDedupParams;

// Statistics from deduplication
typedef struct {
    int total_reads_processed;
    int unique_clusters;
    size_t scratch_arena_used;
    size_t result_arena_used;
} NaoDedupStats;

// Error codes
typedef enum {
    NAO_DEDUP_OK = 0,
    NAO_DEDUP_OUT_OF_MEMORY,
    NAO_DEDUP_INVALID_PARAMS,
    NAO_DEDUP_NOT_FINALIZED
} NaoDedupError;

// ============================================================================
// Lifecycle Management
// ============================================================================

// Create a new deduplication context
// Returns NULL on error (check nao_dedup_get_error_message)
NaoDedupContext* nao_dedup_create(NaoDedupParams params);

// Destroy context and free all resources
void nao_dedup_destroy(NaoDedupContext* ctx);

// Get last error message (if creation failed)
const char* nao_dedup_get_error_message(void);

// ============================================================================
// Pass 1: Process reads and build similarity index
// ============================================================================

// Process a read pair and determine its exemplar
// Returns the exemplar ID for this read (may be the read itself)
// fwd_qual and rev_qual can be NULL (disables quality-based tiebreaking in
// exemplar selection)
// Strings do not need to be null-terminated; lengths are explicitly provided
const char* nao_dedup_process_read(
    NaoDedupContext* ctx,
    const char* read_id, size_t id_len,
    const char* fwd_seq, size_t fwd_len,
    const char* rev_seq, size_t rev_len,
    const char* fwd_qual, size_t fwd_qual_len,  // Optional, can be NULL/0
    const char* rev_qual, size_t rev_qual_len   // Optional, can be NULL/0
);

// Finalize Pass 1: compute cluster leaders, free scratch memory
// Must be called before nao_dedup_get_final_exemplar
void nao_dedup_finalize(NaoDedupContext* ctx);

// ============================================================================
// Pass 2: Query final exemplars
// ============================================================================

// Get the final cluster leader for a read
// Can only be called after nao_dedup_finalize()
// Returns the final exemplar ID, or the read_id itself if not found
const char* nao_dedup_get_final_exemplar(
    NaoDedupContext* ctx,
    const char* read_id
);

// ============================================================================
// Statistics
// ============================================================================

// Get statistics from the deduplication process
void nao_dedup_get_stats(NaoDedupContext* ctx, NaoDedupStats* stats);

#endif // NAO_DEDUP_H
