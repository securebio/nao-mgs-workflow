/*
 * nao_dedup - High-performance sequence deduplication library
 * Implementation
 */

#include "nao_dedup.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

// ============================================================================
// Internal Data Structures
// ============================================================================

// Nucleotide lookup table for fast hashing (compile-time initialized, thread-safe)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winitializer-overrides"
static const uint8_t NT_TABLE[256] = {
    [0 ... 255] = 255,
    ['A'] = 0, ['C'] = 1, ['G'] = 2, ['T'] = 3,
    ['a'] = 0, ['c'] = 1, ['g'] = 2, ['t'] = 3
};
#pragma clang diagnostic pop

// Arena allocator for efficient memory management
typedef struct {
    char* base;
    char* current;
    size_t size;
    size_t used;
} Arena;

// Exemplar: a representative read for a similarity cluster
typedef struct Exemplar {
    char* read_id;
    char* fwd_seq;
    char* rev_seq;
    int fwd_len;
    int rev_len;
    struct Exemplar* next;
} Exemplar;

// Cluster statistics for finding best exemplar
typedef struct ClusterStats {
    char* key;              // Immutable: initial exemplar ID (hash table key)
    char* best_read_id;     // Mutable: current best read ID
    double best_score;
    int count;
    struct ClusterStats* next;
} ClusterStats;

// Hash table bucket for exemplars (now using linked list instead of dynamic array)
typedef struct ExemplarBucket {
    Exemplar* head;  // Linked list of exemplars
    int count;
} ExemplarBucket;

// Exemplar database (hash table)
typedef struct {
    ExemplarBucket* buckets;
    int size;
} ExemplarDB;

// Cluster leaders (hash table)
typedef struct {
    ClusterStats** buckets;
    int size;
    int count;
} ClusterLeaders;

// Read-to-exemplar mapping entry
typedef struct MappingEntry {
    char* read_id;
    char* exemplar_id;
    struct MappingEntry* next;
} MappingEntry;

// Read-to-exemplar mapping (hash table)
typedef struct {
    MappingEntry** buckets;
    int size;
} ReadToExemplar;

// Main context structure
struct NaoDedupContext {
    // Configuration
    NaoDedupParams params;
    int hash_table_size;

    // Arenas
    Arena* scratch_arena;  // Freed after finalize
    Arena* result_arena;   // Kept for lookups

    // Data structures
    ExemplarDB* exemplar_db;
    ClusterLeaders* cluster_leaders;
    ReadToExemplar* read_to_exemplar;

    // State
    int finalized;
    int total_reads;

    // Last error
    const char* error_msg;
};

// Thread-local error message for creation failures
static __thread const char* last_error_msg = NULL;

// ============================================================================
// Helper Functions
// ============================================================================

// init_tables() removed - NT_TABLE is now compile-time initialized

// Compute hash table size from expected reads
static int compute_hash_table_size(size_t expected_reads) {
    // Use a prime number slightly larger than expected_reads
    // For 20M reads, we use 16777259
    size_t target = expected_reads * 1.2;

    // Find next prime (simple approximation)
    if (target < 1000) return 1009;
    if (target < 10000) return 10007;
    if (target < 100000) return 100003;
    if (target < 1000000) return 1000003;
    if (target < 10000000) return 10000019;
    return 16777259;  // Good for up to ~20M reads
}

// ============================================================================
// Arena Allocator
// ============================================================================

static Arena* arena_create(size_t size) {
    Arena* arena = malloc(sizeof(Arena));
    if (!arena) return NULL;

    arena->base = malloc(size);
    if (!arena->base) {
        free(arena);
        return NULL;
    }

    arena->current = arena->base;
    arena->size = size;
    arena->used = 0;
    return arena;
}

static void arena_destroy(Arena* arena) {
    if (!arena) return;
    free(arena->base);
    free(arena);
}

static void* arena_alloc(Arena* arena, size_t size) {
    // Align to 8 bytes
    size = (size + 7) & ~7;

    if (arena->used + size > arena->size) {
        return NULL;  // Out of memory
    }

    void* ptr = arena->current;
    arena->current += size;
    arena->used += size;
    return ptr;
}

static char* arena_strdup(Arena* arena, const char* str) {
    size_t len = strlen(str) + 1;
    char* copy = arena_alloc(arena, len);
    if (!copy) return NULL;
    memcpy(copy, str, len);
    return copy;
}

static char* arena_strndup(Arena* arena, const char* str, size_t n) {
    char* copy = arena_alloc(arena, n + 1);
    if (!copy) return NULL;
    memcpy(copy, str, n);
    copy[n] = '\0';
    return copy;
}

// ============================================================================
// Hash Functions
// ============================================================================

static uint64_t hash_string_n(const char* str, size_t len) {
    uint64_t hash = 5381;
    for (size_t i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + (unsigned char)str[i];
    }
    return hash;
}

static uint64_t hash_kmer_fast(const char* seq, int start, int len, int seq_len) {
    if (start + len > seq_len) return 0;

    uint64_t hash = 0;
    for (int i = 0; i < len; i++) {
        uint8_t base = NT_TABLE[(unsigned char)seq[start + i]];
        if (base == 255) return 0;  // Invalid base (N or other non-ACGT)
        hash = (hash << 2) | base;
    }
    return hash == 0 ? 1 : hash;  // Avoid returning 0 for valid all-A k-mers
}

// ============================================================================
// Quality and Scoring
// ============================================================================

static double calculate_mean_quality(const char* qual, int len) {
    if (len == 0 || !qual) return 0.0;

    double sum = 0.0;
    for (int i = 0; i < len; i++) {
        sum += (qual[i] - 33);  // Phred+33 encoding
    }
    return sum / len;
}

static double calculate_score(int fwd_len, int rev_len, double mean_qual) {
    return (fwd_len + rev_len) + mean_qual;
}

// ============================================================================
// Minimizer Extraction
// ============================================================================

static uint64_t extract_minimizer_fast(const char* seq, int seq_len,
                                       int window_idx, int kmer_len, int window_len) {
    int window_start = window_idx * window_len;

    if (window_start + kmer_len > seq_len) return 0;

    uint64_t min_hash = UINT64_MAX;
    int limit = window_start + window_len - kmer_len;
    if (limit > seq_len - kmer_len) {
        limit = seq_len - kmer_len;
    }

    for (int i = window_start; i <= limit; i++) {
        uint64_t h = hash_kmer_fast(seq, i, kmer_len, seq_len);
        if (h > 0 && h < min_hash) {
            min_hash = h;
        }
    }

    return (min_hash == UINT64_MAX) ? 0 : min_hash;
}

static int get_stable_keys(const char* fwd_seq, int fwd_len,
                           const char* rev_seq, int rev_len,
                           uint64_t* keys, int kmer_len,
                           int window_len, int num_windows) {
    int num_keys = 0;

    for (int i = 0; i < num_windows; i++) {
        uint64_t k_fwd = extract_minimizer_fast(fwd_seq, fwd_len, i, kmer_len, window_len);
        uint64_t k_rev = extract_minimizer_fast(rev_seq, rev_len, i, kmer_len, window_len);

        if (k_fwd > 0) {
            keys[num_keys++] = k_fwd;
        }
        if (k_rev > 0) {
            keys[num_keys++] = k_rev;
        }
    }

    return num_keys;
}

// ============================================================================
// Sequence Matching (Python-compatible offset-based algorithm)
// ============================================================================

static int mismatch_count(const char* s1, const char* s2, int len) {
    int mismatches = 0;
    for (int i = 0; i < len; i++) {
        if (s1[i] != s2[i]) mismatches++;
    }
    return mismatches;
}

static int sequences_match(const char* seq1, int len1,
                           const char* seq2, int len2,
                           int max_offset, double max_error_frac) {
    // Handle empty sequences
    if (len1 == 0 && len2 == 0) return 1;

    // Try all offsets from -max_offset to +max_offset
    for (int offset = -max_offset; offset <= max_offset; offset++) {
        int mismatches;
        int overlap_len;

        if (offset >= 0) {
            // seq1 shifted left: seq1[offset:] aligns with seq2[0:]
            overlap_len = (len1 - offset < len2) ? len1 - offset : len2;
            if (overlap_len <= 0) continue;
            mismatches = mismatch_count(seq1 + offset, seq2, overlap_len);
        } else {
            // seq1 shifted right: seq1[0:] aligns with seq2[-offset:]
            overlap_len = (len1 < len2 + offset) ? len1 : len2 + offset;
            if (overlap_len <= 0) continue;
            mismatches = mismatch_count(seq1, seq2 - offset, overlap_len);
        }

        // Check if this alignment is within error threshold
        // offset counts as errors, plus actual mismatches
        int abs_offset = (offset < 0) ? -offset : offset;
        if (abs_offset + mismatches <= max_error_frac * overlap_len) {
            return 1;
        }
    }

    return 0;
}

static int read_matches_exemplar(const char* fwd_seq, int fwd_len,
                                 const char* rev_seq, int rev_len,
                                 const Exemplar* ex,
                                 int max_offset, double max_error_frac) {
    // Check standard orientation (fwd-fwd, rev-rev)
    if (sequences_match(fwd_seq, fwd_len, ex->fwd_seq, ex->fwd_len, max_offset, max_error_frac) &&
        sequences_match(rev_seq, rev_len, ex->rev_seq, ex->rev_len, max_offset, max_error_frac)) {
        return 1;
    }

    // Check swapped orientation (fwd-rev, rev-fwd) - tolerant mode
    if (sequences_match(fwd_seq, fwd_len, ex->rev_seq, ex->rev_len, max_offset, max_error_frac) &&
        sequences_match(rev_seq, rev_len, ex->fwd_seq, ex->fwd_len, max_offset, max_error_frac)) {
        return 1;
    }

    return 0;
}

// ============================================================================
// Exemplar Database
// ============================================================================

static ExemplarDB* init_exemplar_db(int size) {
    ExemplarDB* db = malloc(sizeof(ExemplarDB));
    if (!db) return NULL;

    db->size = size;
    db->buckets = calloc(size, sizeof(ExemplarBucket));
    if (!db->buckets) {
        free(db);
        return NULL;
    }

    return db;
}

static void add_exemplar(ExemplarDB* db, Arena* arena, uint64_t key,
                        const char* read_id, size_t id_len,
                        const char* fwd_seq, int fwd_len,
                        const char* rev_seq, int rev_len) {
    int bucket_idx = key % db->size;
    ExemplarBucket* bucket = &db->buckets[bucket_idx];

    // Allocate new exemplar from arena
    Exemplar* ex = arena_alloc(arena, sizeof(Exemplar));
    if (!ex) return;

    ex->read_id = arena_strndup(arena, read_id, id_len);
    ex->fwd_seq = arena_strndup(arena, fwd_seq, fwd_len);
    ex->rev_seq = arena_strndup(arena, rev_seq, rev_len);
    if (!ex->read_id || !ex->fwd_seq || !ex->rev_seq) return;

    ex->fwd_len = fwd_len;
    ex->rev_len = rev_len;

    // Insert at head of linked list (O(1))
    ex->next = bucket->head;
    bucket->head = ex;
    bucket->count++;
}

static const char* find_matching_exemplar(ExemplarDB* db,
                                         const char* fwd_seq, int fwd_len,
                                         const char* rev_seq, int rev_len,
                                         const uint64_t* keys, int num_keys,
                                         int max_offset, double max_error_frac) {
    for (int i = 0; i < num_keys; i++) {
        int bucket_idx = keys[i] % db->size;
        ExemplarBucket* bucket = &db->buckets[bucket_idx];

        // Iterate through linked list
        for (Exemplar* ex = bucket->head; ex != NULL; ex = ex->next) {
            if (read_matches_exemplar(fwd_seq, fwd_len, rev_seq, rev_len, ex, max_offset, max_error_frac)) {
                return ex->read_id;
            }
        }
    }

    return NULL;
}

static void destroy_exemplar_db(ExemplarDB* db) {
    if (!db) return;
    // Note: Exemplars are allocated in the arena, so we don't need to free them individually
    // The linked lists will be freed when the arena is destroyed
    free(db->buckets);
    free(db);
}

// ============================================================================
// Cluster Leaders
// ============================================================================

static ClusterLeaders* init_cluster_leaders(int size) {
    ClusterLeaders* cl = malloc(sizeof(ClusterLeaders));
    if (!cl) return NULL;

    cl->size = size;
    cl->count = 0;
    cl->buckets = calloc(size, sizeof(ClusterStats*));
    if (!cl->buckets) {
        free(cl);
        return NULL;
    }

    return cl;
}

static ClusterStats* get_or_create_cluster(ClusterLeaders* cl, Arena* arena,
                                           const char* exemplar_id, size_t ex_len) {
    uint64_t hash = hash_string_n(exemplar_id, ex_len);
    int bucket_idx = hash % cl->size;

    ClusterStats* curr = cl->buckets[bucket_idx];
    while (curr) {
        // Compare against the immutable key, not best_read_id which can change
        if (strncmp(curr->key, exemplar_id, ex_len) == 0 &&
            curr->key[ex_len] == '\0') {
            return curr;
        }
        curr = curr->next;
    }

    ClusterStats* stats = arena_alloc(arena, sizeof(ClusterStats));
    if (!stats) return NULL;

    // Store the immutable key
    stats->key = arena_strndup(arena, exemplar_id, ex_len);
    if (!stats->key) return NULL;

    // Initialize best_read_id to the same value initially
    stats->best_read_id = stats->key;
    stats->best_score = -1.0;
    stats->count = 0;
    stats->next = cl->buckets[bucket_idx];
    cl->buckets[bucket_idx] = stats;
    cl->count++;

    return stats;
}

// ============================================================================
// Read-to-Exemplar Mapping
// ============================================================================

static ReadToExemplar* init_read_to_exemplar(int size) {
    ReadToExemplar* rte = malloc(sizeof(ReadToExemplar));
    if (!rte) return NULL;

    rte->size = size;
    rte->buckets = calloc(size, sizeof(MappingEntry*));
    if (!rte->buckets) {
        free(rte);
        return NULL;
    }

    return rte;
}

static void add_read_mapping(ReadToExemplar* rte, Arena* arena,
                            const char* read_id, size_t id_len,
                            const char* exemplar_id) {
    uint64_t hash = hash_string_n(read_id, id_len);
    int bucket_idx = hash % rte->size;

    MappingEntry* entry = arena_alloc(arena, sizeof(MappingEntry));
    if (!entry) return;

    entry->read_id = arena_strndup(arena, read_id, id_len);
    entry->exemplar_id = arena_strdup(arena, exemplar_id);  // exemplar_id is internal, already null-terminated
    if (!entry->read_id || !entry->exemplar_id) return;

    entry->next = rte->buckets[bucket_idx];
    rte->buckets[bucket_idx] = entry;
}

static const char* find_exemplar(ReadToExemplar* rte, const char* read_id) {
    // read_id here is null-terminated (from Pass 2 user query)
    size_t len = strlen(read_id);
    uint64_t hash = hash_string_n(read_id, len);
    int bucket_idx = hash % rte->size;

    MappingEntry* curr = rte->buckets[bucket_idx];
    while (curr) {
        if (strcmp(curr->read_id, read_id) == 0) {
            return curr->exemplar_id;
        }
        curr = curr->next;
    }

    return NULL;
}

// ============================================================================
// Public API Implementation
// ============================================================================

NaoDedupContext* nao_dedup_create(NaoDedupParams params) {
    // Validate parameters
    if (params.kmer_len <= 0 || params.window_len <= 0 ||
        params.num_windows <= 0 || params.expected_reads == 0) {
        last_error_msg = "Invalid parameters";
        return NULL;
    }

    if (params.max_offset < 0) {
        last_error_msg = "max_offset must be >= 0";
        return NULL;
    }

    if (params.max_error_frac < 0.0 || params.max_error_frac > 1.0) {
        last_error_msg = "max_error_frac must be between 0.0 and 1.0";
        return NULL;
    }

    NaoDedupContext* ctx = calloc(1, sizeof(NaoDedupContext));
    if (!ctx) {
        last_error_msg = "Failed to allocate context";
        return NULL;
    }

    ctx->params = params;
    ctx->hash_table_size = compute_hash_table_size(params.expected_reads);

    // Create arenas
    ctx->scratch_arena = arena_create(2ULL * 1024 * 1024 * 1024);  // 2GB
    ctx->result_arena = arena_create(512ULL * 1024 * 1024);        // 512MB

    if (!ctx->scratch_arena || !ctx->result_arena) {
        last_error_msg = "Failed to allocate arenas";
        nao_dedup_destroy(ctx);
        return NULL;
    }

    // Initialize data structures
    ctx->exemplar_db = init_exemplar_db(ctx->hash_table_size);
    ctx->cluster_leaders = init_cluster_leaders(ctx->hash_table_size);
    ctx->read_to_exemplar = init_read_to_exemplar(ctx->hash_table_size);

    if (!ctx->exemplar_db || !ctx->cluster_leaders || !ctx->read_to_exemplar) {
        last_error_msg = "Failed to initialize data structures";
        nao_dedup_destroy(ctx);
        return NULL;
    }

    ctx->finalized = 0;
    ctx->total_reads = 0;

    return ctx;
}

void nao_dedup_destroy(NaoDedupContext* ctx) {
    if (!ctx) return;

    arena_destroy(ctx->scratch_arena);
    arena_destroy(ctx->result_arena);

    destroy_exemplar_db(ctx->exemplar_db);

    if (ctx->cluster_leaders) {
        free(ctx->cluster_leaders->buckets);
        free(ctx->cluster_leaders);
    }

    if (ctx->read_to_exemplar) {
        free(ctx->read_to_exemplar->buckets);
        free(ctx->read_to_exemplar);
    }

    free(ctx);
}

const char* nao_dedup_get_error_message(void) {
    return last_error_msg ? last_error_msg : "No error";
}

const char* nao_dedup_process_read(
    NaoDedupContext* ctx,
    const char* read_id, size_t id_len,
    const char* fwd_seq, size_t fwd_len,
    const char* rev_seq, size_t rev_len,
    const char* fwd_qual, size_t fwd_qual_len,
    const char* rev_qual, size_t rev_qual_len
) {
    if (!ctx || ctx->finalized) return read_id;

    ctx->total_reads++;

    // Calculate quality score
    double mean_qual = 0.0;
    if (fwd_qual && fwd_qual_len > 0 && rev_qual && rev_qual_len > 0) {
        mean_qual = (calculate_mean_quality(fwd_qual, fwd_qual_len) +
                    calculate_mean_quality(rev_qual, rev_qual_len)) / 2.0;
    }
    double score = calculate_score(fwd_len, rev_len, mean_qual);

    // Get minimizer keys
    uint64_t keys[ctx->params.num_windows * 2];
    int num_keys = get_stable_keys(fwd_seq, fwd_len, rev_seq, rev_len, keys,
                                   ctx->params.kmer_len, ctx->params.window_len,
                                   ctx->params.num_windows);

    if (num_keys == 0) {
        // No valid keys, treat as its own exemplar
        add_read_mapping(ctx->read_to_exemplar, ctx->result_arena, read_id, id_len, read_id);
        ClusterStats* stats = get_or_create_cluster(ctx->cluster_leaders,
                                                    ctx->result_arena, read_id, id_len);
        if (stats) {
            stats->best_read_id = arena_strndup(ctx->result_arena, read_id, id_len);
            stats->best_score = score;
            stats->count = 1;
        }
        return read_id;
    }

    // Check if matches existing exemplar
    const char* matching_exemplar = find_matching_exemplar(
        ctx->exemplar_db, fwd_seq, fwd_len, rev_seq, rev_len, keys, num_keys,
        ctx->params.max_offset, ctx->params.max_error_frac);

    if (matching_exemplar) {
        // Found a match
        add_read_mapping(ctx->read_to_exemplar, ctx->result_arena, read_id, id_len, matching_exemplar);
        ClusterStats* stats = get_or_create_cluster(ctx->cluster_leaders,
                                                    ctx->result_arena, matching_exemplar, strlen(matching_exemplar));
        if (stats) {
            stats->count++;
            if (score > stats->best_score) {
                stats->best_score = score;
                stats->best_read_id = arena_strndup(ctx->result_arena, read_id, id_len);
            }
        }
        return matching_exemplar;
    } else {
        // New unique sequence
        add_read_mapping(ctx->read_to_exemplar, ctx->result_arena, read_id, id_len, read_id);
        ClusterStats* stats = get_or_create_cluster(ctx->cluster_leaders,
                                                    ctx->result_arena, read_id, id_len);
        if (stats) {
            stats->best_read_id = arena_strndup(ctx->result_arena, read_id, id_len);
            stats->best_score = score;
            stats->count = 1;
        }

        // Add to exemplar database
        for (int i = 0; i < num_keys; i++) {
            add_exemplar(ctx->exemplar_db, ctx->scratch_arena, keys[i],
                       read_id, id_len, fwd_seq, fwd_len, rev_seq, rev_len);
        }

        return read_id;
    }
}

void nao_dedup_finalize(NaoDedupContext* ctx) {
    if (!ctx || ctx->finalized) return;

    // Free scratch arena (no longer needed)
    arena_destroy(ctx->scratch_arena);
    ctx->scratch_arena = NULL;

    // Free exemplar database (no longer needed)
    destroy_exemplar_db(ctx->exemplar_db);
    ctx->exemplar_db = NULL;

    ctx->finalized = 1;
}

const char* nao_dedup_get_final_exemplar(
    NaoDedupContext* ctx,
    const char* read_id
) {
    if (!ctx || !ctx->finalized) return read_id;

    const char* initial = find_exemplar(ctx->read_to_exemplar, read_id);
    if (!initial) return read_id;

    // Find cluster leader by the immutable key (initial is internal, null-terminated)
    size_t init_len = strlen(initial);
    uint64_t hash = hash_string_n(initial, init_len);
    int bucket_idx = hash % ctx->cluster_leaders->size;

    ClusterStats* curr = ctx->cluster_leaders->buckets[bucket_idx];
    while (curr) {
        // Compare against the immutable key, not best_read_id
        if (strcmp(curr->key, initial) == 0) {
            return curr->best_read_id;
        }
        curr = curr->next;
    }

    return initial;
}

void nao_dedup_get_stats(NaoDedupContext* ctx, NaoDedupStats* stats) {
    if (!ctx || !stats) return;

    stats->total_reads_processed = ctx->total_reads;
    stats->unique_clusters = ctx->cluster_leaders ? ctx->cluster_leaders->count : 0;
    stats->scratch_arena_used = ctx->scratch_arena ? ctx->scratch_arena->used : 0;
    stats->result_arena_used = ctx->result_arena ? ctx->result_arena->used : 0;
}
