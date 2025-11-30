/*
 * Similarity-based duplicate marking for alignment-unique reads.
 *
 * This is the TSV driver that uses the nao_dedup library for the heavy lifting.
 * It handles file I/O, TSV parsing, and business logic specific to our pipeline.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include "nao_dedup.h"

// TSV parsing constants
#define INITIAL_LINE_BUFFER 65536
#define MAX_ID_LEN 4096  // Long-read headers (Nanopore/PacBio) can exceed 256

// ============================================================================
// Growing Line Buffer (TSV I/O)
// ============================================================================

typedef struct {
    char* data;
    size_t size;
    size_t capacity;
} LineBuffer;

static LineBuffer* linebuf_create(void) {
    LineBuffer* lb = malloc(sizeof(LineBuffer));
    if (!lb) {
        fprintf(stderr, "Error: Failed to allocate line buffer\n");
        exit(1);
    }

    lb->capacity = INITIAL_LINE_BUFFER;
    lb->data = malloc(lb->capacity);
    if (!lb->data) {
        fprintf(stderr, "Error: Failed to allocate line buffer data\n");
        exit(1);
    }

    lb->size = 0;
    return lb;
}

static void linebuf_destroy(LineBuffer* lb) {
    free(lb->data);
    free(lb);
}

static char* gzgets_growing(gzFile fp, LineBuffer* lb) {
    lb->size = 0;

    while (1) {
        // Ensure we have space to read
        size_t remaining = lb->capacity - lb->size;
        if (remaining < 2) {
            // Need more space
            lb->capacity *= 2;
            char* new_data = realloc(lb->data, lb->capacity);
            if (!new_data) {
                fprintf(stderr, "Error: Failed to grow line buffer\n");
                exit(1);
            }
            lb->data = new_data;
            remaining = lb->capacity - lb->size;
        }

        // Try to read into current buffer
        char* result = gzgets(fp, lb->data + lb->size, remaining);
        if (!result) {
            // EOF or error
            if (lb->size == 0) return NULL;  // Nothing read at all
            break;  // EOF after partial read
        }

        size_t read_len = strlen(lb->data + lb->size);
        lb->size += read_len;

        // Check if we got a complete line (ends with newline or EOF)
        if (lb->size > 0 && lb->data[lb->size - 1] == '\n') {
            break;  // Complete line
        }

        // Check for EOF without newline (last line of file)
        if (gzeof(fp)) {
            break;
        }

        // Line continues, need more space
    }

    return lb->data;
}

// ============================================================================
// TSV Parsing
// ============================================================================

static int parse_tsv_line(char* line, char** fields, size_t* field_lens, int max_fields) {
    int field_count = 0;
    char* ptr = line;

    while (*ptr && field_count < max_fields) {
        char* field_start = ptr;
        fields[field_count] = field_start;

        // Find next tab or end of line
        while (*ptr && *ptr != '\t' && *ptr != '\n' && *ptr != '\r') {
            ptr++;
        }

        // Store length before null-terminating
        field_lens[field_count] = ptr - field_start;
        field_count++;

        if (*ptr) {
            *ptr = '\0';
            ptr++;
        }
    }

    return field_count;
}

// ============================================================================
// Pass 1: Process alignment-unique reads
// ============================================================================

static void process_alignment_unique_reads(
    const char* input_path,
    NaoDedupContext* deduper,
    int* total_reads
) {
    gzFile fp = gzopen(input_path, "r");
    if (!fp) {
        fprintf(stderr, "Error: Cannot open input file: %s\n", input_path);
        exit(1);
    }

    LineBuffer* lb = linebuf_create();
    char* fields[1024];  // Support annotation pipelines with many columns
    size_t field_lens[1024];  // Field lengths (avoid strlen in hot loop)

    // Read header
    if (!gzgets_growing(fp, lb)) {
        fprintf(stderr, "Error: Empty input file\n");
        gzclose(fp);
        exit(1);
    }

    // Find column indices
    int num_fields = parse_tsv_line(lb->data, fields, field_lens, 1024);
    int seq_id_idx = -1, query_seq_idx = -1, query_seq_rev_idx = -1;
    int query_qual_idx = -1, query_qual_rev_idx = -1, prim_align_idx = -1;

    for (int i = 0; i < num_fields; i++) {
        if (strcmp(fields[i], "seq_id") == 0) seq_id_idx = i;
        else if (strcmp(fields[i], "query_seq") == 0) query_seq_idx = i;
        else if (strcmp(fields[i], "query_seq_rev") == 0) query_seq_rev_idx = i;
        else if (strcmp(fields[i], "query_qual") == 0) query_qual_idx = i;
        else if (strcmp(fields[i], "query_qual_rev") == 0) query_qual_rev_idx = i;
        else if (strcmp(fields[i], "prim_align_dup_exemplar") == 0) prim_align_idx = i;
    }

    if (seq_id_idx < 0 || query_seq_idx < 0 || query_seq_rev_idx < 0 ||
        query_qual_idx < 0 || query_qual_rev_idx < 0 || prim_align_idx < 0) {
        fprintf(stderr, "Error: Missing required columns in input file\n");
        gzclose(fp);
        exit(1);
    }

    fprintf(stderr, "Running similarity-based deduplication on alignment-unique reads...\n");

    int alignment_unique_count = 0;

    // Process reads
    while (gzgets_growing(fp, lb)) {
        (*total_reads)++;

        int nf = parse_tsv_line(lb->data, fields, field_lens, 1024);
        if (nf <= prim_align_idx) continue;

        char* seq_id = fields[seq_id_idx];
        char* prim_align_exemplar = fields[prim_align_idx];

        // BUSINESS LOGIC: Only process alignment-unique reads
        if (strcmp(seq_id, prim_align_exemplar) != 0) {
            continue;
        }

        alignment_unique_count++;

        // Delegate to library (pass lengths to avoid strlen)
        nao_dedup_process_read(
            deduper,
            seq_id, field_lens[seq_id_idx],
            fields[query_seq_idx], field_lens[query_seq_idx],
            fields[query_seq_rev_idx], field_lens[query_seq_rev_idx],
            fields[query_qual_idx], field_lens[query_qual_idx],
            fields[query_qual_rev_idx], field_lens[query_qual_rev_idx]
        );
    }

    fprintf(stderr, "Processed %d alignment-unique reads (out of %d total reads)\n",
            alignment_unique_count, *total_reads);

    // Get stats from library
    NaoDedupStats stats;
    nao_dedup_get_stats(deduper, &stats);
    fprintf(stderr, "Found %d unique sequence clusters\n", stats.unique_clusters);

    gzclose(fp);
    linebuf_destroy(lb);
}

// ============================================================================
// Pass 2: Write output with sim_dup_exemplar column
// ============================================================================

static void write_output_with_sim_column(
    const char* input_path,
    const char* output_path,
    NaoDedupContext* deduper,
    int* n_prim_align_dups,
    int* n_sim_dups
) {
    gzFile fp_in = gzopen(input_path, "r");
    if (!fp_in) {
        fprintf(stderr, "Error: Cannot open input file: %s\n", input_path);
        exit(1);
    }

    gzFile fp_out = gzopen(output_path, "w");
    if (!fp_out) {
        fprintf(stderr, "Error: Cannot open output file: %s\n", output_path);
        gzclose(fp_in);
        exit(1);
    }

    fprintf(stderr, "Pass 2: Writing output with sim_dup_exemplar column...\n");

    LineBuffer* lb = linebuf_create();

    *n_prim_align_dups = 0;
    *n_sim_dups = 0;

    // Read and write header
    if (!gzgets_growing(fp_in, lb)) {
        fprintf(stderr, "Error: Empty input file\n");
        gzclose(fp_in);
        gzclose(fp_out);
        exit(1);
    }

    // Remove trailing newline from header
    int len = lb->size;
    while (len > 0 && (lb->data[len-1] == '\n' || lb->data[len-1] == '\r')) {
        lb->data[--len] = '\0';
    }

    gzprintf(fp_out, "%s\tsim_dup_exemplar\n", lb->data);

    // Find seq_id and prim_align_dup_exemplar column indices
    char* fields[1024];  // Support annotation pipelines with many columns
    size_t field_lens[1024];  // Field lengths
    int num_fields = parse_tsv_line(lb->data, fields, field_lens, 1024);
    int seq_id_idx = -1, prim_align_idx = -1;

    for (int i = 0; i < num_fields; i++) {
        if (strcmp(fields[i], "seq_id") == 0) seq_id_idx = i;
        else if (strcmp(fields[i], "prim_align_dup_exemplar") == 0) prim_align_idx = i;
    }

    if (seq_id_idx < 0 || prim_align_idx < 0) {
        fprintf(stderr, "Error: Missing required columns (seq_id or prim_align_dup_exemplar)\n");
        gzclose(fp_in);
        gzclose(fp_out);
        exit(1);
    }

    // The seq_id column must come before the prim_align_dup_exemplar column
    // for the fast-path optimization below which skips from seq_id to
    // prim_align_dup_exemplar without parsing all fields.  We could make this
    // more general, but seq_id is always the first column so instead raise an
    // error so we know if this stops being the case.
    if (seq_id_idx >= prim_align_idx) {
        fprintf(stderr, "Error: seq_id column (index %d) must come before "
                "prim_align_dup_exemplar column (index %d)\n",
                seq_id_idx, prim_align_idx);
        gzclose(fp_in);
        gzclose(fp_out);
        exit(1);
    }

    // Process data rows (optimized fast path for alignment duplicates)
    while (gzgets_growing(fp_in, lb)) {
        // Remove trailing newline
        len = lb->size;
        while (len > 0 && (lb->data[len-1] == '\n' || lb->data[len-1] == '\r')) {
            lb->data[--len] = '\0';
            lb->size = len;
        }

        // Fast path: check if this is an alignment duplicate
        // We do this by finding the seq_id and prim_align_dup_exemplar without full parsing
        char* line = lb->data;
        char* seq_id_start = line;
        char* prim_align_start = NULL;

        // Skip to seq_id column
        for (int i = 0; i < seq_id_idx; i++) {
            while (*seq_id_start && *seq_id_start != '\t') seq_id_start++;
            if (*seq_id_start) seq_id_start++;
        }

        // Find end of seq_id
        char* seq_id_end = seq_id_start;
        while (*seq_id_end && *seq_id_end != '\t') seq_id_end++;

        // Skip to prim_align column
        prim_align_start = seq_id_end;
        for (int i = seq_id_idx + 1; i < prim_align_idx; i++) {
            if (*prim_align_start) prim_align_start++;
            while (*prim_align_start && *prim_align_start != '\t') prim_align_start++;
        }
        if (*prim_align_start) prim_align_start++;

        // Compare seq_id and prim_align_dup_exemplar
        char* prim_align_end = prim_align_start;
        while (*prim_align_end && *prim_align_end != '\t') prim_align_end++;

        int seq_id_len = seq_id_end - seq_id_start;
        int prim_align_len = prim_align_end - prim_align_start;

        if (seq_id_len != prim_align_len ||
            memcmp(seq_id_start, prim_align_start, seq_id_len) != 0) {
            // Alignment duplicate - fast path
            gzprintf(fp_out, "%s\t%s\n", lb->data, "NA");
            (*n_prim_align_dups)++;
        } else {
            // Alignment-unique - query library for similarity exemplar
            char seq_id_buf[MAX_ID_LEN];
            if (seq_id_len >= MAX_ID_LEN) seq_id_len = MAX_ID_LEN - 1;
            memcpy(seq_id_buf, seq_id_start, seq_id_len);
            seq_id_buf[seq_id_len] = '\0';

            const char* sim_exemplar = nao_dedup_get_final_exemplar(deduper, seq_id_buf);
            gzprintf(fp_out, "%s\t%s\n", lb->data, sim_exemplar);

            if (strcmp(sim_exemplar, seq_id_buf) != 0) {
                (*n_sim_dups)++;
            }
        }
    }

    gzclose(fp_in);
    gzclose(fp_out);
    linebuf_destroy(lb);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <input.tsv.gz> <output.tsv.gz>\n", argv[0]);
        return 1;
    }

    const char* input_path = argv[1];
    const char* output_path = argv[2];

    clock_t start = clock();

    // Create deduplication context
    // Match Python default parameters from DedupParams
    NaoDedupParams params = {
        .kmer_len = 15,
        .window_len = 25,
        .num_windows = 4,
        .max_offset = 1,           // Python default
        .max_error_frac = 0.01,    // Python default
        .expected_reads = 20000000  // 20M reads
    };

    NaoDedupContext* deduper = nao_dedup_create(params);
    if (!deduper) {
        fprintf(stderr, "Error: Failed to create deduplication context: %s\n",
                nao_dedup_get_error_message());
        return 1;
    }

    // Pass 1: Process alignment-unique reads
    int n_reads = 0;
    process_alignment_unique_reads(input_path, deduper, &n_reads);

    // Finalize Pass 1 (frees scratch memory)
    nao_dedup_finalize(deduper);

    // Pass 2: Write output
    int n_prim_align_dups = 0;
    int n_sim_dups = 0;
    write_output_with_sim_column(input_path, output_path, deduper,
                                 &n_prim_align_dups, &n_sim_dups);

    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    fprintf(stderr, "Done!\n");
    fprintf(stderr, "Marked similarity duplicates processing %d reads in %.0fs, of which "
            "%d were already known to be duplicate and %d were additionally recognized as duplicate.\n",
            n_reads, elapsed, n_prim_align_dups, n_sim_dups);

    // Cleanup
    nao_dedup_destroy(deduper);

    return 0;
}
