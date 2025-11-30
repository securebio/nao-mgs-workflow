# nao_dedup

Sequencing read deduplication with error-tolerant matching

## Overview

`nao_dedup` identifies and removes duplicate read pairs from sequencing data
while being tolerant of small alignment shifts and sequencing errors. It uses
minimizer-based bucketing for efficiency to avoid comparing every read pair
against every other pair.

This library is available in two implementations:
- **Python**: Graph-based and streaming implementations for flexibility
- **C**: Streaming-only, very high-performance (~50x faster)

In maintaining this library we keep the C and Python streaming versions
completely in sync in terms of functionality.  This allows the Python version
to serve as a reference (for both humans and LLMs) for what we intend to be
doing, in part because our team is overall much stronger working in Python.


## Features

- **Error-tolerant matching**: Identifies duplicates even when reads have small
  differences due to sequencing errors
- **Alignment shift tolerance**: Handles reads that are slightly offset from
  each other
- **Efficient bucketing**: Uses minimizers to avoid comparing every read pair
  against every other pair
- **Quality-based selection**: Selects the highest quality read as the exemplar
  for each duplicate cluster
- **Flexible orientation handling**: Can operate in strict mode (same
  orientation required) or tolerant mode (handles mate-pair swaps)
- **High performance**: C implementation is 47x faster than Python

---

## Python Implementation

### Installation

#### Requirements

- Python 3.8 or higher

#### Install dependencies

```bash
pip install -r requirements.txt
```

### Usage

#### Basic Example

```python
from dedup import ReadPair, deduplicate_read_pairs, DedupParams

# Create read pairs
read_pairs = [
    ReadPair("read1", "ACGTACGT", "TGCATGCA", "IIIIIIII", "IIIIIIII"),
    ReadPair("read2", "ACGTACGT", "TGCATGCA", "IIIIIIII", "IIIIIIII"),  # Duplicate
    ReadPair("read3", "GGGGAAAA", "CCCCTTTT", "IIIIIIII", "IIIIIIII"),  # Different
]

# Run deduplication (graph-based)
result = deduplicate_read_pairs(read_pairs)

# Or use streaming implementation for memory efficiency
result = deduplicate_read_pairs_streaming(read_pairs)

# Check results
for rp in result:
    print(f"{rp.read_id} -> exemplar: {rp.exemplar_id}")
```

#### Advanced Configuration

```python
from dedup import DedupParams, MinimizerParams, ORIENT_STRICT, ORIENT_TOLERANT

# Configure deduplication parameters
dedup_params = DedupParams(
    max_offset=1,           # Maximum alignment shift in bases
    max_error_frac=0.01,    # Maximum 1% mismatch rate
    orientation=ORIENT_TOLERANT  # Allow swapped mate pairs
)

# Configure minimizer parameters (rarely needs changing)
minimizer_params = MinimizerParams(
    num_windows=3,    # Number of windows per read
    window_len=25,    # Base pairs per window
    kmer_len=7        # K-mer size for minimizers
)

# Run with custom parameters
result = deduplicate_read_pairs(
    read_pairs,
    dedup_params=dedup_params,
    minimizer_params=minimizer_params,
    verbose=True
)
```

### Python Parameters

#### DedupParams

- `max_offset` (default: 1): Maximum number of bases a read can be shifted and
  still be considered a duplicate
- `max_error_frac` (default: 0.01): Maximum fraction of mismatches allowed
  (errors/overlap length)
- `orientation` (default: "tolerant"): Either `ORIENT_STRICT` (F-R must match
  F-R) or `ORIENT_TOLERANT` (also allows F-R to match R-F)

#### MinimizerParams

- `num_windows` (default: 3): Number of windows to extract from each read
- `window_len` (default: 25): Size of each window in base pairs
- `kmer_len` (default: 7): Size of k-mers for minimizer calculation

---

## C Implementation

### Features

- **High Performance**: 47x faster than Python implementation
- **Memory Efficient**: Arena allocators minimize fragmentation, separate scratch/result memory
- **Scalable**: Handles 20M+ reads with configurable hash table sizing
- **Quality-Aware**: Uses sequence quality scores for tie-breaking between duplicates
- **Generic**: TSV-agnostic - works with any data source

### API Overview

#### Lifecycle

```c
// Create context with parameters
NaoDedupParams params = {
    .kmer_len = 15,
    .window_len = 25,
    .num_windows = 4,
    .max_offset = 1,
    .max_error_frac = 0.01,
    .expected_reads = 20000000,
    .scratch_arena_size = 0,  // 0 = use default (2GB)
    .result_arena_size = 0    // 0 = use default (512MB)
};
NaoDedupContext* ctx = nao_dedup_create(params);

// ... process reads ...

// Clean up
nao_dedup_destroy(ctx);
```

#### Pass 1: Build Index

```c
// Feed reads to the library (strings don't need to be null-terminated)
const char* exemplar = nao_dedup_process_read(
    ctx,
    read_id, id_len,
    fwd_seq, fwd_len,
    rev_seq, rev_len,
    fwd_qual, fwd_qual_len,  // Can be NULL/0
    rev_qual, rev_qual_len   // Can be NULL/0
);
// exemplar is the similarity exemplar for this read

// Finalize when done (frees scratch memory)
nao_dedup_finalize(ctx);
```

#### Pass 2: Query Results

```c
// After finalization, query final exemplars
const char* final_exemplar = nao_dedup_get_final_exemplar(ctx, read_id);
```

#### Statistics

```c
NaoDedupStats stats;
nao_dedup_get_stats(ctx, &stats);
printf("Processed %d reads in %d clusters\n",
       stats.total_reads_processed,
       stats.unique_clusters);
```

### Integration Example

See `../src/similarity_duplicate_marking.c` for a complete example of how to integrate this library with TSV file I/O.

The driver handles:
- File I/O (gzipped TSV files)
- TSV parsing
- Business logic (only processing alignment-unique reads)
- Output formatting

The library handles:
- Minimizer extraction
- Similarity matching
- Cluster management
- Memory management

### Performance

**Small file (70K reads)**:
- Python: 28.4s
- C library: 2.6s
- **Speedup: 11x**

**Large file (2M reads)**:
- Python: ~60 minutes (estimated)
- C library: 76 seconds
- **Speedup: 47x**

### Memory Usage

- **Scratch arena**: 2GB (freed after Pass 1)
- **Result arena**: 512MB (kept for Pass 2 lookups)
- **Hash tables**: ~16M buckets for 20M reads

### Thread Safety

This library is **not thread-safe**. Each thread should use its own `NaoDedupContext`.

### Error Handling

The C library uses different error handling strategies depending on the
operation phase:

#### During Initialization (`nao_dedup_create`)

Functions return `NULL` on error. Use `nao_dedup_get_error_message()` to
retrieve error details:

```c
NaoDedupContext* ctx = nao_dedup_create(params);
if (!ctx) {
    fprintf(stderr, "Error: %s\n", nao_dedup_get_error_message());
    exit(1);
}
```

Common initialization errors:
- Invalid parameters (e.g., negative offsets)
- Memory allocation failure for context or arenas
- Hash table initialization failure

#### During Processing (`nao_dedup_process_read`, etc.)

**Memory allocation failures during processing are fatal and cause immediate
program termination.**

If arena capacity is exceeded during processing, the library prints an error
message to stderr and exits with code 1:

```
FATAL: Out of memory in arena_alloc: arena capacity exceeded
The dataset has exceeded the pre-allocated arena capacity.

You can increase arena sizes via NaoDedupParams:

     params.scratch_arena_size = 4ULL * 1024 * 1024 * 1024;   // 4GB
     params.result_arena_size = 1024ULL * 1024 * 1024;        // 1GB
     (defaults: scratch=2GB, result=512MB)
```

**Important: Two types of memory errors**

1. **System out of memory at creation** - If `malloc()` fails when allocating
   the arenas, `nao_dedup_create()` returns NULL gracefully. This means
   your system truly doesn't have enough RAM.

2. **Arena capacity exceeded during processing** - If your dataset uses more
   than the configured arena sizes processing will exit with the error
   above. This doesn't mean your system is out of RAM - it means the **arena
   sizes are too small** for your dataset and you need to configure
   NaoDedupParams with larger ones.

**Why this design?**

1. **Arena sizes are configurable** - Set via `NaoDedupParams` with sensible
   defaults (2GB scratch + 512MB result). For most datasets these defaults are
   sufficient.

2. **Graceful error handling is not possible** - If we can't allocate memory to
   store read data, we cannot produce correct deduplication results. Continuing
   would silently produce incorrect output (missing duplicates).

3. **Fast failure is better than wrong results** - For batch processing
   pipelines, it's better to fail immediately with a clear error than to return
   subtly incorrect data.

### Compilation

```bash
gcc -O3 -march=native -I. your_driver.c nao_dedup.c -lz -o your_program
```

---

## How It Works

### 1. Minimizer Extraction

Each read is divided into windows, and the lexicographically smallest k-mer
(minimizer) is extracted from each window. This creates a signature for each
read pair.

### 2. Bucketing

Read pairs with matching minimizers are assigned to the same buckets. This
dramatically reduces the number of pairwise comparisons needed.

### 3. Pairwise Comparison

Within each bucket, read pairs are compared to determine if they're
duplicates. Comparison allows for:
- Small alignment offsets (configurable via `max_offset`)
- Sequencing errors (configurable via `max_error_frac`)
- Optional mate-pair orientation swaps (configurable via `orientation`)

### 4. Clustering (Python graph-based) or Streaming (C and Python streaming)

**Python graph-based**:
- An equivalence graph is built where nodes are read pairs and edges connect duplicates
- Connected components in the graph represent duplicate clusters
- For each cluster, an exemplar is selected based on graph centrality, quality score, read length, and read ID

**C and Python streaming**:
- No graph construction - purely streaming approach
- First matching read in a bucket becomes the cluster exemplar
- Subsequent matches are assigned to that exemplar
- Two-pass algorithm: Pass 1 builds index, Pass 2 queries final exemplars

### 5. Exemplar Selection

**Python graph-based** selects based on:
1. Graph centrality (lower eccentricity is better)
2. Mean quality score (higher is better)
3. Total read length (longer is better)
4. Read ID (lexicographic tie-breaker)

**C/Python streaming** selects based on:
1. First match in bucket (becomes exemplar)
2. Quality score for tie-breaking when applicable

## Testing

### Python tests

Run all Python tests:

```bash
pytest tests/test_dedup.py
```

### C tests

Build the test library and run tests:

```bash
make test
```

This builds `tests/libnaodedup_test.so` and runs parametrized tests that verify
parity between Python and C implementations.
