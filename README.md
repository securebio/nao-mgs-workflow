# nao_dedup

Sequencing read deduplication with error-tolerant matching

## Overview

`nao_dedup` identifies and removes duplicate read pairs from sequencing data
while being tolerant of small alignment shifts and sequencing errors. It uses
minimizer-based bucketing for efficiency to avoid comparing every read pair
against every other pair.

This library is available in two implementations:
- **Python**: Graph-based and streaming implementations for flexibility
- **Rust**: Streaming-only, very high-performance

In maintaining this library we keep the Rust and Python streaming versions
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
- **High performance**: Rust implementation is ~40x faster than Python, and
  uses much less memory.

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

## Rust Implementation

### Features

- **High Performance**: Significantly faster than Python implementation
- **Memory Efficient**: Only stores exemplar reads, not entire dataset
- **Scalable**: Handles millions of reads efficiently
- **Quality-Aware**: Uses sequence quality scores for exemplar selection

### API Overview

The Rust library is designed as a stateful context that processes reads in two
passes:

#### Pass 1: Build Index

```rust
use nao_dedup::{DedupContext, DedupParams, MinimizerParams, ReadPair};

// Create context with default or custom parameters
let dedup_params = DedupParams::default();
let minimizer_params = MinimizerParams::default();
let mut ctx = DedupContext::new(dedup_params, minimizer_params);

// Process each read
let read_pair = ReadPair {
    read_id: "read1".to_string(),
    fwd_seq: "ACGTACGT".to_string(),
    rev_seq: "TGCATGCA".to_string(),
    fwd_qual: "IIIIIIII".to_string(),
    rev_qual: "IIIIIIII".to_string(),
};
ctx.process_read(read_pair);

// Finalize to build final exemplar mappings
ctx.finalize();
```

#### Pass 2: Query Results

```rust
// After finalization, query final exemplars
let exemplar = ctx.get_final_exemplar("read1");
println!("read1 -> {}", exemplar);

// Get statistics
let (total_processed, unique_clusters) = ctx.stats();
```

### Integration Example

See `post-processing/rust_dedup/src/similarity_duplicate_marking.rs` in
https://github.com/securebio/nao-mgs-workflow for a complete example of how to
integrate this library with TSV file I/O.

### Performance

**Large file (685K reads, 456K alignment-unique)**:
- Rust: 127 seconds
- Memory: 1.37 GB peak

We balance memory and speed, storing only exemplar reads rather than the entire
dataset.

### Building

The library is a standard Rust crate. Build with:

```bash
cargo build --release
```

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

### 4. Clustering (Python graph-based) or Streaming (Rust and Python streaming)

**Python graph-based**:
- An equivalence graph is built where nodes are read pairs and edges connect
  duplicates
- Connected components in the graph represent duplicate clusters
- For each cluster, an exemplar is selected based on graph centrality, quality
  score, read length, and read ID

**Rust and Python streaming**:
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

**Rust/Python streaming** selects based on:
1. First match in bucket (becomes exemplar)
2. Quality score for tie-breaking when applicable

## Development Setup

### Rust Toolchain

On macOS with Homebrew:
```bash
brew install rust
```

On Linux:
```bash
# Ubuntu/Debian
sudo apt install rustc cargo

# Fedora
sudo dnf install rust cargo
```

Verify installation:
```bash
cargo --version
```

### Python Dependencies

For runtime dependencies:
```bash
pip install -r requirements.txt
```

For development and testing:
```bash
pip install -r requirements-dev.txt
```

## Testing

### Python and Rust tests

Run all tests (Python and Rust implementations):

```bash
pytest
```

To ensure the Python and Rust implementations of streaming dedup stay in sync,
tests are parametrized by implementation.  The Rust library is built
automatically when tests are run if not already present.
