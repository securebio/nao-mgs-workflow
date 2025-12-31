# nao-dedup

Sequencing read deduplication with error-tolerant matching

## Overview

`nao-dedup` identifies and removes duplicate read pairs from sequencing data
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
- **Flexible orientation handling** (Python only): Can operate in strict mode
  (same orientation required) or tolerant mode (handles mate-pair swaps). Rust
  always uses tolerant mode.
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

#### Choosing an Algorithm

The Python implementation of `nao-dedup` provides two deduplication algorithms:

1. **`deduplicate_read_pairs()`** - Graph-based algorithm
   - **Best for**: Small to medium datasets (< 100k reads)
   - **Advantages**: Uses graph centrality to select optimal exemplars
   - **Memory usage**: Stores all reads in memory (~3GB for 250k reads)

2. **`deduplicate_read_pairs_streaming()`** - Streaming two-pass algorithm
   - **Best for**: Large datasets (> 100k reads)
   - **Advantages**: Only stores unique sequences in memory (~2-3x less memory
     in the typical case, far more on pathological datasets)
   - **Memory usage**: Much lower (~1GB for 250k reads with 75k unique)
   - **Quality**: Still selects high-quality representatives based on length
     and quality

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
exemplar_mapping = deduplicate_read_pairs(read_pairs)

# Or use streaming implementation for memory efficiency
exemplar_mapping = deduplicate_read_pairs_streaming(read_pairs)

# Check results
for read_id, exemplar_id in exemplar_mapping.items():
    print(f"{read_id} -> exemplar: {exemplar_id}")
```

#### Advanced Configuration

```python
from dedup import deduplicate_read_pairs_streaming, DedupParams, \
                  MinimizerParams, ORIENT_TOLERANT

# Configure deduplication parameters
dedup_params = DedupParams(
    max_offset=1,                # Maximum alignment shift in bases
    max_error_frac=0.01,         # Maximum 1% mismatch rate
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
- `orientation` (default: `ORIENT_TOLERANT`): Either `ORIENT_STRICT` (F-R must
  match F-R) or `ORIENT_TOLERANT` (also allows F-R to match R-F)

#### MinimizerParams

- `num_windows` (default: 3): Number of windows to extract from each read
- `window_len` (default: 25): Size of each window in base pairs
- `kmer_len` (default: 7): Size of k-mers for minimizer calculation

**Note**: Rust uses different defaults (`kmer_len=15`, `num_windows=4`) because
it's expected to handle much larger inputs where more selective minimizers
reduce memory usage and comparisons.

---

## Rust Implementation

### Features

- **High Performance**: Significantly faster than Python implementation
- **Memory Efficient**: Only stores exemplar reads, not entire dataset
- **Scalable**: Handles millions of reads efficiently
- **Quality-Aware**: Uses sequence quality scores for exemplar selection
- **Tolerant Orientation Only**: Always allows swapped mate pairs (no strict
  orientation mode)

### API Overview

The Rust library is designed as a stateful context that processes reads in two
passes:

#### Pass 1: Process Reads

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
```

#### Pass 2: Finalize

```rust
// Finalize to build final exemplar mappings
ctx.finalize();
```

#### Query Results

```rust
// After finalization, query cluster IDs
let cluster_id = ctx.get_cluster_id("read1");
println!("read1 -> {}", cluster_id);

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

Note that this is single-threaded performance.  We ought to be able to do even
better with more cores, though in practice we just mark duplicates on multiple
files in parallel.

### Building

The library is a standard Rust crate. Build with:

```bash
cargo build --release
```

### Command-Line Binary

A command-line tool for deduplicating interleaved FASTQ.gz files is included.

#### Building

```bash
cargo build --release --bin dedup_interleaved_fastq
```

The binary will be created at `target/release/dedup_interleaved_fastq`.

#### Usage

Basic usage with default parameters:

```bash
./target/release/dedup_interleaved_fastq input.fastq.gz output.fastq.gz
```

With custom parameters:

```bash
./target/release/dedup_interleaved_fastq \
  --max-offset 2 \
  --max-error-frac 0.02 \
  --kmer-len 15 \
  --window-len 25 \
  --num-windows 4 \
  input.fastq.gz output.fastq.gz
```

View all options:

```bash
./target/release/dedup_interleaved_fastq --help
```

#### Input Format

The binary expects interleaved paired-end FASTQ files where R1 and R2 reads
alternate (R1, R2, R1, R2, ...). The input file must be gzip-compressed.

The input file must not be streamed, because we process it in two passes.  So
you can't do:

```bash
./target/release/dedup_interleaved_fastq \
    <(aws s3 cp s3://.../input.fastq.gz -) output.fastq.gz
```

#### Output

The output file contains only the exemplar read pairs (one representative per
duplicate cluster), maintaining the interleaved format. The binary performs
two passes:

1. **Pass 1**: Reads all pairs and builds the deduplication index
2. **Pass 2**: Writes only exemplar pairs to the output file

Progress is reported during both passes, along with deduplication statistics.

#### Parameters

- `--max-offset`: Maximum alignment offset in bases (default: 1)
- `--max-error-frac`: Maximum error fraction allowed (default: 0.01)
- `--kmer-len`: K-mer length for minimizers (default: 15)
- `--window-len`: Window length for minimizers (default: 25)
- `--num-windows`: Number of windows per read (default: 4)

## How It Works

### 1. Minimizer Extraction

Each read is divided into windows, and the lexicographically smallest k-mer
(minimizer) is extracted from each window. This creates a signature for each
read pair.

#### K-mer Encoding

K-mers are encoded using a 2-bit DNA encoding (A=0, C=1, G=2, T=3) that
represents each base with exactly 2 bits. This provides several advantages when
used as a hash key:

- **Fast**: Just bit shifts and ORs, much faster than CRC32 or polynomial hashing
- **No collisions**: Bijective mapping for k-mers up to length 32 (fits in 64 bits)
- **Consistent**: Python and Rust implementations produce identical hash values
- **DNA-aware**: Leverages the 4-base structure of DNA sequences

K-mers containing non-ACGT bases (primarily N) return a sentinel value,
ensuring they won't be selected as minimizers.

#### Window Placement

Windows are placed adjacently starting from the beginning of each read
(positions 0, window_len, 2×window_len, etc.). This strategy:
- Focuses on the most stable region of reads (the beginning)
- Avoids the tail region, which is most likely to be trimmed or contain
  sequencing errors

For example, with 3 windows of 25bp on a 150bp read:
- Window 0: positions [0, 25)
- Window 1: positions [25, 50)
- Window 2: positions [50, 75)
- Positions [75, 150) are not examined for minimizers

### 2. Bucketing

Read pairs with matching minimizers are assigned to the same buckets. This
dramatically reduces the number of pairwise comparisons needed.

**Python graph-based**:
- All reads are assigned to buckets up front using tuple keys `(fwd_hash, rev_hash)`
- Generates n² bucket keys per read (all combinations of forward and reverse minimizers)
- Each read appears in multiple buckets
- More precise bucketing with fewer collisions
- Stores all reads in memory

**Rust and Python streaming**:
- Reads are processed one at a time
- Uses individual minimizer hashes as keys (not tuples)
- Generates 2n keys per read (just the minimizers from forward and reverse reads)
- Simpler bucketing trades precision for memory efficiency
- More bucket collisions are acceptable since full sequence comparison happens anyway
- Only stores unique exemplars, not all reads

### 3. Pairwise Comparison

Within each bucket, read pairs are compared to determine if they're
duplicates. Comparison allows for:
- Small alignment offsets (configurable via `max_offset`)
- Sequencing errors (configurable via `max_error_frac`)
- Mate-pair orientation swaps (always enabled in Rust; configurable via
  `orientation` in Python)

### 4. Clustering

**Python graph-based**:
- An equivalence graph is built where nodes are read pairs and edges connect
  duplicates
- Connected components in the graph represent duplicate clusters
- For each cluster, an exemplar is selected based on graph centrality, quality
  score, read length, and read ID

**Rust and Python streaming**:
- No graph construction - purely streaming approach
- First read in a cluster identifies that cluster (becomes the cluster ID)
- As reads are processed, the best read (by quality and length) becomes the exemplar
- Subsequent reads matching the cluster are compared against the current exemplar
- If a better read is found, it replaces the previous exemplar
- Two-pass algorithm: Pass 1 processes reads and builds index, Pass 2 finalizes exemplar mappings

### 5. Exemplar Selection

**Python graph-based** selects based on:
1. Graph centrality (lower eccentricity is better)
2. Mean quality score (higher is better)
3. Total read length (longer is better)
4. Read ID (lexicographic tie-breaker)

**Rust/Python streaming** selects based on:
1. Mean quality score (higher is better)
2. Total read length (longer is better)
3. First read in cluster serves as tie-breaker

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

# Redhat derived distributions (Fedora, Amazon Linux 2023, etc)
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

To ensure the Python and Rust implementations stay in sync, tests include:

- **Implementation parity tests**: Parametrized tests that run on both
  streaming implementations to ensure they produce identical results for all
  scenarios including edge cases with N's, offsets, and errors
- **End-to-end tests**: Verify deduplication works correctly on realistic data
  with various read configurations and quality scores

The Rust library is built automatically when tests are run if not already
present.
