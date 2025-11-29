# nao-dedup

Sequencing read deduplication with error-tolerant matching

## Overview

`nao-dedup` identifies and removes duplicate read pairs from sequencing data
while being tolerant of small alignment shifts and sequencing errors. It uses
minimizer-based bucketing for efficiency and graph-based clustering to handle
reads that differ slightly due to sequencing errors or alignment variations.

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

## Installation

### Requirements

- Python 3.8 or higher

### Install dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Choosing an Algorithm

`nao-dedup` provides two deduplication algorithms:

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

### Basic Example

```python
from dedup import ReadPair, deduplicate_read_pairs_streaming, DedupParams

# Create read pairs
read_pairs = [
    ReadPair("read1", "ACGTACGT", "TGCATGCA", "IIIIIIII", "IIIIIIII"),
    ReadPair("read2", "ACGTACGT", "TGCATGCA", "IIIIIIII", "IIIIIIII"),  # Duplicate
    ReadPair("read3", "GGGGAAAA", "CCCCTTTT", "IIIIIIII", "IIIIIIII"),  # Different
]

# Run deduplication
result = deduplicate_read_pairs(read_pairs)
# Or deduplicate_read_pairs_streaming(read_pairs) for large datasets.

# Check results
for rp in result:
    print(f"{rp.read_id} -> exemplar: {rp.exemplar_id}")
```

### Advanced Configuration

```python
from dedup import DedupParams, MinimizerParams, ORIENT_STRICT, ORIENT_TOLERANT

# Configure deduplication parameters
dedup_params = DedupParams(
    max_offset=1,           # Maximum alignment shift in bases
    max_error_frac=0.01,    # Maximum 1% mismatch rate
    orientation=ORIENT_TOLERANT  # Allow swapped mate pairs
)

# Configure minimizer parameters
# Note: For large datasets with many reads, use a larger kmer_len to avoid
# bucket explosions. With default kmer_len=7, there are only 4^7 (~16k)
# possible sequences.
minimizer_params = MinimizerParams(
    num_windows=4,    # Number of windows per read
    window_len=25,    # Base pairs per window
    kmer_len=15       # K-mer size (use 15 for large datasets)
)

# Run with custom parameters
result = deduplicate_read_pairs_streaming(
    read_pairs,
    dedup_params=dedup_params,
    minimizer_params=minimizer_params,
    verbose=True
)
```

## How It Works

### Graph-based Algorithm (`deduplicate_read_pairs`)

1. **Minimizer Extraction**: Each read is divided into windows, and the
   lexicographically smallest k-mer (minimizer) is extracted from each window.

2. **Bucketing**: Read pairs with matching minimizers are assigned to the same
   buckets, reducing the number of pairwise comparisons needed.

3. **Pairwise Comparison**: Within each bucket, read pairs are compared to
   determine if they're duplicates, allowing for:
   - Small alignment offsets (configurable via `max_offset`)
   - Sequencing errors (configurable via `max_error_frac`)
   - Optional mate-pair orientation swaps (configurable via `orientation`)

4. **Graph Construction**: An equivalence graph is built where nodes are read
   pairs and edges connect duplicates.

5. **Clustering**: Connected components in the graph represent duplicate clusters.

6. **Exemplar Selection**: For each cluster, an exemplar is selected based on:
   - Graph centrality (lower eccentricity is better)
   - Mean quality score (higher is better)
   - Total read length (longer is better)
   - Read ID (lexicographic tie-breaker)

### Streaming Algorithm (`deduplicate_read_pairs_streaming`)

The streaming algorithm uses a two-pass approach that provides near-optimal
exemplar selection while using significantly less memory:

**Pass 1: Cluster and Track Best Representatives**

1. **Stream through reads**: Process reads one at a time, not loading all into
   memory
2. **Find matches**: For each read, use minimizer-based bucketing to find matching
   unique sequences (exemplars)
3. **Update or create cluster**:
   - If match found: Check if this read is better than the current cluster
     representative
   - If no match: Create new cluster with this read as the exemplar
4. **Track best**: Maintain only the best read seen so far for each cluster

**Pass 2: Assign Final Exemplars**

1. **Stream through reads again**: Process all reads a second time
2. **Look up cluster**: Find which cluster each read belongs to (same logic as
   Pass 1)
3. **Assign best exemplar**: Use the best representative identified in Pass 1

**Key Advantages**:
- **Memory efficient**: Only stores unique sequences
- **Fast**: Two linear passes through data vs. O(N²) graph construction

## Parameters

### DedupParams

- `max_offset` (default: 1): Maximum number of bases a read can be shifted and
  still be considered a duplicate
- `max_error_frac` (default: 0.01): Maximum fraction of mismatches allowed
  (errors/overlap length)
- `orientation` (default: "tolerant"): Either `ORIENT_STRICT` (F-R must match
  F-R) or `ORIENT_TOLERANT` (also allows F-R to match R-F)

### MinimizerParams

- `num_windows` (default: 3): Number of windows to extract from each read
- `window_len` (default: 25): Size of each window in base pairs
- `kmer_len` (default: 7): Size of k-mers for minimizer calculation

## Testing

Run all tests:

```bash
pytest
```
