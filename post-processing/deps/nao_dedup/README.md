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

### Basic Example

```python
from dedup import ReadPair, deduplicate_read_pairs, DedupParams

# Create read pairs
read_pairs = [
    ReadPair("read1", "ACGTACGT", "TGCATGCA", "IIIIIIII", "IIIIIIII"),
    ReadPair("read2", "ACGTACGT", "TGCATGCA", "IIIIIIII", "IIIIIIII"),  # Duplicate
    ReadPair("read3", "GGGGAAAA", "CCCCTTTT", "IIIIIIII", "IIIIIIII"),  # Different
]

# Run deduplication
result = deduplicate_read_pairs(read_pairs)

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

### 4. Graph Construction

An equivalence graph is built where nodes are read pairs and edges connect
duplicates.

### 5. Clustering

Connected components in the graph represent duplicate clusters.

### 6. Exemplar Selection

For each cluster, an exemplar is selected based on:

1. Graph centrality (lower eccentricity is better)
2. Mean quality score (higher is better)
3. Total read length (longer is better)
4. Read ID (lexicographic tie-breaker)

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
