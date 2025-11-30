# Post-Processing

## Philosophy

We have not yet figured out how we want to handle situations where there's
additional processing we'd like to do on top of workflow outputs.  For now,
post-processing/ contains scripts which can be run manually on these outputs to
perform additional analyses.  If these end up being ones we like, we'll figure
out how to fully include them.

## Scripts

### bin/similarity_duplicate_marking

A high-performance C tool that runs similarity-based duplicate marking as a
supplement to existing alignment-based duplicate marking.

#### Overview

We have two kinds of duplicate detection:

1. **Alignment-based deduplication** (from the `prim_align_dup_exemplar`
   column): DOWNSTREAM already grouped some duplicate reads through alignment
   analysis.

2. **Similarity-based deduplication** (using
   [nao-dedup](https://github.com/securebio/nao-dedup)): Group reads based on
   sequence similarity, tolerating small alignment shifts and sequencing
   errors.

The similarity-based tool only processes reads where `prim_align_dup_exemplar
== seq_id` (alignment-unique reads), and produces a `sim_dup_exemplar` column.
If you only want the exemplars you can filter to reads where sim_dup_exemplar
== seq_id.  If you need the groups, first look up the record identified in
prim_align_dup_exemplar, and then group by that record's sim_dup_exemplar.

Testing on 69,652 read pairs, 33,920 of which were unique per alignment-based
duplication identification, 22,683 were unique after additionally considering
similarity. With the new approach, we only need to run similarity deduplication
on the 33,920 alignment-unique reads instead of all 69,652, making it much
faster and more memory-efficient.

#### Building

The similarity duplicate marking tool is implemented in C for
performance. Build it with:

```bash
cd src
make
```

This will compile the binary to `bin/similarity_duplicate_marking`. Build
artifacts (object files) are placed in `obj/`.

**Requirements:**
- C compiler (gcc or clang)
- zlib development headers (`zlib.h`)

To clean build artifacts:

```bash
cd src
make clean
```

#### Usage

```bash
./bin/similarity_duplicate_marking <input.tsv.gz> <output.tsv.gz>
```

#### Input Format

The input must be a gzipped TSV file with at least these columns:

- `seq_id`: Unique identifier for each read
- `query_seq`: Forward read sequence
- `query_seq_rev`: Reverse read sequence
- `query_qual`: Quality scores for forward read
- `query_qual_rev`: Quality scores for reverse read
- `prim_align_dup_exemplar`: Alignment-based duplicate exemplar (set to the
  read's own `seq_id` if not a duplicate).

Additional columns are preserved in the output.  This means the tool is
agnostic to whether it is run on a `_validation_hits.tsv.gz`,
`_duplicate_reads.tsv.gz`, or anything else with the same structure.

#### Output Format

The output is a gzipped TSV file containing all input columns in the same order
as the input, plus a final new column `sim_dup_exemplar`:

- For reads where `prim_align_dup_exemplar == seq_id`: Contains the similarity
  exemplar ID (the read's own `seq_id` if not a similarity duplicate)
- For all other reads: Contains `'NA'` (these reads were already identified as
  duplicates by alignment, so no similarity check is needed)

#### Algorithm

1. **First pass**: Reads only alignment-unique reads from input:
   - Filters to reads where `prim_align_dup_exemplar == seq_id`
   - Runs similarity-based deduplication on these reads using nao-dedup's
     streaming algorithm

2. **Second pass**: Writes output with the new `sim_dup_exemplar` column:
   - For alignment-unique reads: Uses the similarity exemplar
   - For alignment duplicates: Writes `'NA'`

This two-pass approach avoids loading the entire TSV into memory.

#### Memory Considerations

During the first pass, only alignment-unique read pairs are loaded into memory.
Memory usage scales with the number of alignment-unique reads, which is
typically much smaller than the total number of reads. The streaming algorithm
further reduces memory usage by only storing unique sequences. The second pass
streams through the file without loading it all into memory.

#### Example

If you have:
- Read A with `prim_align_dup_exemplar = "A"` (alignment-unique)
- Read B with `prim_align_dup_exemplar = "A"` (alignment duplicate of A)
- Read C with `prim_align_dup_exemplar = "C"` (alignment-unique)
- Similarity deduplication finds that A and C have similar sequences but C is
  higher quality.

Then:
- Read A gets `sim_dup_exemplar = "C"` (chosen as exemplar based on quality)
- Read B gets `sim_dup_exemplar = "NA"` (already marked as duplicate via
  alignment)
- Read C gets `sim_dup_exemplar = "C"` (similarity duplicate of A)

To learn that the "all things considered" exemplar for B is C, you'd see that B
has a `prim_align_dup_exemplar` of A, and that A has a `sim_dup_exemplar` of C.

## Testing

Run tests with:

```bash
pytest
```

**Note:** The binary must be built before running tests. Run `cd src && make`
if needed.

## Implementation

The tool is implemented in C for performance:

- **`src/similarity_duplicate_marking.c`**: Main driver that handles TSV I/O
  and calls the deduplication library
- **`src/Makefile`**: Build configuration
- **`deps/nao_dedup/`**: Git subtree containing the nao-dedup library (see below)

Build artifacts:
- **`bin/similarity_duplicate_marking`**: Compiled binary
- **`obj/`**: Object files

## Dependencies

### deps/nao_dedup

This is a git subtree from https://github.com/securebio/nao-dedup, tracking the
`jefftk/c-implementation` branch for now.  Once that branch is merged
we'll track `main` instead.

The library provides the core deduplication algorithm with offset-based sequence matching.

#### Pulling in updates

To pull in changes from the upstream repository:

```bash
git subtree pull \
    --prefix=post-processing/deps/nao_dedup \
    https://github.com/securebio/nao-dedup \
    jefftk/c-implementation \
    --squash
```

#### Pushing changes upstream

If you make changes to the subtree that should be pushed back to the upstream repository:

```bash
git subtree push \
    --prefix=post-processing/deps/nao_dedup \
    https://github.com/securebio/nao-dedup \
    jefftk/c-implementation
```
