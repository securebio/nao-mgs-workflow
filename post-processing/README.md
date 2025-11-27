# Post-Processing

## Philosophy

We have not yet figured out how we want to handle situations where there's
additional processing we'd like to do on top of workflow outputs.  For now,
post-processing/ contains scripts which can be run manually on these outputs to
perform additional analyses.  If these end up being ones we like, we'll figure
out how to fully include them.

## Scripts

### bin/combination_duplicate_marking.py

Runs similarity-based duplicate marking as a supplement to existing
alignment-based duplicate marking.

#### Overview

We have two kinds of duplicate detection:

1. **Alignment-based deduplication** (from the `prim_align_dup_exemplar`
   column): DOWNSTREAM already grouped some duplicate reads through alignment
   analysis.

2. **Similarity-based deduplication** (using
   [nao-dedup](https://github.com/securebio/nao-dedup)): Group reads based on
   sequence similarity, tolerating small alignment shifts and sequencing
   errors.

If any member of one alignment group is similar to any member of another
alignment group, all reads in both groups are marked with the same
`combined_dup_exemplar`.

Testing on 69,652 read pairs, 33,920 of which were unique per alignment-based
duplication identification, 22,683 were unique after additionally considering
similarity.  This ran in 1m18s, which is fast enough to be usable, but slow
enough that we'd really like to integrate this into DOWNSTREAM.

#### Usage

```bash
./bin/combination_duplicate_marking.py <input.tsv.gz> <output.tsv.gz>
```

#### Input Format

The input must be a gzipped TSV file with at least these columns:

- `seq_id`: Unique identifier for each read
- `query_seq`: Forward read sequence
- `query_seq_rev`: Reverse read sequence
- `query_qual`: Quality scores for forward read
- `query_qual_rev`: Quality scores for reverse read
- `prim_align_dup_exemplar`: Alignment-based duplicate exemplar (set to the read's own `seq_id` if not a duplicate)

Additional columns are preserved in the output.  This means the tool is
agnostic to whether it is run on a `_validation_hits.tsv.gz`,
`_duplicate_reads.tsv.gz`, or anything else with the same structure.

#### Output Format

The output is a gzipped TSV file containing all input columns in the same order
as the input, plus a final new column `combined_dup_exemplar`.  This is the
final exemplar ID after combining both deduplication methods .

For reads that are not duplicates of anything, `combined_dup_exemplar` is set
to the read's own `seq_id`.

#### Algorithm

1. **First pass**: Reads deduplication columns from input:

   - Groups reads by their `prim_align_dup_exemplar`
   - Runs similarity-based deduplication on all reads
   - Merges alignment groups when their members are found to be similar
   - Selects final exemplars using centrality-based logic from nao_dedup

2. **Second pass**: Writes output with the new `combined_dup_exemplar` column.

This two-pass approach avoids loading the entire TSV into memory.

#### Memory Considerations

During the first pass, all read pairs (sequences and quality scores) are loaded
into memory for similarity-based deduplication. Memory usage scales linearly
with the number of reads in the input file. For very large files (millions of
reads), this can be quite a bit of memory. The second pass streams through the
file without loading it all into memory.

#### Example

If you have:
- Reads A and B marked as alignment duplicates (both have
  `prim_align_dup_exemplar = "A"`)
- Reads C and D marked as alignment duplicates (both have
  `prim_align_dup_exemplar = "C"`)
- Similarity deduplication finds that A and C have similar sequences

Then all four reads (A, B, C, D) will receive the same `combined_dup_exemplar`
value, chosen using quality and centrality metrics.

## Testing

Run tests with:

```bash
pytest
```

## Dependencies

### deps/nao_dedup

This is a git subtree from https://github.com/securebio/nao-dedup, tracking the
`jefftk/import-code` branch for now.  Once that branch is merged we'll track
`main` instead.

#### Pulling in updates

To pull in changes from the upstream repository:

```bash
git subtree pull \
    --prefix=post-processing/deps/nao_dedup \
    https://github.com/securebio/nao-dedup \
    jefftk/import-code \
    --squash
```

#### Pushing changes upstream

If you make changes to the subtree that should be pushed back to the upstream repository:

```bash
git subtree push \
    --prefix=post-processing/deps/nao_dedup \
    https://github.com/securebio/nao-dedup \
    jefftk/import-code
```
