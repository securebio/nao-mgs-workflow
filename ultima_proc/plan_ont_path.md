# Ultima Adaptation: ONT Path

## Strategy
Minimally modify the existing `EXTRACT_VIRAL_READS_ONT` subworkflow to route Ultima
reads through the long-read (minimap2-based) pipeline. Leverages minimap2's natural
indel tolerance but requires new minimap2 indexes built with a short-read preset.

## Key Rationale
Minimap2 handles indels much more gracefully than Bowtie2 -- its gap-affine scoring
and chaining algorithm were designed for error profiles with frequent indels. All three
LLM notes agree minimap2 is a strong choice for Ultima. The ONT path already uses
minimap2 end-to-end, making it a natural fit.

---

## Input Handling

Same as Illumina path:
1. Concatenate per-sample FASTQs (i5+i7 and i5-only files) before pipeline entry.
2. Single-end samplesheet format (`sample,fastq`).
3. Add `"ultima"` platform, route to the `params.platform == "ont"` branch in run.nf
   (or add a third branch `params.platform == "ultima"` that calls
   `EXTRACT_VIRAL_READS_ONT` with different parameters).

## Viral Read Extraction (EXTRACT_VIRAL_READS_ONT adaptations)

### Step 1: Quality/length filtering (currently FILTLONG)
**Current**: FILTLONG with min_length=50, max_length=15000, min_mean_q=90.
**Problem**: FILTLONG is designed for ONT reads (long, high error rate). Its quality
model interprets quality scores differently than what Ultima produces. The min_mean_q=90
threshold is calibrated for ONT Phred scores, not Ultima's neural-network-derived
quality scores. Max_length=15000 is irrelevant (Ultima reads are <300bp).
**Change**: Replace FILTLONG with FASTP in single-end mode for this step:
- `--length_required 50` (min length after any trimming)
- `--cut_front --cut_tail --cut_mean_quality 15` (light quality trimming)
- `--trim_poly_x` (remove poly-A/T tails)
- `--low_complexity_filter` (entropy filtering)
- `--dont_eval_duplication` (not useful for metagenomics)
- Keep adapter trimming enabled (`--adapter_sequence` / `--adapter_fasta` with standard
  Illumina adapters) as a safety net. The Minnesota RNA-seq benchmark found residual
  Illumina adapter sequences (~70bp) surviving on-instrument Trimmer processing when
  Illumina-adapter library prep kits were used on Ultima -- exactly our scenario. If
  Trimmer already removed everything, FASTP finds nothing. If residuals remain, they
  get caught.
FASTP is already available in the pipeline and well-suited for short variable-length
reads.
**Alternative**: Use FILTLONG with adjusted parameters (min_length=50, max_length=500,
min_mean_q=70). Simpler change but FILTLONG's quality model may not be well-calibrated
for Ultima.

### Step 2: Complexity masking (currently MASK_FASTQ_READS)
**Current**: Window=25, complexity_threshold=0.55. Masks low-complexity regions to
reduce spurious alignments.
**Change**: Keep this step but relax threshold slightly to 0.45-0.50. Ultima reads
have fewer extreme homopolymer runs than ONT (typical homopolymers <12bp vs ONT's
>20bp stretches). Aggressive masking may remove legitimate viral sequences with
biological poly-A tails (common in RNA viruses).
**Rationale**: The Claude notes specifically flag this tension -- "poly-A-adjacent read
quality will be lower than average" but "aggressive low-complexity filtering could
remove real viral reads with legitimate poly-A tails."

### Step 3: Human depletion (currently MINIMAP2_HUMAN)
**Current**: Minimap2 with `lr:hq` preset index, no extra alignment params.
**CRITICAL CHANGE**: The existing minimap2 indexes are built with `-x lr:hq` preset
(k=19, w=19), which is optimized for long reads. Ultima reads (~250-300bp) need
short-read indexes built with `-x sr` preset (k=21, w=11).
**Options**:
  a) Build new minimap2 indexes with `-x sr` and store alongside existing ones
     (e.g., `mm2-sr-human-index`, `mm2-sr-virus-index`, `mm2-sr-other-index`).
     This requires running the INDEX workflow with modified MINIMAP2_INDEX preset.
  b) Pass `-x sr` as alignment_params at runtime, overriding the index preset.
     Minimap2 CAN re-derive indexing parameters at alignment time, but this is slower
     and the docs warn about preset mismatches.
  c) Use the existing `lr:hq` indexes and just pass reads -- minimap2 may still work
     but with suboptimal sensitivity for short reads.
**Recommendation**: Option (a) -- build proper `-x sr` indexes. This is a one-time
cost and critical for alignment quality. The INDEX workflow already has MINIMAP2_INDEX;
just need to parameterize the preset.
**Alignment params**: `-ax sr` for short-read mode. Add `-N 10` for viral alignment
(multiple hits for LCA).

#### How hard is rebuilding indexes?

Not hard. The MINIMAP2_INDEX process (minimap2/main.nf:1-22) is a single command:
```
minimap2 -x sr -d output.mmi reference.fasta
```
The preset is currently hardcoded as `lr:hq` on line 14. To build sr-preset indexes,
you'd change that to `sr` (or parameterize it) and re-run the INDEX workflow. The
process uses the `max` label (32 CPUs, 64GB RAM), which is appropriate.

Existing lr:hq indexes on S3 (`s3://nao-mgs-index/20250825/output/results/`):
- `mm2-virus-index`: 2.8 GiB
- `mm2-human-index`: 5.5 GiB
- `mm2-other-index`, `mm2-ribo-index`: similar scale

The sr-preset indexes will be comparable in size. Building each one is a single
minimap2 command that takes minutes to tens of minutes on a beefy instance.

**Two approaches to building:**
1. **Use the INDEX workflow**: Parameterize the preset, run the full workflow. This
   rebuilds everything (bt2 indexes, kraken DB, etc.) which is overkill if you only
   need minimap2 indexes.
2. **Just run minimap2 manually on EC2**: Download each reference FASTA, run
   `minimap2 -x sr -d mm2_index.mmi reference.fasta`, upload to S3. Simpler and
   faster for a one-off. The reference FASTAs are already on S3 as intermediates
   from the INDEX workflow (e.g., `virus-genomes-masked.fasta.gz`).

Either way, this is a straightforward, low-risk task.

### Step 4: Contaminant depletion (currently MINIMAP2_CONTAM)
**Change**: Same as human depletion -- use sr-preset indexes, `-ax sr` alignment.
**Note**: The ONT path uses `MINIMAP2_NON_STREAMED` for contaminant depletion (writes
full SAM to disk before partitioning, line 92 of minimap2/main.nf). This is used
because the contaminant index needs `--split-prefix` for large multi-reference indexes.
For Ultima with ~300M+ reads reaching this step, the intermediate SAM file could be
60-150GB. This will work on an instance with enough disk space but is worth being
aware of. See scaling section below.

### Step 5: Viral identification (currently MINIMAP2_VIRUS)
**Current**: Minimap2 with `-N 10` for multi-mapping.
**Change**: Use sr-preset virus index, `-ax sr -N 10` alignment params.
This is where minimap2 really shines for Ultima -- its gap-tolerant alignment will
recover viral reads that Bowtie2 would miss due to homopolymer indels.

### Step 6: Extract unmasked viral reads (EXTRACT_VIRAL_FILTERED_READS)
**Current**: Uses `seqtk comp` to get read IDs from virus-mapped FASTQ, then
`seqtk subseq` to extract those reads from the original filtered FASTQ.
**Change**: Works as-is conceptually. The input is virus-mapped reads (typically <1%
of total), so the read ID list is small even with 333M total reads. However, the
second input (original filtered FASTQ) is the full ~10-40GB file, which `seqtk subseq`
will need to scan. This should stream fine.
**Resource concern**: The process has label `single` (1 CPU, 4GB RAM). `seqtk comp`
on a small virus-mapped FASTQ should be fine. But if virus mapping rate is higher
than expected, consider bumping to `small`.

### Step 7: SAM processing (PROCESS_VIRAL_MINIMAP2_SAM)
**Current**: Processes minimap2 SAM output to TSV.
**Change**: Should work as-is. The minimap2 SAM format is the same regardless of
read length. Verify that score normalization (length_normalized_score) behaves
reasonably for ~250bp reads vs the >1000bp ONT reads it was designed for.

### Step 8-9: LCA and output processing
**Change**: Works as-is -- same as ONT path.

---

## Scaling Concerns for Ultima File Sizes

ONT files are typically 50MB-5GB per sample. Ultima FASTQs are 10-40GB with ~333M
reads each. Several parts of the ONT path will be stressed:

### Minimap2 threading (IMPORTANT)
The MINIMAP2 and MINIMAP2_NON_STREAMED processes do NOT pass `-t` to minimap2 (no
`-t` flag in the commands at minimap2/main.nf:52,92). Minimap2 defaults to 3 threads.
The processes are allocated 16 CPUs (large) or 32 CPUs (max) that go unused.

For 333M reads, each minimap2 run at 3 threads could take many hours. With 3
sequential minimap2 runs (human, contam, virus), this adds up.

**Fix**: Add `-t ${task.cpus}` to both minimap2 invocations. This is a one-line
change that dramatically improves throughput. Should be done regardless of which
path we choose.

### MINIMAP2_NON_STREAMED disk usage
Writes `complete_sam.sam` to disk (line 92). For ~300M input reads (post human
depletion), this SAM file could be 60-150GB. The `max` label doesn't specify disk,
so this depends on instance configuration. Need to ensure sufficient local disk or
use an instance with large NVMe storage.

The streamed `MINIMAP2` process (used for human and virus) avoids this by piping
through samtools directly -- no intermediate file. If possible, investigate whether
`--split-prefix` can work in streamed mode for the contaminant index.

### PROCESS_VIRAL_MINIMAP2_SAM sort operations
The `sort_sam.py` and `sort_fastq.py` scripts use GNU sort with `-S 2G` default
buffer. For virus-mapped reads (<1% of total), the SAM and FASTQ files should be
manageable (a few GB at most). Not a concern unless virus mapping rate is
unexpectedly high.

### Overall assessment
With the threading fix, the ONT path should handle Ultima file sizes fine. The main
bottleneck is raw alignment time (3 sequential minimap2 runs on 300M+ reads), but
with 16-32 threads this is feasible in hours, not days. The intermediate file sizes
are large but manageable with appropriate instance storage.

**Sharding**: Splitting input FASTQs into chunks (e.g., 50M reads each) and
processing in parallel could help with the 3 sequential minimap2 runs. Nextflow
supports this via `splitFastq`. However, this adds complexity (need to merge
results) and may not be necessary if the threading fix is sufficient. Consider
this as a fallback if runtime is still too long.

---

## Subset/Trim (SUBSET_TRIM adaptations)

**Current (ONT)**: FILTLONG_STRINGENT (100-15000bp, Q90) and FILTLONG_LOOSE (1-500000bp).
**Change**: Replace both with FASTP (as in Step 1 above). For the "stringent" version,
use quality trimming. For the "loose" version (just to avoid OOM), pass through as-is
or use minimal FASTP filtering.
**Alternative**: Add an `ultima` branch in SUBSET_TRIM alongside `ont` and the else
(short-read) branch.

## QC (RUN_QC)
- Works as-is with single-end data.

## Taxonomic Profiling (PROFILE adaptations)

### Ribo separation
**Current (ONT)**: Minimap2 with mm2-ribo-index.
**Change**: Need sr-preset ribo index as well (`mm2-sr-ribo-index`), or switch to
BBDuk for ribo separation (simpler, already works for short reads, and ribo sequences
are well-conserved so kmer matching is less affected by indels).
**Recommendation**: Use BBDuk for ribo separation (borrow from Illumina path). Ribo
sequences have few homopolymers, so kmer sensitivity loss is minimal.

### Taxonomy (Kraken/Bracken)
- Single-end: skip BBMERGE/JOIN_FASTQ (pass-through via MERGE_JOIN_READS).
- Kraken2 with standard parameters should work. Ultima reads are in the same length
  range as Illumina, so k=35 is fine.

---

## Should this be a new EXTRACT_VIRAL_READS_ULTIMA subworkflow?

**Yes, probably.** The changes accumulate enough that piggybacking on
EXTRACT_VIRAL_READS_ONT becomes messy:

1. **FILTLONG → FASTP**: Different module, different parameters, different outputs
2. **MASK_FASTQ_READS**: Different threshold, possibly skip entirely
3. **Minimap2 indexes**: Different preset (sr vs lr:hq) -- need different index paths
4. **Minimap2 alignment params**: Need `-ax sr` added to all 3 calls
5. **Minimap2 threading**: Need `-t ${task.cpus}` (should fix for ONT too, but still)
6. **FILTLONG_STRINGENT/LOOSE in SUBSET_TRIM**: Need FASTP substitutions

You could parameterize all of this via a params map, but at that point the
subworkflow has more conditional logic than actual pipeline logic. A dedicated
`EXTRACT_VIRAL_READS_ULTIMA` subworkflow that reuses the same modules (MINIMAP2,
PROCESS_VIRAL_MINIMAP2_SAM, LCA_TSV, etc.) with Ultima-appropriate parameters
would be cleaner and easier to understand.

This also applies to the Illumina path (see plan_illumina_path.md) -- the changes
to EXTRACT_VIRAL_READS_SHORT (BBDuk interleaving, FASTP interleaved flag,
FILTER_VIRAL_SAM single-end bypass, PROCESS_BOWTIE2_SAM paired flag) are similarly
pervasive.

The new subworkflow would be ~80-100 lines of Nextflow, composed entirely of
existing modules with Ultima-specific parameters. The effort is modest and the
result is much clearer than conditional spaghetti in an existing subworkflow.

---

## Infrastructure Changes

1. **loadSampleSheet**: Add `"ultima"` platform with `"single"` endedness.
2. **run.nf**: Either route `ultima` to ONT branch, or create a third branch.
3. **MINIMAP2_INDEX**: Parameterize the preset (currently hardcoded `lr:hq`). Add
   ability to build sr-preset indexes.
4. **INDEX workflow**: Build 4 new minimap2 indexes (virus, human, other, ribo) with
   `-x sr` preset. Store as `mm2-sr-*-index` in ref_dir.
   OR: just build them manually on EC2 (simpler for a one-off).
5. **Minimap2 threading**: Add `-t ${task.cpus}` to MINIMAP2 and MINIMAP2_NON_STREAMED.
   (This benefits ONT runs too -- should be done regardless.)
6. **No new containers needed** -- minimap2, fastp, samtools all already available.

## Pros
- **Best alignment sensitivity for Ultima's error profile** -- minimap2 handles indels
  naturally, no gap penalty tuning needed.
- **Proven for metagenomics** -- minimap2 `-ax sr` is well-validated for short-read
  metagenomics.
- **Fast** -- minimap2 is ~3x faster than Bowtie2 for >100bp reads, which matters
  at 10 billion reads. (But only with threading fix.)
- **SAM processing reuse** -- PROCESS_VIRAL_MINIMAP2_SAM already handles single-end
  minimap2 output correctly (no paired-end column issues, no FILTER_VIRAL_SAM rework).

## Cons
- **Requires new minimap2 indexes** -- must build mm2-sr-* indexes (4 indexes: virus,
  human, other, ribo). One-time cost but adds a prerequisite step before running.
  (Straightforward to build -- see index section above.)
- **Less comparable to Illumina results** -- different aligner means sensitivity/
  specificity profiles will differ from the Illumina Bowtie2 runs, making head-to-head
  comparison harder. Differences in viral hits could reflect aligner differences rather
  than platform differences.
- **FILTLONG replacement** -- need to swap FILTLONG for FASTP, which is a code change
  in the subworkflow (not just a parameter tweak).
- **Score threshold calibration** -- minimap2 scores are on a different scale than
  Bowtie2. The existing `bt2_score_threshold` doesn't apply. Need to determine
  appropriate minimap2 score cutoff (or rely on the ONT path's existing thresholds,
  which may not be calibrated for short reads).
- **Index preset mismatch risk** -- if someone accidentally uses lr:hq indexes with
  sr alignment, results will silently degrade.

## Estimated Effort
**Medium** (unchanged). Requires building new indexes (straightforward), swapping
FILTLONG for FASTP, adding minimap2 threading, and parameterizing the minimap2 preset.
With a new EXTRACT_VIRAL_READS_ULTIMA subworkflow (~80-100 lines), the code is clean
and composed of existing modules. The viral extraction logic itself is straightforward
to adapt because PROCESS_VIRAL_MINIMAP2_SAM already handles single-end data correctly.
Main risk is score threshold calibration requiring empirical iteration.
