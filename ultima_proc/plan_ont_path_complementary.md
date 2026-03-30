# Ultima Adaptation: Modified ONT Path (Complement to Simulated PE)

## Role in the Analysis

This pipeline is designed as a **diagnostic complement** to plan_simulated_pe.md,
not as a standalone "best approach." The two pipelines together enable a three-way
comparison:

- **Illumina data -> standard pipeline**: the reference
- **Ultima data -> simulated PE (plan_simulated_pe)**: holds bioinformatics ~constant,
  so differences from the reference isolate **sequencing platform effects**
- **Ultima data -> this pipeline**: deliberately different bioinformatics, so
  differences from simulated_pe (same data, different pipeline) isolate
  **bioinformatic choice effects**

Any viral hit that appears in all three is robust. Hits that appear in only one or
two point to either platform or methodology as the explanation -- the pattern of
agreement/disagreement is the diagnostic signal.

## Strategy

All-minimap2, no BBDuk pre-filter, no masking, minimap2 ribo separation.
Maximally different from simulated_pe on every pipeline dimension:

| Dimension | Simulated PE | This pipeline |
|-----------|:-----------:|:-------------:|
| Viral aligner | Bowtie2 | minimap2 |
| Depletion aligner | Bowtie2 | minimap2 |
| BBDuk pre-filter | yes (hard kmer filter) | **no** |
| Processing order | viral-first + retroactive filter | deplete-first |
| Endedness | simulated PE hack | native single-end |
| Low-complexity handling | FASTP filter | FASTP filter (same) |
| Ribo separation | BBDuk (kmer) | minimap2 (alignment) |
| Retroactive SAM filter | FILTER_VIRAL_SAM | none needed |

### Why each difference matters for interpretation

**No BBDuk pre-filter**: Both simulated_pe and plan_middle_path use BBDuk as a hard
pre-filter, sharing the risk that viral reads with indel-disrupted kmers are
permanently lost. Skipping BBDuk here lets us directly quantify that sensitivity cost.
If this pipeline finds viruses that simulated_pe doesn't, BBDuk kmer sensitivity for
Ultima is a candidate explanation.

**minimap2 for depletion (not just viral)**: Bowtie2's gap penalties could cause it
to miss human reads with homopolymer indels, letting contamination leak into viral
results. Using minimap2 for depletion lets us detect this. If simulated_pe has higher
"viral" hit rates that this pipeline doesn't replicate, undepleted human reads are a
candidate explanation.

**Deplete-first ordering**: Reads reaching viral alignment are already
human/contaminant-free, so no retroactive FILTER_VIRAL_SAM needed. This sidesteps
the paired-end assumptions in filter_viral_sam.py entirely.

---

## Data Preparation

### Input
Same concatenated single-end FASTQs as simulated_pe (before the seqtk revcomp step).

### Samplesheet
Single-end format with `ultima` platform:
```csv
sample,fastq_1
sample_01,s3://bucket/sample_01.fastq.gz
sample_02,s3://bucket/sample_02.fastq.gz
```

Requires adding `"ultima"` platform to loadSampleSheet with `"single"` endedness.

---

## Pipeline Flow

```
Raw reads (single-end FASTQ, concatenated per sample)
  -> FASTP (QC, poly-X trim, complexity filter, adapter safety net)
  -> minimap2-human (-ax sr, mm2-sr-human-index, depletion)
  -> minimap2-contaminant (-ax sr, mm2-sr-other-index, depletion)
  -> minimap2-virus (-ax sr -N 10, mm2-sr-virus-index)
     -> SAM + reads_mapped FASTQ
  -> PROCESS_VIRAL_MINIMAP2_SAM (all alignments, with scores)
  => pre_threshold_viral_hits.tsv.gz (intermediate output for threshold tuning)
  -> Score threshold filter (mm2_score_threshold, default 15)
  -> SORT_TSV (sort by seq_id)
  -> LCA_TSV (taxonomic assignment)
  -> PROCESS_LCA_ALIGNER_OUTPUT (column selection, final output)
  => virus_hits.tsv.gz
```

### Step 1: FASTP quality/complexity cleanup
**Module**: FASTP (existing)
**Mode**: Single-end (not interleaved)
**Parameters**:
- `--length_required 50`
- `--cut_front --cut_tail --cut_mean_quality 15` (conservative for Ultima's
  neural-network-derived Q-scores)
- `--trim_poly_x` (poly-A/T artifact removal -- important for RNA-seq + Ultima)
- `--low_complexity_filter` (entropy filter, replaces MASK_FASTQ_READS)
- `--dont_eval_duplication`
- No `--correction` or `--detect_adapter_for_pe` (single-end)
- Keep adapter trimming enabled as safety net for residual Illumina adapters
  surviving Ultima's on-instrument Trimmer

**Why FASTP rather than FILTLONG or other tools**: FILTLONG is designed for ONT's
long reads -- its quality model may not be calibrated for Ultima's neural-network-
derived Q-scores, and it lacks adapter trimming, poly-X trimming, and complexity
filtering. FASTP is purpose-built for short reads, handles all of these in one
pass, and produces QC JSON that integrates with the pipeline's reporting. Other
candidates considered: Cutadapt (excellent trimmer but no complexity filtering --
would need a second tool), BBDuk-as-preprocessor (can do quality + entropy
filtering, but not standard for comprehensive QC and doesn't produce the QC JSON),
Trimmomatic (largely superseded by FASTP).

**Why FASTP's complexity filter instead of MASK_FASTQ_READS**: The key trade-off:
FASTP's `--low_complexity_filter` removes entire reads below a complexity threshold,
while BBMask (MASK_FASTQ_READS) masks low-complexity *regions* with N's, preserving
the rest of the read for alignment. Masking is better for reads with mixed content
(e.g., 200bp viral + 50bp low-complexity). However, this concern is partially
mitigated by FASTP's `--trim_poly_x` running *before* the complexity filter -- the
most common case (RNA virus read with terminal poly-A) gets the poly-A trimmed
first, leaving complex sequence that passes. The problem case -- internal low-
complexity regions -- is less common in 250bp reads than in ONT's multi-kb reads.

Additional considerations: FASTP's complexity threshold is not user-configurable
(hardcoded internally), while BBMask's entropy threshold (0.55) and window size
(25) are both tunable. If fine control is needed, BBMask is better.

**Contingency**: After running 2-3 samples, check FASTP's JSON for how many reads
the complexity filter removes. If loss is substantial (>5% of reads), replace
`--low_complexity_filter` with a BBMask step between FASTP and the first minimap2
alignment. This preserves reads while still preventing low-complexity regions from
driving spurious alignments, at the cost of needing EXTRACT_VIRAL_FILTERED_READS
downstream (to recover unmasked sequences for the final output).

### Step 2: minimap2 human depletion
**Module**: MINIMAP2 (existing, streamed version)
**Index**: New `mm2-sr-human-index` (built with `-x sr` preset)
**Alignment params**: `-ax sr -t ${task.cpus}`
**Output**: Unmapped reads (human-depleted) continue downstream.

### Step 3: minimap2 contaminant depletion
**Module**: MINIMAP2_NON_STREAMED (existing -- needed for large composite index
with `--split-prefix`)
**Index**: New `mm2-sr-other-index` (built with `-x sr` preset)
**Alignment params**: `-ax sr -t ${task.cpus}`

**Disk note**: Without BBDuk pre-filtering, ~300M+ reads per sample reach this
step. The intermediate SAM file (`complete_sam.sam`) could be 60-150GB. Need
instances with sufficient local disk (large NVMe). See compute section below.

### Step 4: minimap2 viral alignment
**Module**: MINIMAP2 (existing, streamed version)
**Index**: New `mm2-sr-virus-index` (built with `-x sr` preset)
**Alignment params**: `-ax sr -N 10 -t ${task.cpus}`
**`-N 10`**: Report up to 10 secondary alignments for LCA-based taxonomic
assignment.

### Step 5: SAM processing
**Module**: PROCESS_VIRAL_MINIMAP2_SAM (existing, from ONT path)
**Input**: SAM + reads_mapped FASTQ from step 4.

No EXTRACT_VIRAL_FILTERED_READS needed (no masking to undo). The reads entering
minimap2 already have their final sequences from FASTP cleanup, so `reads_mapped`
can be used directly.

No FILTER_VIRAL_SAM needed. Because we deplete before viral alignment, reads
reaching minimap2 are already human/contaminant-free.

### Step 6: Score threshold filter
**New step** (not present in the current ONT path).

The current ONT path applies NO post-alignment score threshold -- all reads that
minimap2 reports as aligned pass through to the final output. The `bt2_score_threshold`
parameter in `run_ont.config` is defined but never consumed by the ONT extraction
workflow. This works for ONT because long reads (>1kb) produce high normalized
scores even for marginal alignments. For 250bp reads with `-ax sr`, minimap2 may
be more permissive, so we add an explicit threshold.

**Score metric**: `length_normalized_score = AS / ln(query_length)`, same formula
as both the Bowtie2 and existing minimap2 paths. Calculated by
PROCESS_VIRAL_MINIMAP2_SAM for every aligned read.

**Starting threshold: 15.** Rationale:
- For a 250bp read: ln(250) ~ 5.52, so threshold 15 requires raw AS >= 83. With
  minimap2's match bonus of +2, that's ~42 matched bases (17% of read).
- minimap2 `-ax sr` has lighter mismatch penalties (-4) than Bowtie2 (-6) and
  lighter gap penalties, so raw scores run higher for equivalent alignment quality.
  Threshold 15 for minimap2 is roughly comparable stringency to threshold 20 for
  Bowtie2.
- Starting conservatively low lets us see what's near the boundary. We can raise
  it without re-running alignment.

**Implementation**: Filter the PROCESS_VIRAL_MINIMAP2_SAM output TSV, keeping
rows where `length_normalized_score >= mm2_score_threshold`. This can be a small
inline awk/python step in the subworkflow, or a new lightweight module. The
existing FILTER_TSV_COLUMN_BY_VALUE module does string matching only, so it
doesn't work for numeric thresholds.

**Threshold tuning procedure** (see also "Making threshold tuning easy" below):
1. Run initial samples with threshold 15.
2. Examine the pre-threshold TSV (emitted as an intermediate output) to see the
   score distribution. Plot it alongside the Illumina Bowtie2 score distribution
   for the same samples.
3. If junk alignments are passing (high edit distance relative to aligned length),
   raise threshold to 18-20.
4. If real viral hits are being filtered (check by looking at reads just below
   threshold -- are they real viral alignments with homopolymer indels?), lower
   to 12-13.
5. Re-running from the filter step takes seconds per sample (see table below).

**Making threshold tuning easy**:
1. **Emit pre-threshold TSV as intermediate output**: Add the
   PROCESS_VIRAL_MINIMAP2_SAM output to the subworkflow's `emit:` block.
   This allows offline re-filtering without touching Nextflow:
   ```bash
   zcat pre_threshold.tsv.gz | \
     awk -F'\t' 'NR==1 || $COL >= 15' | gzip > filtered.tsv.gz
   ```
2. **Parameterize the threshold**: Add `mm2_score_threshold` as a pipeline
   parameter (in the config file, passable via `--mm2_score_threshold 18` at
   the command line).
3. **Re-run cost for threshold change**:

| Step | Re-run? | Cost |
|------|---------|------|
| FASTP | No | -- |
| minimap2 human/contam/viral | No | -- |
| PROCESS_VIRAL_MINIMAP2_SAM | No | -- |
| **Score filter** | **Yes** | Seconds |
| SORT_TSV | Yes | Seconds |
| LCA_TSV | Yes | Seconds |
| PROCESS_LCA_ALIGNER_OUTPUT | Yes | Seconds |

Everything expensive (alignment) is upstream. Re-running from the filter step
takes seconds per sample. Nextflow's `-resume` will cache the upstream steps.

### Step 7: Sort + LCA + output
**Modules**: SORT_TSV, LCA_TSV, PROCESS_LCA_ALIGNER_OUTPUT (all existing)
**Column lists**: ONT-style (no paired-end columns). Output schemas can be
ignored/disabled for this pilot.

---

## Taxonomic Profiling (PROFILE)

Use the ONT profiling approach (minimap2 for ribo separation) rather than the
Illumina approach (BBDuk). This keeps the all-minimap2 philosophy consistent
and makes profiling another diagnostic axis -- if Kraken results differ
between this pipeline and simulated_pe, we can check whether ribo separation
method or read merging contributes.

### Ribo separation
**Use minimap2** (from ONT path) with a new `mm2-sr-ribo-index`.
- The ONT path already uses minimap2 for ribo separation via the PROFILE
  subworkflow (routes aligned reads as ribo, unaligned as non-ribo).
- Requires a 4th minimap2 index: `mm2-sr-ribo-index` (built with `-x sr`,
  same trivial process as the other three).
- This differs from simulated_pe (which uses BBDuk kmer matching for ribo),
  providing another comparison point.

### Taxonomy (Kraken2/Bracken)
- Single-end: skip BBMERGE/JOIN_FASTQ (pass-through via MERGE_JOIN_READS).
  Kraken sees raw single-end reads, not merged/joined pairs as in the
  Illumina path. This is another difference from simulated_pe.
- Kraken2 with standard k=35 -- Ultima reads are in the same length range
  as Illumina.
- Bracken abundance estimation with standard parameters.

### Profiling differences from simulated_pe

| Step | Simulated PE | This pipeline |
|------|:-----------:|:-------------:|
| Ribo separation | BBDuk (kmer, k=27) | minimap2 (-ax sr) |
| Ribo index | ribo-ref-concat.fasta.gz | mm2-sr-ribo-index |
| Read prep for Kraken | BBMERGE + JOIN_FASTQ | passthrough (SE) |
| Kraken input | merged/joined reads | raw single-end reads |

---

## QC (RUN_QC)
Works as-is with single-end data. FastQC handles single-end fine.

## Subset/Trim (SUBSET_TRIM)
Route through single-end subsetting. Use FASTP for trimming (single-end mode,
same parameters as Step 1).

---

## Infrastructure Changes

1. **loadSampleSheet**: Add `"ultima"` platform with `"single"` endedness.
2. **run.nf**: Add branch for `params.platform == "ultima"` that calls
   `EXTRACT_VIRAL_READS_ULTIMA`. Route PROFILE to the ONT-style minimap2
   ribo separation path.
3. **New subworkflow**: `subworkflows/local/extractViralReadsUltima/main.nf` --
   assembles existing modules in the order above (~80-100 lines of Nextflow).
   Includes score threshold filter step and emits pre-threshold intermediate.
4. **New parameter**: `mm2_score_threshold` (default 15) in the config,
   consumed by the new subworkflow.
5. **Minimap2 threading**: Add `-t ${task.cpus}` to MINIMAP2 and
   MINIMAP2_NON_STREAMED. (Benefits ONT runs too -- should be done regardless.)
6. **Build 4 new minimap2 indexes manually on EC2**: mm2-sr-virus,
   mm2-sr-human, mm2-sr-other, mm2-sr-ribo. Each is a single command:
   `minimap2 -x sr -d mm2_index.mmi reference.fasta`. No changes to the
   MINIMAP2_INDEX module needed (indexes built by hand, not via the INDEX
   workflow).
7. **No new containers or bioinformatics tools needed.**

---

## Compute Considerations

Without BBDuk pre-filtering, ALL reads go through three sequential minimap2 runs.
This is substantially more expensive than simulated_pe or the middle path.

### Per-sample estimate (~333M reads)

| Step | Reads in | Notes |
|------|----------|-------|
| FASTP | 333M | Fast, single-pass |
| minimap2 human | ~320M (post-FASTP) | With `-t 32`, ~1-2 hours |
| minimap2 contam | ~300M (post-human) | ~1-2 hours; large intermediate SAM |
| minimap2 viral | ~280M (post-contam) | ~1-2 hours |
| PROCESS_VIRAL_MINIMAP2_SAM | <1M (virus-mapped) | Minutes |

Total per sample: ~4-7 hours with 32 CPUs. For 30 samples running in parallel
on AWS Batch, wall-clock time is dominated by the slowest sample.

### The threading fix is critical

The existing MINIMAP2 processes don't pass `-t` to minimap2 (defaults to 3
threads while 16-32 CPUs are allocated). Without the fix, each minimap2 run
takes 5-10x longer. This fix should be applied regardless of which path is
chosen.

### Recommended phasing

1. **Run 2-3 samples first** without BBDuk pre-filter. This directly quantifies
   BBDuk's sensitivity cost (compare viral hits to simulated_pe results for the
   same samples).
2. **If compute is prohibitive for 30 samples**, add BBDuk pre-filter for the
   full run. You've already quantified the sensitivity cost from step 1, so you
   know what you're giving up. The pipeline with BBDuk is essentially the middle
   path but with minimap2 depletion.
3. **Subsampling**: For the initial 2-3 samples, consider subsampling to
   50-100M reads to get results in hours rather than overnight.

---

## Interpreting the Three-Way Comparison

### Patterns and what they mean

| Hit pattern | Illumina std | Simulated PE | This pipeline | Interpretation |
|---|:-:|:-:|:-:|---|
| All agree | Y | Y | Y | Robust finding |
| Platform effect | Y | N | N | Both Ultima pipelines miss it -- sequencing platform limitation |
| Platform effect (reverse) | N | Y | Y | Both Ultima pipelines find it -- Ultima detects something Illumina doesn't |
| Bowtie2 indel issue | Y | N | Y | Bowtie2 struggling with Ultima indels; minimap2 recovers |
| BBDuk sensitivity | N | N | Y | No BBDuk pre-filter catches reads that kmer screen misses |
| Depletion leak | N | Y | N | simulated_pe "viral" hit is likely undepleted human/contaminant |
| minimap2 sensitivity | Y | Y | N | minimap2 depletion over-removing, or score threshold issue |
| Aligner artifact | N | Y | N | Bowtie2-specific false positive or simulated PE artifact |

### Key metrics to compare

1. **Viral hit overlap**: Jaccard similarity of detected virus taxa across
   pipelines, per sample.
2. **Read-level concordance (Ultima pipelines only)**: For viruses detected by
   both Ultima pipelines (simulated_pe and this), what fraction of individual
   reads are called as viral by both? This is only meaningful between the two
   Ultima pipelines (same reads, different bioinformatics). Ultima vs Illumina
   comparison must be at the taxon level, not read level, since different
   sequencing runs produce entirely different reads.
3. **Depletion rates**: Fraction of reads removed at human/contaminant steps.
   If this pipeline removes more, minimap2 depletion may be catching reads
   Bowtie2 misses.
4. **Score distributions**: Compare alignment score distributions for shared
   viral taxa. Different score scales (Bowtie2 vs minimap2) but the shapes
   should be informative.
5. **BBDuk sensitivity cost**: (From 2-3 sample no-BBDuk run) How many
   additional viral hits does skipping BBDuk recover?
6. **Taxonomic profile concordance**: Compare Kraken/Bracken abundance
   estimates between pipelines. Differences may reflect ribo separation
   method (minimap2 vs BBDuk) or read preparation (raw SE vs merged pairs).

---

## Pros
- **Maximum diagnostic contrast** with simulated_pe -- differs on every pipeline
  dimension, maximizing interpretive power of the three-way comparison
- **No BBDuk pre-filter** -- directly quantifies kmer sensitivity cost for Ultima
- **Best alignment sensitivity** -- minimap2 handles Ultima indels naturally
- **Clean single-end handling** -- PROCESS_VIRAL_MINIMAP2_SAM already works for
  single-end; no FILTER_VIRAL_SAM rework
- **No masking complexity** -- FASTP complexity filter replaces
  MASK_FASTQ_READS + EXTRACT_VIRAL_FILTERED_READS
- **No new containers or tools** -- all existing modules

## Cons
- **High compute cost** without BBDuk pre-filter -- ~1 billion alignments per
  sample across 3 minimap2 runs. Mitigated by phased approach (2-3 samples
  first, optional BBDuk for full 30).
- **4 new minimap2 indexes** -- one-time build cost, straightforward (one
  `minimap2 -x sr -d` command each on EC2).
- **Score threshold calibration** -- the current ONT path has no explicit
  threshold; we're adding one starting at 15 (see Step 6). Requires empirical
  validation, but re-running from the threshold step is cheap (seconds per
  sample).
- **FASTP complexity filter trade-off** -- removes whole reads vs. BBMask's
  region-level masking. May lose reads with mixed viral + low-complexity content.
  Mitigated by `--trim_poly_x` handling the most common case; contingency plan
  to add BBMask if loss is excessive (see Step 1).
- **Less directly comparable** to Illumina standard pipeline (different aligner,
  different ordering) -- but that's the point; simulated_pe provides the
  comparable run.

## Estimated Effort
**Medium.** New subworkflow (~80-100 lines, composed of existing modules), 4 new
minimap2 indexes (straightforward to build by hand on EC2), minimap2 threading
fix, score threshold filter step, and new `mm2_score_threshold` parameter. No
new tools, containers, or fundamental pipeline logic. Lower code risk than
plan_illumina_path (no FILTER_VIRAL_SAM rework). Main risk is score threshold
calibration requiring empirical iteration, but the pipeline is designed to make
this cheap to iterate on.

---

## Relationship to Other Plans

- **plan_simulated_pe.md**: The primary analysis route. Run first. This pipeline
  is its diagnostic complement, not a replacement.
- **plan_middle_path.md**: A natural follow-up if this pipeline and simulated_pe
  disagree substantially. The middle path (Bowtie2 depletion + minimap2 viral)
  can decompose whether disagreements are driven by the viral aligner or the
  depletion aligner. Also a candidate for production if Ultima sequencing is
  adopted.
- **plan_illumina_path.md**: Ruled out -- superseded by simulated_pe.
