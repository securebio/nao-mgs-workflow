# Ultima Adaptation: Illumina Path

## Strategy
Minimally modify the existing `EXTRACT_VIRAL_READS_SHORT` subworkflow and supporting
infrastructure to route Ultima single-end reads through the short-read (Bowtie2-based)
pipeline. Keeps maximal comparability with existing Illumina results.

## Key Ultima Facts Driving Decisions
- Single-end, variable length (~50-300bp, avg ~250-300bp)
- Adapter-trimmed by Ultima Trimmer, but residual adapters possible (see Step 2)
- Indel-dominant error profile (especially homopolymers)
- Each sample has 2 FASTQs to concatenate before pipeline entry
- ~333M reads/sample, 30 samples

---

## Input Handling

1. **Concatenate per-sample FASTQs** before pipeline entry (the "i5+i7" and "i5-only"
   files). This is a pre-pipeline step -- just `cat` them together.
2. **Samplesheet**: Use single-end format (`sample,fastq`).
3. **Platform registration**: Add `"ultima"` to `loadSampleSheet` as an allowed platform
   with `single` endedness. Route it to the `else` (short-read) branch in `run.nf`,
   not the ONT branch.

## Viral Read Extraction (EXTRACT_VIRAL_READS_SHORT adaptations)

### Overview of single-end changes needed

The existing `EXTRACT_VIRAL_READS_SHORT` subworkflow is pervasively paired-end.
Several modules and calls need to be branched or replaced for single-end Ultima data.
The key structural changes are:
- Replace `BBDUK_HITS_INTERLEAVE` (which hardcodes `reads[0]`/`reads[1]` interleaving)
  with a single-end BBDuk call
- Pass `interleaved=false` to FASTP (currently hardcoded `true` at line 54)
- Pass `interleaved=false` to Bowtie2 calls (unmapped_flag=4 instead of 12)
- Route around the paired-end FILTER_VIRAL_SAM logic (see Step 5 below)
- Pass `--paired False` to PROCESS_VIRAL_BOWTIE2_SAM (currently hardcoded `true`)
- Use unpaired column lists for downstream LCA and output processing

### Step 1: BBDuk initial viral kmer screen
**Current**: k=24, min_kmer_hits=1, exact match, interleaved via `BBDUK_HITS_INTERLEAVE`.
**Change**: Use a non-interleaving BBDuk call (either the existing `BBDUK` module with
appropriate params, or a small `BBDUK_HITS_SINGLE` variant). Lower k to 21, add
`hdist=1` to tolerate single-base indels in homopolymer regions.
**Why a different BBDuk module**: `BBDUK_HITS_INTERLEAVE` hardcodes `reads[0]`/`reads[1]`
array indexing to paste two FASTQs together. Single-end data has one file -- this would
crash. The regular `BBDUK` module uses `minkmerfraction` rather than `minkmerhits`, so
we may need to add `minkmerhits` support or create a single-end variant.
**Risk**: Slightly increased false-positive rate in the pre-filter (more non-viral reads
passed to alignment), but this is a permissive pre-filter anyway -- false negatives
(missed viral reads) are the bigger concern. Modest compute cost increase from
aligning more reads.
**Alternative**: Skip BBDuk entirely and align all reads. With ~333M reads/sample this
would be expensive but feasible. Probably not worth it since relaxed BBDuk should
recover most viral reads.

### Step 2: FASTP adapter trimming / QC
**Current**: Full adapter removal, poly-X trimming, quality trimming, low-complexity
filter, overlap-based correction, interleaved mode.
**Change**: Disable interleaved mode (`--interleaved_in` removed). Disable
`--detect_adapter_for_pe` and `--correction` (both paired-end features). Keep poly-X
trimming (useful for Ultima poly-A/T artifacts). Keep low-complexity filter. Keep
quality trimming but consider lowering `--cut_mean_quality` to 15 (Ultima Q-scores
reflect homopolymer length probability, not substitution probability, so Q20 may be
overly aggressive). Add `--length_required 50` minimum length filter.
**Adapter trimming -- keep it enabled**: The core says Trimmer removes adapters, but
the Minnesota RNA-seq benchmark (referenced in the Claude LLM notes) found that when
using Illumina-adapter library prep kits on Ultima, adapters comprising the first ~70
base pairs were still present after on-instrument trimming and had to be removed
separately. Our library prep uses Illumina dual-unique barcodes on Ultima -- exactly
the scenario Minnesota flagged. Keep `--adapter_sequence` / `--adapter_fasta` with
standard Illumina adapters as a safety net. If Trimmer already removed everything,
FASTP finds nothing and moves on. If there are residuals, they get caught. Could
also run FastQC on a small raw sample first to check the Adapter Content plot.

### Step 3: Bowtie2 viral alignment
**Current**: `--local --very-sensitive-local --score-min G,0.1,19 -k 10`, interleaved.
**Change**:
- Remove `--interleaved` flag (single-end data).
- Relax gap open/extend penalties: add `--rdg 3,1 --rfg 3,1` (default is 5,3 / 5,3).
  This is the single most important tuning for Ultima -- Bowtie2's default gap penalties
  are calibrated for Illumina's rare indels, while Ultima's dominant error mode is indels.
- Keep `--very-sensitive-local` and `-k 10` (multi-mapping for LCA).
- Keep `--score-min G,0.1,19` initially, but may need to lower threshold since relaxed
  gap penalties change the score distribution.
**Risk**: This is the weakest link in this approach. Even with tuned penalties, Bowtie2
was designed for substitution-dominant errors. Expect ~5-15% lower viral mapping rate
vs BWA-MEM or minimap2 for reads traversing homopolymers. For a pilot comparison
with Illumina data, this tradeoff may be acceptable for comparability.

### Step 4: Bowtie2 human/contaminant depletion
**Current**: `--local --very-sensitive-local`, interleaved.
**Change**: Remove `--interleaved`. Add same relaxed gap penalties `--rdg 3,1 --rfg 3,1`.
For depletion, we want to be aggressive (catch all human reads), so relaxed gaps
actually help here -- human reads with homopolymer indels are more likely to be
correctly identified as human.
**Risk**: Marginal increase in false-positive depletion (non-human reads incorrectly
mapped to human). Unlikely to be significant given --very-sensitive-local.

### Step 5: FILTER_VIRAL_SAM -- significant rework needed
**Current**: Keeps contaminant-free reads, applies score threshold, adds synthetic
unmapped mates for unpaired reads. Deeply paired-end throughout.
**What breaks for single-end** (from code audit of `filter_viral_sam.py`):
- **Synthetic mate creation** (lines 194-235): Flips SAM flag bits 64/128 (read1/read2)
  via XOR. Single-end reads don't have these bits set, producing invalid flags.
- **`group_other_alignments()`** (lines 308-364): Asserts `mate_alignment_score` (YS:i
  tag) is not None. Single-end reads have no YS:i tag -- **crashes with assertion error**.
- **TLEN grouping**: Uses `abs(alignment.tlen)` as a grouping key. Single-end tlen=0,
  so unrelated alignments could incorrectly group together.
- **Pair status branching**: Code branches on CP/DP/UP pair status tags that Bowtie2
  won't produce in the same way for single-end.
**What to do**: The simplest approach is to add a `--single-end` flag to
`filter_viral_sam.py` that skips the mate-related logic entirely: no synthetic mate
creation, no pair-based grouping, no YS:i tag extraction. For single-end reads, the
filtering reduces to: (1) keep only reads present in the clean FASTQ, (2) apply
score threshold, (3) sort by read ID. The pair-grouping and mate-synthesis logic is
specifically about reconstructing pairs for downstream paired-end output -- irrelevant
for single-end.
**Testing**: This is the highest-risk change in this plan. Will need careful testing
with a small subset to verify the single-end path produces correct output.

### Step 6: PROCESS_VIRAL_BOWTIE2_SAM -- use unpaired mode
**Current**: Hardcoded `paired=true` in the subworkflow call (line 80).
**Change**: Pass `--paired False`. The script already has a `process_unpaired_sam()`
function (lines 619-666) and `SAM_HEADERS_UNPAIRED` schema (without `_rev` columns,
`fragment_length`, or `pair_status`). This path works for single-end data as-is.
**Score threshold**: The `bt2_score_threshold` (default 20) may need adjustment given
the changed gap penalty scoring, but this is a tuning question, not a code change.

### Steps 7-9: LCA and output processing
**Change**: Use the ONT-style `col_keep_no_prefix` and `col_keep_add_prefix` lists
(without paired-end-specific columns like `pair_status`, `fragment_length`,
`query_len_rev`, etc.). The LCA logic itself is platform-agnostic. The output schemas
can be ignored/disabled for this pilot since they're not needed for non-production code.

---

## Subset/Trim (SUBSET_TRIM adaptations)

- Route through single-end subsetting (`SUBSET_READS_SINGLE_TARGET`), not paired.
  `SUBSET_READS_PAIRED_TARGET` hardcodes `reads[0]`/`reads[1]` and would crash.
- The SUBSET_TRIM subworkflow already branches on `single_end` for subsetting, so
  this should work if `single_end` is correctly propagated.
- For trimming: Use FASTP in single-end mode (not interleaved). Same parameter
  adjustments as Step 2 above.

## QC (RUN_QC)
- Should work as-is with single-end data. FastQC handles single-end fine.
- `COUNT_READS` already handles single-end correctly (`n_read_pairs=NA`).

## Taxonomic Profiling (PROFILE adaptations)

### Ribo separation
**Current (short-read)**: BBDuk with ribo reference, interleaved mode.
**Change**: BBDuk in single-end mode. The PROFILE subworkflow already sets
`interleaved: !single_end` dynamically (line 41), so this should just work if
`single_end` is propagated correctly. Consider relaxing kmer parameters slightly
(hdist=1) for same indel-tolerance reasons.

### Taxonomy (Kraken/Bracken)
**Current (paired-end)**: BBMERGE + JOIN_FASTQ to produce single sequence per pair,
then Kraken2.
**Change**: Since Ultima is single-end, route through the single-end branch of
MERGE_JOIN_READS (pass-through, no merging). This branching already exists.
Kraken2 with default k=35 should work but expect slightly reduced classification
rate vs Illumina. Consider lowering `--confidence` threshold slightly to compensate
for kmer loss from indels.

---

## Should this be a new EXTRACT_VIRAL_READS_ULTIMA subworkflow?

**Yes.** The single-end changes to EXTRACT_VIRAL_READS_SHORT are pervasive enough that
a new subworkflow is cleaner than conditional branching:
- BBDuk: replace BBDUK_HITS_INTERLEAVE with single-end variant
- FASTP: interleaved=false (currently hardcoded true)
- Bowtie2: interleaved=false for all 3 calls
- FILTER_VIRAL_SAM: need single-end bypass of mate logic
- PROCESS_VIRAL_BOWTIE2_SAM: paired=false (currently hardcoded true)
- Column lists: unpaired schema instead of paired

That's conditional logic at nearly every step. A dedicated subworkflow (~80-100 lines)
using the same modules with Ultima-appropriate parameters is easier to write, test,
and understand. The same conclusion applies to the ONT path (see plan_ont_path.md).

## Infrastructure Changes

1. **loadSampleSheet**: Add `"ultima"` to allowed_platforms with `"single"` endedness.
   Add to implemented_platforms/implemented_endedness (or use development_mode).
2. **run.nf**: Add a third branch for `params.platform == "ultima"` that calls
   `EXTRACT_VIRAL_READS_ULTIMA`.
3. **Bowtie2 module**: Already handles single-end input via `interleaved` parameter.
4. **No new containers needed** -- all tools already Dockerized.
5. **No new indexes needed** -- reuses existing Bowtie2 indexes.

## Pros
- **Maximum comparability** with existing Illumina results (same tools, same workflow)
- **No new indexes** -- reuses existing bt2-virus-index, bt2-human-index, bt2-other-index
- **No new tools or containers**

## Cons
- **Bowtie2 is suboptimal for Ultima's indel errors** -- even with tuned gap penalties,
  expect lower viral sensitivity than minimap2 or BWA-MEM. This is the main weakness.
- **BBDuk kmer screening lossy** -- homopolymer indels break exact kmers. Relaxing
  params helps but doesn't fully solve it.
- **Score calibration uncertainty** -- Bowtie2's score-min and the downstream
  bt2_score_threshold were calibrated for Illumina error profiles. May need empirical
  tuning for Ultima.
- **FILTER_VIRAL_SAM requires significant rework** -- not just "verify it handles
  single-end gracefully." The paired-end logic (synthetic mate creation, pair-based
  grouping, YS:i tag assertions) must be bypassed or rewritten for single-end. This is
  the biggest code risk in this plan.
- **EXTRACT_VIRAL_READS_SHORT has multiple paired-end hardcodings** --
  `BBDUK_HITS_INTERLEAVE`, `FASTP(interleaved=true)`, `PROCESS_VIRAL_BOWTIE2_SAM(paired=true)`
  all need branching or replacement. The subworkflow needs nontrivial restructuring
  rather than just parameter tweaks.

## Estimated Effort
**Medium** (revised upward from small-medium). The parameter changes (gap penalties,
kmer sizes, quality thresholds) are straightforward. But the single-end adaptation of
`EXTRACT_VIRAL_READS_SHORT` is more involved than initially scoped:
- `filter_viral_sam.py` needs a new `--single-end` code path (skip mate logic, simplify
  grouping) -- this is the highest-risk item and will need careful testing
- The subworkflow itself needs branching at 3-4 points for interleaved/paired flags
- BBDuk needs a single-end variant or adaptation

The PROFILE, QC, and SUBSET_TRIM arms are lower risk -- they already have single-end
branching in place. The main risk concentration is in the viral extraction subworkflow,
specifically FILTER_VIRAL_SAM.
