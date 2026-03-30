# Ultima Adaptation: Simulated Paired-End Path

## Strategy
Simulate paired-end data from Ultima single-end reads by generating R2 = revcomp(R1)
for each read, then run through the existing Illumina pipeline with zero (or near-zero)
code changes. This is a quick smoke test, not a production solution.

Two variants:
- **Variant A (zero code changes)**: Use existing pipeline parameters as-is.
- **Variant B (parameter-tuned)**: Edit ~5 hardcoded lines to relax gap penalties and
  kmer parameters for Ultima's indel profile. Same structural pipeline, no plumbing.

---

## Why This Works

When R2 = revcomp(R1), Bowtie2 sees a concordant FR pair where both mates map to the
same position with fragment length = read length. This is equivalent to an Illumina
library with insert size equal to read length (100% overlap) -- unusual but valid.

Every pipeline step handles this correctly:

| Step | What happens | Result |
|------|-------------|--------|
| BBDUK_HITS_INTERLEAVE | Both mates have identical kmer content | Same filter result as single-end |
| FASTP (interleaved) | **Must remove `--detect_adapter_for_pe` and `--correction`** (see Variant B). With those removed, QC/trimming/poly-X/complexity filtering all work fine | Correct after 1-line edit |
| Bowtie2 viral | Concordant FR alignment, TLEN = read length | Correct viral identification |
| Bowtie2 human/other | Both mates co-map | Correct depletion |
| FILTER_VIRAL_SAM | Both mates always present, YS:i tags populated, no synthetic mates needed | Works on trivially well-behaved input |
| PROCESS_VIRAL_BOWTIE2_SAM | Paired mode, fragment_length = read_length | Correct viral hits, meaningless fragment metric |
| BBMERGE (for Kraken) | Merges 100%-overlapping pair back into original read | Kraken sees original read -- taxonomy results identical to single-end |

The key insight: all the plumbing work in plan_illumina_path (single-end BBDuk variant,
FASTP interleaved=false, FILTER_VIRAL_SAM --single-end rework, PROCESS_VIRAL_BOWTIE2_SAM
paired=false, unpaired column lists) exists to make the pipeline accept single-end data.
Simulated PE makes all of that unnecessary by making the data paired-end.

### Scoring and alignment integrity

A natural concern: does simulating paired-end change how reads score, or could the hack
produce misleading alignment results? Short answer: **no**. The paired-end mode is
essentially a no-op wrapper around what is functionally single-end alignment.

**No score bonus from pairing.** Bowtie2 scores each mate independently (AS:i tag).
There is no paired-end score bonus. Since revcomp(R2) = R1, both mates align to the
same position with the same CIGAR and the same score. FILTER_VIRAL_SAM's threshold uses
`max(normalized_score_R1, normalized_score_R2)`, which is `max(x, x) = x` -- identical
to single-end thresholding.

**Mates cannot get primary alignments to different reference genomes.** For concordant
pairs, both mates must be on the same reference. Since every valid alignment position
gives a concordant pair (both mates at the same position, fragment ≤ 300bp < `--maxins`
500), Bowtie2 never falls through to discordant or unpaired modes. All pairs are CP
(concordant pair), both mates on the same reference. This means the pipeline's handling
of discordant references (`genome_id_all = "virus_A/virus_B"`) is never triggered.

**Multi-mapping (-k 10) is unaffected.** Each of the 10 reported alignments is a
concordant pair at a different reference/position. The 10 hits correspond to the same
10 positions single-end mode would report. LCA sees the same set of taxids.

**Soft-clipping is identical.** In `--local` mode, Bowtie2 can soft-clip read ends.
Since revcomp(R2) = R1, both mates present the same sequence for alignment, producing
the same CIGAR string and the same clipping pattern.

**Search heuristics.** Bowtie2 uses slightly different seed strategies in PE vs SE mode.
With `--very-sensitive-local` (the most thorough preset), this should not systematically
change which alignments are found. The primary alignment should be identical to SE.

**Reads >500bp and `--maxins`.** Bowtie2's default `--maxins` (`-X`) is 500 -- the
maximum fragment length for a concordant pair. Fragment length = outer distance from
leftmost to rightmost mapped position, which for our simulated PE equals the read
length. Reads ≤500bp are concordant (CP); reads >500bp would be classified discordant
(DP). Both mates still align correctly to the same position -- only the pair_status
tag differs. FILTER_VIRAL_SAM and PROCESS_VIRAL_BOWTIE2_SAM handle DP pairs correctly,
so this is functionally harmless. However, to keep all reads concordant and avoid any
edge-case surprises, we add `-X 1000` to all Bowtie2 calls (see Variant A and B
below). This provides ample headroom for any Ultima read length.

---

## Data Preparation

### Generate simulated R2 files

```bash
# For each sample's concatenated FASTQ:
seqtk seq -r sample.fastq.gz | gzip > sample_R2.fastq.gz
mv sample.fastq.gz sample_R1.fastq.gz
```

`seqtk seq -r` reverse-complements the sequence and reverses the quality string, which
is exactly what paired-end R2 represents. Read IDs are preserved, so Bowtie2's paired
alignment correctly matches R1 and R2 by position in interleaved mode.

### Samplesheet

Standard paired-end format, platform = `illumina` (or `aviti`):

```csv
sample,fastq_1,fastq_2
sample_01,s3://bucket/sample_01_R1.fastq.gz,s3://bucket/sample_01_R2.fastq.gz
sample_02,s3://bucket/sample_02_R1.fastq.gz,s3://bucket/sample_02_R2.fastq.gz
```

No samplesheet validation changes needed -- the pipeline sees standard paired-end
Illumina data.

### Storage note

R2 files double FASTQ storage (~10-40GB per sample). For 30 samples, that's ~300GB-1.2TB
of additional S3 storage. Trivial cost for a pilot.

---

## Variant A: Near-Zero Code Changes

Two mandatory edits:
1. Remove `--detect_adapter_for_pe` and `--correction` from FASTP (line 30 of
   `modules/local/fastp/main.nf`). These are paired-end features that interact badly
   with simulated PE data -- `--detect_adapter_for_pe` in particular could misinterpret
   the 100% R1/R2 overlap and aggressively trim real sequence (see Variant B for details).
2. Add `-X 1000` to the three Bowtie2 par_strings in
   `subworkflows/local/extractViralReadsShort/main.nf` (lines 62, 67, 71). This raises
   the concordant fragment length ceiling so reads >500bp aren't classified discordant.

**What you get:**
- BBDuk with k=24, minkmerhits=1, no hamming distance tolerance
- FASTP with cut_mean_quality=20, poly-X trimming, low-complexity filter (no PE
  correction or PE adapter detection)
- Bowtie2 with default gap penalties (--rdg 5,3 --rfg 5,3), `-X 1000`
- bt2_score_threshold=20
- Standard Illumina processing throughout

**Limitations:**
- BBDuk k=24 exact match will miss more viral reads than necessary due to homopolymer
  indels breaking kmer matches. This is the same concern as in plan_illumina_path, but
  without mitigation.
- Bowtie2's default gap penalties (5,3 open/extend) penalize indels heavily. Reads with
  homopolymer-length errors may fail to align or get low scores, reducing both viral
  sensitivity and depletion completeness.
- FASTP quality threshold Q20 may be slightly aggressive for Ultima's neural-network-
  derived quality scores.

**When to use:** As a first smoke test to confirm the data is usable, check for obvious
quality issues, and get a rough viral hit count. The sensitivity loss from untuned
parameters is modest (~5-15% for viral mapping) and acceptable for "does anything work
at all?" questions.

---

## Variant B: Parameter-Tuned (Recommended)

Edit ~5 hardcoded parameter lines to optimize for Ultima's indel error profile. No
structural changes, no new modules, no plumbing.

### Changes to existing files

**1. Bowtie2 gap penalties** (`subworkflows/local/extractViralReadsShort/main.nf`)

Add `--rdg 3,1 --rfg 3,1` to relax gap open/extend penalties from 5,3 to 3,1. This
is the single most impactful tuning for Ultima -- it makes Bowtie2 much more tolerant
of the homopolymer indels that dominate Ultima's error profile.

```groovy
// Line 62: viral alignment
def par_string = "--local --very-sensitive-local --score-min G,0.1,19 -k 10 --rdg 3,1 --rfg 3,1 -X 1000"
// Line 67: human depletion
def par_string = "--local --very-sensitive-local --rdg 3,1 --rfg 3,1 -X 1000"
// Line 71: contaminant depletion
def par_string = "--local --very-sensitive-local --rdg 3,1 --rfg 3,1 -X 1000"
```

The `-X 1000` raises the maximum concordant fragment length from 500 to 1000. Since
fragment length = read length for simulated PE, this keeps all Ultima reads (even
the handful >500bp) concordant. Without this, reads >500bp get classified discordant --
functionally harmless, but cleaner to avoid.

**2. BBDuk kmer parameters** (`workflows/run.nf`, lines 83-88)

Lower k from 24 to 21 and add hdist=1 for hamming distance tolerance:

```groovy
min_kmer_hits: "1",
k: "21",            // was 24
hdist: "1",         // new -- tolerate single-base mismatches in kmers
```

Note: `hdist` is not currently passed to the BBDUK_HITS_INTERLEAVE command line. You'd
also need to add `hdist=${params_map.hdist}` to the bbduk command in
`modules/local/bbduk/main.nf` (the BBDUK_HITS_INTERLEAVE process, line ~52). This is
one additional line.

**3. FASTP parameters** (`modules/local/fastp/main.nf`, line 30)

Three changes to the `par` string:

```bash
# Remove --correction and --detect_adapter_for_pe; lower quality to Q15
--cut_front --cut_tail --trim_poly_x --cut_mean_quality 15 --average_qual 15 --qualified_quality_phred 15 --verbose --dont_eval_duplication --thread ${task.cpus} --low_complexity_filter
```

- **Remove `--detect_adapter_for_pe`**: This detects adapters by finding where
  revcomp(R2) overlaps with R1 past the insert boundary. With simulated PE,
  revcomp(R2) = R1 (100% identical), creating an ambiguous input for the overlap
  detection algorithm. It *should* conclude "insert = read length, no adapter
  readthrough," but if it misinterprets the 100% overlap it could aggressively trim
  real sequence. Removing it is safe because `--adapter_fasta` (explicit sequence
  matching against known adapter sequences) is still active and handles adapter
  removal reliably.
- **Remove `--correction`**: Overlap-based error correction uses independent R1/R2
  observations. With simulated PE, both reads come from the same sequencing event --
  there's zero independent information. Correction either does nothing (bases agree
  because they're the same data) or reinforces systematic errors. Harmless in practice
  but conceptually wrong and worth removing for clarity.
- **Lower quality from Q20 to Q15**: Ultima Q-scores are neural-network-derived
  probability estimates, not the same signal-to-noise ratio as Illumina Phred scores.
  Q20 may over-trim.

**4. Score threshold** (configurable via params, no code change needed)

The score threshold is applied in FILTER_VIRAL_SAM to `normalized_score =
alignment_score / ln(query_length)`. For a 250bp read, `ln(250) ≈ 5.52`, so
threshold 20 requires a minimum raw alignment score of ~110. With Bowtie2's match
bonus of +2, that's ~55 matched bases -- only 22% of the read. It's quite permissive.

**Impact of relaxed gap penalties:** Changing from `--rdg 5,3` to `--rdg 3,1` saves
~4 raw score per homopolymer indel. For a read with 5 indels: +20 raw → +3.6
normalized. The distribution shifts up modestly. The default threshold of 20 remains
appropriate.

**Recommendation: start with the default (20) for both variants.** Only lower if the
tuning procedure below reveals missed hits.

**Tuning procedure (run after initial results):**

1. Extract score distributions from Ultima and matched Illumina results:
   ```bash
   # Ultima scores
   zcat ultima_virus_hits.tsv.gz | \
     cut -f5 | tail -n+2 | sort -g > ultima_scores.txt
   # Illumina scores (same samples)
   zcat illumina_virus_hits.tsv.gz | \
     cut -f5 | tail -n+2 | sort -g > illumina_scores.txt
   ```
   Column 5 is `aligner_length_normalized_score_mean`. Plot both distributions.

2. Compare viral hit counts per sample between Ultima and Illumina. If Ultima finds
   substantially fewer viruses, check whether hits are clustering just below threshold:
   ```bash
   # Look at the FILTER_VIRAL_SAM output (pre-threshold) vs final output
   # to see how many reads are being filtered at each score level
   ```

3. If lowering the threshold, **inspect the reads that newly pass**. In
   `virus_hits.tsv.gz`, check `prim_align_edit_distance` and `query_len` for
   newly-passing reads -- are they real viral alignments with homopolymer indels
   (edit_distance ~5-15 for a 250bp read) or junk partial matches (edit_distance
   close to aligned length)?

4. A reasonable range is 15-20. Below 15 risks junk; above 20 is unnecessarily
   strict given the permissive score-min function already applied by Bowtie2.

### Summary of edits

| File | Lines changed | What |
|------|:---:|------|
| `subworkflows/local/extractViralReadsShort/main.nf` | 3 | Add `--rdg 3,1 --rfg 3,1 -X 1000` to par_strings |
| `workflows/run.nf` | 2 | Change k=21, add hdist=1 |
| `modules/local/bbduk/main.nf` | 1 | Add `hdist=${params_map.hdist}` to command |
| `modules/local/fastp/main.nf` | 1 | Remove `--correction --detect_adapter_for_pe`, lower Q to 15 |
| **Config only (no code)** | 0 | `--bt2_score_threshold 15` |
| **Total** | **7 lines** | |

Note: The FASTP edit (removing `--correction` and `--detect_adapter_for_pe`) is also
needed for Variant A -- it's a correctness fix, not just tuning. The quality threshold
change (Q20→Q15) is Variant B only.

All edits are parameter value changes on existing lines (plus one new parameter
passthrough). No new modules, no new subworkflows, no conditional logic, no new
processes.

### Important: make these changes on a branch

These parameter changes affect the Illumina pipeline too. Make them on the
katherine-exploratory-ultima branch (or a sub-branch), NOT on dev. The changes
are Ultima-specific and shouldn't be merged.

---

## Compute Cost

The simulated PE approach doubles the number of read-ends entering alignment:

| Step | Reads processed | vs. single-end |
|------|-----------------|:---------:|
| BBDUK kmer screen | 2 × 333M = 666M read-ends | 2x |
| FASTP | ~666M read-ends (interleaved) | 2x |
| Bowtie2 viral | ~6M read-ends (post-BBDuk, paired) | ~1.5x (paired scoring slightly faster than 2× SE) |
| Bowtie2 human/other | ~6M read-ends each | ~1.5x |
| FILTER_VIRAL_SAM | <100K read-ends | ~1x (negligible) |
| Kraken (post-BBMERGE) | ~1M reads (merged back to original) | 1x (identical) |

The headline cost is ~2x for BBDuk and FASTP (the high-throughput steps). Post-BBDuk,
the ~99% reduction makes the doubling irrelevant in absolute terms. Overall wall-clock
increase: roughly 1.5-1.8x vs. a hypothetical single-end run.

**Recommendation**: Run 2-3 samples initially (not all 30). Subsample to 10-50M reads
if you want results in hours rather than overnight. The goal is a quick sanity check.

---

## What the Results Tell You

### Meaningful
- **Viral hit identity**: Which viruses are detected. The Bowtie2 alignments are real;
  the simulated PE doesn't create false positives or false negatives. Compare directly
  to matched Illumina results for the same samples.
- **Taxonomy profiles (Kraken)**: BBMERGE reconstructs the original read, so Kraken
  sees full-length single-end data. Results are essentially identical to running Kraken
  on the original reads.
- **QC metrics**: Read quality, length distribution, adapter content (via FASTP JSON
  and FastQC). These reflect the real Ultima data.
- **Depletion rates**: Fraction of reads mapping to human/contaminant. Reflects real
  data characteristics.

### Meaningless / artifactual (see Output Cleanup section for how to strip these)
- **Fragment length**: Always equals read length. Not a real insert size.
- **Pair status / concordance**: Always CP. Not informative.
- **BBMERGE merge rate**: Always ~100%. Not informative.
- **Insert size histogram** (in fastp.json): Shows read length, not library insert.
- **All `_rev` columns** in virus_hits: Redundant copies of forward columns.
- **`n_reads_single`** in read_counts/QC: 2x the actual Ultima read count.

### Approximate (useful directionally, not precisely)
- **Viral mapping rate**: Real, but ~5-15% lower than you'd get with minimap2 due to
  Bowtie2's indel handling (even with Variant B's relaxed gap penalties). The middle
  path would recover these reads.
- **Alignment scores**: Real scores, but the distribution differs from true Illumina
  paired-end due to different error profiles. See score threshold tuning procedure in
  Variant B section.

---

## Output Cleanup for Sharing

The pipeline produces 15 output files per sample. Here's how each is affected by the
simulated PE hack, and what (if anything) to clean before sharing with partners.

### Files that need no cleanup

| File | Why it's fine |
|------|--------------|
| `*_kraken.tsv.gz` | No PE-specific columns. Counts come post-BBMERGE (which reconstructs the original read), so values are correct. |
| `*_bracken.tsv.gz` | Same -- abundance estimates based on correct Kraken counts. |
| `*_qc_quality_base_stats_{raw,cleaned}.tsv.gz` | Per-position quality stats. FastQC processes the interleaved FASTQ; quality scores are real Ultima data. |
| `*_qc_quality_sequence_stats_{raw,cleaned}.tsv.gz` | Per-sequence mean quality distribution. Real data. |
| `*_qc_length_stats_{raw,cleaned}.tsv.gz` | Read length distribution. Real data. |
| `*_qc_adapter_stats_{raw,cleaned}.tsv.gz` | Adapter content per position. Real data. |

### Files worth cleaning (for partner-facing output)

**1. `virus_hits.tsv.gz`** -- the most important file to clean.

29 columns, of which 9 are PE artifacts. With simulated PE, every `_rev` column is a
redundant copy of the corresponding forward column (same alignment, same score, same
position). `prim_align_fragment_length` = read length (not a real insert size).
`prim_align_pair_status` = always "CP".

Drop these columns before sharing:

```bash
cols_to_drop="query_len_rev,query_seq_rev,query_qual_rev,prim_align_fragment_length,prim_align_best_alignment_score_rev,prim_align_edit_distance_rev,prim_align_ref_start_rev,prim_align_query_rc_rev,prim_align_pair_status"

zcat virus_hits.tsv.gz | python3 -c "
import sys, csv
drop = set('${cols_to_drop}'.split(','))
reader = csv.DictReader(sys.stdin, delimiter='\t')
keep = [c for c in reader.fieldnames if c not in drop]
writer = csv.DictWriter(sys.stdout, fieldnames=keep, delimiter='\t')
writer.writeheader()
for row in reader:
    writer.writerow({k: row[k] for k in keep})
" | gzip > virus_hits_clean.tsv.gz
```

The cleaned file has 20 columns and looks indistinguishable from a native single-end
pipeline output (similar to what the ONT path produces).

**2. `read_counts.tsv`** -- minor but confusing.

```
sample    n_reads_single    n_read_pairs
sample_01 666000000         333000000
```

`n_reads_single` is 2x the actual Ultima read count (because each real read becomes
a pair = 2 read-ends). `n_read_pairs` equals the actual Ultima read count. Fix:

```bash
# Replace n_reads_single with actual count, drop n_read_pairs
awk -F'\t' 'NR==1{print "sample\tn_reads"} NR>1{print $1"\t"$3}' \
  read_counts.tsv > read_counts_clean.tsv
```

**3. `fastp.json`** -- low priority, mainly for internal use.

PE-specific artifacts: `summary.sequencing` says "paired end", `insert_size` histogram
shows insert = read length (not wrong, just not useful), `read2_*` sections mirror
`read1_*`. The QC metrics (quality, adapter, poly-X, complexity filtering) are all
real. If sharing, just note that insert_size reflects read length, not library insert.

**4. `qc_basic_stats_{raw,cleaned}.tsv.gz`** -- same doubling issue as read_counts.

`n_reads_single` is 2x actual; `n_read_pairs` is the real count. Same one-liner fix
as read_counts if sharing.

### Recommended cleanup workflow

For a quick share: just clean `virus_hits.tsv.gz` and `read_counts.tsv`. These are
the files partners actually look at. Everything else is either unaffected (Kraken,
Bracken) or internal QC that's close enough to correct.

```bash
# One-shot cleanup for a sample's results directory
for f in *_virus_hits.tsv.gz; do
  # Drop PE artifact columns
  zcat "$f" | python3 -c "..." | gzip > "${f%.tsv.gz}_clean.tsv.gz"
done
for f in *_read_counts.tsv; do
  awk -F'\t' 'NR==1{print \"sample\tn_reads\"} NR>1{print \$1\"\t\"\$3}' "$f" > "${f%.tsv}_clean.tsv"
done
```

---

## Relationship to Plan: Illumina Path

**For R&D purposes, this approach strictly dominates plan_illumina_path.** Here's why:

Plan_illumina_path proposes medium effort to make the Illumina pipeline accept
single-end data:
1. Single-end BBDuk variant (replace BBDUK_HITS_INTERLEAVE)
2. FASTP single-end mode (interleaved=false)
3. Bowtie2 single-end mode (remove --interleaved, unmapped_flag=4)
4. **FILTER_VIRAL_SAM --single-end rework** (skip mate creation, pair grouping, YS:i
   assertions -- the hardest and riskiest change)
5. PROCESS_VIRAL_BOWTIE2_SAM paired=false
6. Unpaired column lists throughout
7. New EXTRACT_VIRAL_READS_ULTIMA subworkflow

All of that plumbing exists to solve one problem: making the paired-end pipeline accept
single-end input. Simulated PE eliminates the problem entirely. The same Bowtie2 aligner
runs with the same (or better, with Variant B) parameters. The same FILTER_VIRAL_SAM
processes the same reads. The science is identical.

| Dimension | Simulated PE (Variant B) | Illumina path |
|-----------|:-:|:-:|
| Aligner | Bowtie2 (same) | Bowtie2 (same) |
| Gap penalties | Tunable (same) | Tunable (same) |
| BBDuk sensitivity | Tunable (same) | Tunable (same) |
| Viral sensitivity | Identical | Identical |
| Comparability to Illumina | Same | Same |
| Lines changed | 7 parameter values | ~200+ (new subworkflow + FILTER_VIRAL_SAM rework) |
| FILTER_VIRAL_SAM risk | None (works as-is) | Highest-risk item in any plan |
| Compute cost | ~1.5-1.8x | 1x |
| Time to implement | Hours | Days |

The only advantage of plan_illumina_path is ~1x compute instead of ~1.5-1.8x. For a
pilot analysis on 2-3 samples (or even all 30), the extra compute cost is negligible
compared to the engineering time saved -- especially given that FILTER_VIRAL_SAM rework
is the highest-risk item across ALL plans and is completely avoided here.

**Recommendation**: Drop plan_illumina_path. If you want Bowtie2-based results, use
simulated PE. If you want production-quality Ultima support, build the middle path.
The Illumina path's plumbing work is wasted effort that produces identical scientific
results to simulated PE.

---

## Relationship to Other Plans

This approach is **complementary** to the middle path and ONT path, not a replacement:

- **Simulated PE** (this plan): Day-1 smoke test. Zero/minimal code changes. Confirms
  the data is usable and provides preliminary results for comparison. Runs while you
  build the middle path.
- **Middle path**: The recommended production approach. Better viral sensitivity
  (minimap2), cleaner single-end handling, no wasted compute. Build this after the
  smoke test validates the data.
- **ONT path**: Alternative production approach if you want all-minimap2 consistency.
  Higher index cost. Still a valid option.
- **Illumina path**: Superseded by simulated PE for R&D. Would only be relevant if
  you needed production single-end Bowtie2 support (unlikely -- the middle path is
  better for production).

### Suggested timeline
1. **Day 1**: Generate R2 files for 2-3 samples. Run Variant A (zero changes) as
   immediate smoke test.
2. **Day 2**: Review smoke test results. If data looks good, apply Variant B parameter
   tweaks and re-run. Start building the middle path in parallel.
3. **Week 1-2**: Middle path implementation. Compare middle path results to Variant B
   results to quantify minimap2 vs Bowtie2 sensitivity difference for Ultima.

---

## Pros
- **Near-zero code changes** -- Variant A needs 4 line edits (FASTP + `-X 1000`);
  Variant B needs 7 more parameter-value edits on top
- **No new modules, subworkflows, or indexes**
- **No FILTER_VIRAL_SAM rework** -- the hardest item in plan_illumina_path is avoided
- **Maximum comparability** with existing Illumina results (same tools, same code path)
- **Fast to implement** -- data prep is one `seqtk` command per sample
- **Taxonomy results identical** to true single-end (BBMERGE reconstructs original read)
- **Strictly dominates plan_illumina_path** for R&D

## Cons
- **~1.5-1.8x compute cost** from doubled read-ends (modest in absolute terms,
  especially post-BBDuk where data volume is tiny)
- **Artifactual columns in output**: 9 of 29 virus_hits columns are PE artifacts
  (redundant `_rev` columns, fake fragment_length, always-CP pair_status). Needs a
  simple post-processing cleanup before sharing (see Output Cleanup section).
- **Not production-quality** -- a hack for pilot analysis, not something to merge to dev
- **Bowtie2 still suboptimal for Ultima indels** -- even with relaxed gap penalties,
  expect ~5-15% lower viral sensitivity than minimap2 (middle path)
- **Doubled FASTQ storage** (~300GB-1.2TB additional S3 for 30 samples)

## Estimated Effort
- **Variant A**: Trivial. 4 line edits (FASTP cleanup + `-X 1000`), one `seqtk` command
  per sample, paired-end samplesheet.
- **Variant B**: Small. ~11 total line edits + data prep. A few hours total.
