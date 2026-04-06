# DOWNSTREAM Workflow Plan for Simulated PE Ultima Data

## Strategy

Run simulated PE Ultima data through the standard Illumina DOWNSTREAM path
with zero code changes. This maximizes comparability with matched Illumina
samples and is well-suited to the primary goal: determining what fraction of
Ultima viral hits validate with BLAST and enabling an apples-to-apples
comparison of Illumina and Ultima data.

---

## What DOWNSTREAM Does (Recap)

1. **Load & concatenate** RUN outputs by sample group
2. **Mark duplicates** (short-read only) — marks but does NOT remove
3. **Count reads per clade** (short-read only) — uses only non-duplicate exemplars
4. **Validate viral assignments** — VSEARCH clustering → BLAST top cluster
   representatives against core_nt → LCA comparison → propagate validation
   to all hits

---

## Step-by-Step Assessment for Simulated PE Ultima Data

### Duplicate Marking: Imperfect but Harmless for Primary Goal

The duplicate marker identifies duplicates by alignment coordinate proximity
(within 1 nt deviation). With simulated PE, both mates map to the same
position, so duplicate detection is based on R1 start alone — there's no
independent R2 coordinate providing fragment-boundary information.

For Illumina, same-start + same-end = same fragment = likely PCR duplicate.
For simulated PE, same-start could be different-length Ultima reads from
different molecules, so you'll get some false-positive duplicate marks.

**But**: Duplicates are only *marked*, not removed. The marks affect
`COUNT_READS_PER_CLADE` (which skips duplicates) but do NOT affect
`VALIDATE_VIRAL_ASSIGNMENTS`. Since the primary goal is BLAST validation
rate, this is fine.

### VSEARCH Clustering (identity=0.95): Fine As-Is

Before clustering, BBMERGE merges the simulated PE pairs back into the
original Ultima reads (100% overlap → perfect merge). VSEARCH sees the real
single-end sequences.

Ultima's ~0.5-1% error rate means same-template reads are typically >98%
identical. The 95% threshold provides ample room. No adjustment needed.

`cluster_min_len=15` is fine (Ultima reads are ~150-500bp).

### BLAST Validation (perc_id=60%, qcov=30%): Fine As-Is

These thresholds are very permissive. BLAST handles indels well (unlike
Bowtie2), so Ultima's homopolymer errors won't meaningfully affect BLAST
results.

The top-20-clusters-per-species sampling, bitscore filtering (top 10, ≥90%
of best), and LCA assignment are all sequence-content-driven and
platform-agnostic.

---

## Options Considered

### Option A (Recommended): Standard Illumina DOWNSTREAM, Zero Changes

- Set `platform = "illumina"` (or whatever was used for RUN).
- Duplicate marks will be slightly noisy, but don't affect validation.
- Clade counts will be slightly deflated (some non-duplicates falsely
  marked), but this is secondary to the primary goal.
- VSEARCH and BLAST parameters are appropriate.
- **Key advantage: identical processing to the Illumina comparison samples.**
  This is the only option that gives a true apples-to-apples comparison of
  BLAST validation rates.

### Option B: ONT DOWNSTREAM Path

- Set `platform = "ont"`.
- Skips duplicate marking and clade counting entirely.
- Uses more permissive BLAST settings (0% identity, 0% coverage) and sends
  ALL sequences to BLAST (1M clusters instead of 20).
- **Problem**: The Illumina comparison data was processed with the short-read
  path (95% VSEARCH, 60%/30% BLAST, top 20 clusters). Using ONT settings for
  Ultima makes the validation rates non-comparable.
- **Problem**: Sending all sequences to BLAST instead of top-20
  representatives is much more expensive and produces different results.

### Option C: Illumina Path with Parameter Tweaks

- Could theoretically optimize for Ultima (e.g., lower VSEARCH identity,
  adjust BLAST thresholds).
- The current parameters are already well within tolerance for Ultima's error
  profile, so tweaks are unnecessary.
- **Problem**: Any parameter difference between Ultima and Illumina runs
  undermines the apples-to-apples comparison.

---

## Recommendation

**Go with Option A — standard Illumina DOWNSTREAM, zero changes.**

Rationale:

1. **Primary goal is BLAST validation rate comparison.** Identical processing
   is essential. Any parameter difference introduces a confound.
2. **The parameters are already appropriate.** VSEARCH at 95% and BLAST at
   60%/30% are permissive enough that Ultima's error profile won't be the
   limiting factor.
3. **Duplicate marking is the one imperfect step**, but it doesn't affect
   validation — only clade counts, which are secondary. This caveat should
   be noted when reporting clade counts.
4. **BBMERGE perfectly reconstructs the original reads** before VSEARCH, so
   clustering operates on the real Ultima sequences.

---

## Caveats to Note When Reporting Results

- **Duplicate marking**: With simulated PE, duplicate detection uses only the
  R1 alignment start (no independent R2 coordinate). Different-length reads
  starting at the same position may be falsely marked as duplicates. This
  affects clade counts but not BLAST validation.

- **What "validation rate" measures**: Validation is done on cluster
  *representatives* (top 20 per species), not all reads. The "fraction that
  validates" is really "fraction of top-20 cluster representatives that
  BLAST-validate," propagated back to all cluster members. This is the same
  for both platforms, so it's still apples-to-apples — just worth being
  precise about what's being measured.

- **Clade counts**: May be slightly deflated relative to true counts due to
  false-positive duplicate marking. The Illumina comparison won't have this
  issue (real PE data gives accurate duplicate detection), so clade count
  comparisons should be interpreted with this asymmetry in mind.
