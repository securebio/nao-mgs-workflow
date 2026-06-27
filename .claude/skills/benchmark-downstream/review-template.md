# DOWNSTREAM release comparison report

- **Candidate (dev) run:** `s3://path/to/dev/downstream/output`
- **Reference (main) run:** `s3://path/to/main/downstream/output`
- **Dev index:** `s3://nao-mgs-index/<DATE>` · **Main index:** `s3://nao-mgs-index/<DATE>`
- **Code versions:** main `<ver>` · dev `<ver>`
- **Report timestamp:** YYYY-MM-DD HH:MM

> **Holistic release diff — no causal attribution, no verdict.** main and dev
> typically differ in code AND reference index AND QC parameters at once, so a
> difference here cannot be attributed to any one cause, and (absent ground
> truth) is neither good nor bad on its face. Differences are flagged for a
> human to adjudicate; where a likely driver is obvious, it is named as a
> hypothesis only.

---

## Summary

- Bullet list of the most important findings: each ≤2 lines with at most one
  representative number; leave the supporting detail to the numbered findings.
- State the comparison scope: N groups (X Illumina + Y ONT), matched by name.
- **Flags:** one line per flagged finding category with counts (mirrors §Flags).
- Don't link out to file paths; the report must stand alone (embed the numbers).
- Keep language neutral: describe differences, never assert causation or a
  good/bad verdict. Name a likely driver only with an explicit "(hypothesis)".

---

## Findings

### 0. Completeness and schema

- Groups present on both sides / missing on either (from the file inventory).
- Any file type missing for a group, or row-count anomalies worth noting.
- Column conformance: any output whose columns differ from its schema or across
  sides (added/removed/reordered columns); note empty outputs (e.g. bracken).

### 1. Viral assignments

Read-level comparison joined on `(group, sample, seq_id)`; the canonical
assignment is the pipeline call `aligner_taxid_lca`. The vertebrate-viral subset
= reads whose assigned taxid is `infection_status_vertebrate == 1` in the **dev**
index (species rollup; status 3 "likely" excluded — note as a documented choice).

> **Status-3 caveat (state this):** the subset is status-1 ("affirmatively
> infecting") only; status-3 ("likely-infecting") reads are excluded, so a
> regression confined to status-3 taxa would not trip the vertebrate flags. Note
> the status-3 read share if material.

> **Caveat (state this):** counts are per-read (PCR duplicates included), and
> taxids are compared as-is. The index workflow does not currently publish
> `taxonomy-merged.dmp`, so taxid canonicalization is presently always skipped;
> in principle a merged/renumbered taxid could show as a spurious reassignment,
> but do NOT invoke this as the driver for `same-species` reassignments — a
> same-species move (especially a child<->parent pair sharing a species, which
> you can check) is a genuine LCA-specificity change, not a versioning artifact.
> Use the dedup view and the concentration table (below) to qualify the headline
> %. Treat the dedup view as a rough cross-check, not a reliable metric: exemplar
> identity is chosen independently per run, so its lost/gained columns — and, via
> a shifting shared-read denominator, even its reassignment rate — can move from
> exemplar reshuffling rather than real change.

#### 1.1. Lost / gained / reassigned reads (vertebrate-viral subset)

Note the **different denominators**: % lost = lost/main, % gained = gained/dev,
% reassigned = reassigned/shared.

| Group | Platform | main | dev | shared | reassigned | lost | gained | % lost | % gained | % reassigned |
|---|---|---|---|---|---|---|---|---|---|---|
| ... | | | | | | | | | | |

- Findings: groups with notable loss/gain/reassignment; flag large ones.
- For any flagged group, give its concentration (distinct taxid pairs and the
  top pair's share of reassigned reads) so a high % driven by one systematic
  taxid remap is not mistaken for broad instability; and note whether the dedup
  (exemplar) view changes the picture.

#### 1.2. Reassignment severity (how different taxonomically)

| Divergence bucket | Reassigned reads |
|---|---|
| same-species | NNN |
| same-genus | NNN |
| same-family | NNN |
| ... | |
| shared-higher-taxon | NNN |
| cross-root | NNN |
| unresolved-taxid (versioning artifact — not a severity level) | NNN |

- Edge-distance distribution (mean/median/max). Call out cross-root or
  shared-higher-taxon reassignments (a viral read no longer placed within a
  specific viral clade) with example taxid pairs.
- Report `unresolved-taxid` counts separately — these are taxids absent from the
  dev taxonomy (merged/deleted across index versions), not a biological
  reassignment; do not rank them as the most severe bucket.

#### 1.3. Clade-count high-level breakdown (Illumina only)

- Per-family (and per-order) share of viral reads, main vs dev, flagging large
  share shifts (report both reads_clade_total and reads_clade_dedup).
- Call out whole families that appear/disappear across many groups.
- Shares are normalized within the rank (each family's share of family-classified
  reads), so when one family drops to 0 the others' shares mechanically rise —
  read share *increases* as partly renormalization, not necessarily real growth.
  Rank is resolved from the full dev taxonomy (with the old index as a fallback
  for taxids deleted from it) and names from both indexes' annotations; note that
  rank classification uses the dev taxonomy, so a "disappearance" can be a
  re-ranking artifact.

#### 1.4. BLAST validation agreement (secondary)

- Per-group fraction of reads validated and the agreement rate, main vs dev,
  reported together (a rate change on a shifting validated subset is ambiguous).
  "Agreement" = `validation_distance_aligner == 0`, i.e. the aligner call is an
  ancestor of or equal to the BLAST call (not necessarily identical). BLAST runs
  on cluster representatives and is propagated to reads; treat as secondary.

#### 1.5. Vertebrate-status flips between indexes (side table)

- Count of taxa that gained / lost `infection_status_vertebrate == 1` between
  the main and dev index annotations; a driver of category shifts above.

### 2. Kraken abundances

Per group, split by ribosomal read set (TRUE/FALSE); Bray-Curtis dissimilarity
(0 = identical, 1 = disjoint; for relative-abundance vectors that each sum to 1
it equals the total variation distance) at genus and species rank, plus top
movers. Abundances are pooled across a group's samples (depth-weighted) and
computed on subsampled reads, so this is a whole-community sanity check
dominated by abundant (mostly non-viral) taxa, not a viral-signal detector.

| Group | Ribosomal | Rank | Bray-Curtis | n taxa (union) |
|---|---|---|---|---|

- Findings: groups/ranks with high dissimilarity; top taxa moving up/down.

### 3. Quality metrics

Per group/sample: the raw->cleaned read-survival fraction compared across runs
(in percentage points — this reflects a QC/screen change, unlike a cross-run
change in the absolute cleaned count); plus mean_seq_len, percent_gc,
percent_duplicates, n_bases deltas and FASTQC flag transitions.

- Findings: notable survival or QC-metric shifts, any FASTQC flag changes.

### 4. Output file overview (schema-driven)

- Cursory, generic pass over every output file type: per-group presence and
  row-count deltas, derived from schemas + pyproject expected-outputs (no
  per-file logic, so it tracks output changes automatically).
- Note the largest row-count deltas per file type and any structural surprises.

---

## Flags

Consolidated flags for human review (fixed thresholds and/or cohort-outlier).
Group by focus; give the key, value, threshold, and flag type. State the
thresholds used.

When a category mixes flag types, report the per-type counts (from the
`flag_type` column of `flags.tsv`) and do NOT describe `cohort-outlier` flags
with the fixed-threshold comparator — e.g. write "106 clade flags (41
fixed+cohort-outlier >3pp, 65 cohort-outlier <3pp)", never "106 clade flags
(>3pp)". A cohort-outlier can be below the fixed threshold by design.

| Focus | Key | Metric | Value | Threshold | Flag type |
|---|---|---|---|---|---|

---

## Recommendations

Derive the list **deterministically** from the data so independent reviewers
produce the same set. Emit exactly one bullet for each condition below that
holds, in this order, and no others (omit a bullet whose condition is false):

1. Each whole-clade collapse/appearance — one bullet per family/order that goes
   to (or from) ~0 share across many groups (e.g. Picobirnaviridae). (high)
2. The single highest reassignment-rate group, if it is a clear outlier above
   the rest. (high)
3. All remaining groups over the reassignment threshold, as ONE bullet. (medium)
4. Groups over the lost threshold, as ONE bullet — only if any exist. (medium)
5. Groups over the gained threshold (a high gained *fraction*: gained/n_dev, so
   high turnover can trip it even without total-count growth), as ONE bullet —
   only if any exist. (high)
6. Vertebrate-status flips between indexes, as ONE bullet. (medium)
7. BLAST agreement-rate drops over threshold, as ONE bullet — only if any. (medium)
8. Kraken Bray-Curtis flags over threshold, as ONE bullet — only if any. (medium)
9. Any cross-root or shared-higher-taxon reassignments, as ONE bullet — only if
   the count is > 0. (low)
10. Empty outputs (e.g. bracken), as ONE bullet — only if any are empty. (low)
11. A QC note, ALWAYS as exactly ONE bullet (low): state the QC result either
    way — "QC stable: survival Δ within threshold, no FASTQC flag changes" when
    nothing material changed, or the specific shift otherwise. (Always emit this
    bullet so the recommendation count is deterministic across reviewers.)

Each bullet: a one-line argument referencing the findings above, with a concern
level (high | medium | low). No verdict — concern reflects magnitude/breadth only.
Concern levels are FIXED per condition above; do not re-level. Use neutral
wording: "confirm whether X reflects a real change or an artifact", never verdict
words like "over-calling", "legitimate", or "directly caused by"; keep any
driver link explicitly hypothetical ("temporally/structurally consistent with").

---

## Appendix

### A.1. Table subject

| header | header |
|---|---|
