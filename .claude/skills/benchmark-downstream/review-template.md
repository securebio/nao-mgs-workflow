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

- Concise bullet list of the most important findings (one short sentence each).
- State the comparison scope: N groups (X Illumina + Y ONT), matched by name.
- **Flags:** one line per flagged finding category with counts (mirrors §Flags).
- Don't link out to file paths; the report must stand alone (embed the numbers).

---

## Findings

### 0. Completeness and schema

- Groups present on both sides / missing on either (from the file inventory).
- Any file type missing for a group, or row-count anomalies worth noting.
- Column conformance: any output whose columns differ from its schema or across
  sides (added/removed/reordered columns); note empty outputs (e.g. bracken).

### 1. Viral assignments

Read-level comparison on `(group, seq_id)`; the canonical assignment is the
pipeline call `aligner_taxid_lca`. The vertebrate-viral subset = reads whose
assigned taxid is `infection_status_vertebrate == 1` in the **dev** index
(species rollup; status 3 "likely" excluded — note as a documented choice).

#### 1.1. Lost / gained / reassigned reads (vertebrate-viral subset)

| Group | Platform | main reads | shared | reassigned | lost | gained | % lost | % reassigned |
|---|---|---|---|---|---|---|---|---|
| ... | | | | | | | | |

- Findings: groups with notable loss/gain/reassignment; flag large ones.

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

#### 1.4. BLAST validation agreement (secondary)

- Per-group fraction of reads validated and the agreement rate (distance 0),
  main vs dev. Note large agreement-rate shifts. (BLAST runs on cluster
  representatives; treat as a secondary signal.)

#### 1.5. Vertebrate-status flips between indexes (side table)

- Count of taxa that gained / lost `infection_status_vertebrate == 1` between
  the main and dev index annotations; a driver of category shifts above.

### 2. Kraken abundances

Per group, split by ribosomal read set (TRUE/FALSE); Bray-Curtis dissimilarity
(= total variation distance) at genus and species rank, plus top movers.

| Group | Ribosomal | Rank | Bray-Curtis | n taxa (union) |
|---|---|---|---|---|

- Findings: groups/ranks with high dissimilarity; top taxa moving up/down.

### 3. Quality metrics

Per group/sample at raw and cleaned stages: read survival, mean_seq_len,
percent_gc, percent_duplicates, n_bases; plus FASTQC flag transitions.

- Findings: notable QC shifts (e.g. cleaned read survival), any FASTQC flag
  changes.

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
5. Large `gained`-read groups (a group whose dev read count far exceeds main),
   as ONE bullet — only if any exist. (high)
6. Vertebrate-status flips between indexes, as ONE bullet. (medium)
7. BLAST agreement-rate drops over threshold, as ONE bullet — only if any. (medium)
8. Kraken Bray-Curtis flags over threshold, as ONE bullet — only if any. (medium)
9. Any cross-root or shared-higher-taxon reassignments, as ONE bullet — only if
   the count is > 0. (low)
10. Empty outputs (e.g. bracken), as ONE bullet — only if any are empty. (low)
11. A QC note, as ONE bullet — only if a QC/screen parameter changed but is not
    visible in the aggregate QC metrics. (low)

Each bullet: a one-line argument referencing the findings above, with a concern
level (high | medium | low). No verdict — concern reflects magnitude/breadth only.

---

## Appendix

### A.1. Table subject

| header | header |
|---|---|
