# DOWNSTREAM comparison report

This report compares the DOWNSTREAM output of two pipeline runs — a candidate run
against a reference run, each a **(pipeline version, reference index) pair** — to
help a human decide whether the candidate is safe to adopt. With no ground truth
to compare against, a difference is neither good nor bad on its face. **This
report flags differences as questions for a human, never as verdicts**, and names
a likely driver only as a hypothesis.

> **Author instructions.** Print the one attribution statement that matches what
> differs between these two runs (see SKILL.md "Attribution"); delete the others.

`<Attribution statement for THIS comparison — one of: "These runs differ in
<list ≥2 of code / reference index / QC parameters>, so a difference here cannot
be attributed to a single cause." OR "These runs differ only in <the one
dimension>, so differences are attributable to that change.">`

> **How to fill this in (instructions for the report author; delete this block
> and every `> **Author instructions**` block in the final REVIEW.md).** Replace
> every `<placeholder>`. Every number must trace to a TSV produced by
> `bin/compare_downstream_runs.py` — never fabricate or estimate. **Report what
> this comparison's data actually shows** — the examples and illustrative shapes
> throughout this template are format guides, not expected results; do not carry
> their specific taxa, groups, directions, or magnitudes into your report, and do
> not assume a finding exists until the TSVs show it. **Missing-data rule:** if an
> input needed for a metric is absent (e.g. no `--index` → viral analysis skipped;
> empty bracken), say so plainly in that spot and move on; do not invent values.
> **Follow the report-writing principles in SKILL.md** (name every taxon; plain
> language; no tool-internal jargon) throughout — they apply to every section.

## Run identity

| | Candidate (`--dev`) | Reference (`--main`) |
|---|---|---|
| DOWNSTREAM output | `s3://path/to/candidate/...` | `s3://path/to/reference/...` |
| Index | `s3://nao-mgs-index/<DATE>` | `s3://nao-mgs-index/<DATE>` |
| Pipeline version | `<ver, if known>` | `<ver, if known>` |

- **What differs between the runs:** `<list which of {pipeline code, reference
  index, QC parameters} actually differ — this sets how far any difference can be
  attributed (see the intro). If only one differs, say so.>`
- **Comparison scope:** `<N>` groups (`<X>` Illumina + `<Y>` ONT), matched by
  name. `<note any group or file type missing on one side; if none, say so>`.
- **Report generated:** `<YYYY-MM-DD HH:MM>`

---

## Summary

> **Author instructions.** Keep this short — it is an at-a-glance orientation,
> NOT a recap of every finding (Main findings carries the full set). One sentence
> of comparison scope, then a numbered list of only the **2–3 broadest or largest
> differences** this comparison surfaced (the ones a release reviewer would want
> first), ranked by breadth across groups/platforms × magnitude. One representative
> number each. Do not enumerate the rest, do not list the stable dimensions here,
> and do not give per-category flag counts (those live in the Flags appendix).
> Keep it neutral — describe differences, never assert a cause or verdict. If
> nothing crossed a threshold, say so in one line and stop.

Compared `<N>` groups (`<X>` Illumina + `<Y>` ONT). The differences that stand
out:

1. `<broadest/largest difference — what changed, with one number>`
2. `<next>`
3. `<next, if warranted>`

---

## Main findings

> **Author instructions.** Write one `###` subsection per metric dimension that
> showed a *substantial* difference in THIS comparison — a consolidated flag, or
> a difference large enough that a human should see it even if it didn't trip a
> threshold. **Coverage is deterministic: every dimension that produced a flag
> MUST get a subsection here** (don't drop one as "minor" — that is what makes two
> independent reviewers surface the same set). Dimensions that were checked but
> stayed within threshold go in "Checked, no action needed" below, not here. The
> candidate dimensions are listed below so no class of finding is missed; **give a
> dimension a subsection only if its data shows something**, and delete the rest.
> Don't presume any of them happened — let the TSVs decide. **Title each
> subsection after what the data actually shows**, not after the dimension's
> name: a neutral, factual headline (e.g. for the clade dimension, "Family <Name>
> drops to zero share in N Illumina groups" if that is what occurred — not the
> generic label). Lead each with a one-sentence statement of *what changed*, then
> 2–4 plain sentences of supporting numbers; annotate the breadth and magnitude
> (how many groups and platforms, how far past threshold) rather than a fixed
> high/medium/low verdict — that is the human's prioritization signal. Where a
> difference is bidirectional or mixed (some groups up, some down), say so rather
> than forcing a single direction. Put method and statistical caveats in the
> matching Detailed-investigation section and the Methodology appendix, not here.
> Do not assert causes; if a Likely-drivers investigation was done, point to it
> rather than restating it.
>
> **End each subsection with a `**To confirm:**` line** — one or two plain
> sentences asking the human to confirm whether the change reflects a real change
> or an artifact, naming the taxa/groups, keeping any driver link explicitly
> hypothetical ("consistent with", "hypothesis only") and using no verdict words
> ("over-calling", "legitimate", "wrong", "caused by").
>
> Candidate dimensions (each maps to a Detailed-investigation section; cross-
> reference it). For each, the kind of thing to report if present:
>
> - **Read-level viral assignment changes** — lost / gained / reassigned reads in
>   the vertebrate-viral subset: which groups cross a threshold, the rate range,
>   the highest group, and (for reassignment) whether the moves concentrate in a
>   few recurring taxid pairs (name them, give the top-pair share) or are broad.
>   Split group counts by platform (in the form "<n> Illumina + <m> ONT"); the
>   flag key does not carry platform, so do not assume every flagged group is
>   Illumina. Note the denominators (% lost ÷ main, % gained ÷ dev, % reassigned ÷
>   shared) and, for gains, that the metric is a fraction of the dev total so
>   turnover can trip it without net growth. State what is and is NOT known: the
>   reads differ on the matched key; whether real or an index/annotation effect is
>   not established here.
> - **Reassignment taxonomic severity** — how far reassigned reads moved: any
>   cross-root or shared-higher-taxon moves (viral reads no longer within a
>   specific viral clade), with named example pairs. Report `unresolved-taxid`
>   separately — it is a versioning artifact, not a severity level.
> - **Clade-share shifts (family / order, Illumina)** — clades whose share of
>   total viral reads moved materially, including any family/order that drops to
>   zero dev share or newly appears; give taxids and the largest per-group share
>   moves in percentage points. State two distinct counts without conflating them:
>   how many groups *flagged* (share change past the threshold), and separately in
>   how many groups the clade reached zero dev share (a collapse can occur below
>   the flag threshold, and a flagged move need not be a collapse). Before calling
>   a share drop a collapse, check the raw read counts (`reads_main`/`reads_dev`,
>   i.e. `delta_reads`): a clade's share can fall purely because the group's total
>   viral reads grew, so confirm the clade's own reads actually dropped. State the
>   alternatives (a real change vs. a reference/classification effect) as
>   alternatives — but note a clade present in the table with `reads_dev == 0` is
>   still in the dev taxonomy, so a re-ranking/deletion artifact does not explain
>   it (that applies only to a clade missing from the table entirely).
> - **BLAST-validation agreement** — groups whose agreement rate moved past
>   threshold, with both the validated fraction and the agreement rate (a rate
>   change on a shifting validated subset is ambiguous — note which groups have a
>   stable validated fraction).
> - **Vertebrate-infecting annotation flips** — taxa that gained or lost the
>   annotation between the indexes, with named examples; note it as a possible
>   driver of subset-membership changes (hypothesis only).
> - **Kraken whole-community shifts** — group/rank/read-set combinations past the
>   Bray-Curtis threshold, the range, and the top-moving taxa (viral vs.
>   environmental).
> - **Quality metrics** — a subsection here only if a QC metric (survival, GC,
>   duplication, length, FASTQC flags) shifted past threshold; if all are within
>   threshold, report that in "Checked, no action needed" instead of here.
> - **Schema / inventory** — a subsection here only if something is anomalous
>   (files missing on one side, column changes, an unexpected empty output, a
>   group skipped for a metric); a clean inventory goes in "Checked, no action
>   needed".

`<One ### subsection per dimension that THIS comparison shows a substantial
difference in, titled after the observed result, each ending in a **To confirm:**
line. Delete the dimensions with nothing to report.>`

---

## Checked, no action needed

> **Author instructions.** One short bullet per dimension that was checked and
> showed no flagged difference, so the reader knows it was examined rather than
> skipped — the counterpart to Main findings. ALWAYS include quality metrics and
> schema/inventory here (unless they shifted, in which case they are a Main
> finding instead). State the result plainly with the bounding number (e.g.
> "survival within X pp, no FASTQC flag changes"). No recommendations here — these
> need no action.

- `<dimension: result, with the bounding number — e.g. quality metrics, schema/
  inventory, and any metric dimension checked but within threshold>`

---

## Likely drivers

> **Author instructions.** This section is OPTIONAL and is NOT produced by the
> script — it records any lightweight, by-hand investigation into the *probable
> mechanism* behind a Main finding (see "Optionally investigate likely drivers"
> in SKILL.md for cheap query patterns). Include it only when such investigation
> was actually done; otherwise delete the whole section. Everything here is an
> evidence-backed **hypothesis**, never a verdict — there is no ground truth.
> One short subsection per finding investigated: state the suspected mechanism in
> one sentence, then the concrete evidence (named taxa with taxids, accessions,
> read counts, and the one-line query that produced them) so a reviewer can
> re-run it. Keep this clearly separate from the deterministic findings above.

`<One "### <finding> — <one-line mechanism>" subsection per investigated finding,
or delete this section if none were investigated.>`

---

## Detailed investigation

> **Author instructions.** This is the reference layer behind the Main findings:
> the full per-group tables and the numbers each finding summarizes. Keep prose
> minimal here — let the tables carry it. Each subsection opens with a one-line,
> jargon-free statement of what it measures; deeper method notes and statistical
> caveats live in the Methodology appendix. Report Illumina and ONT separately
> where they differ, and note where ONT has no data (no clade counts, no
> duplicate marking) rather than leaving a blank.

### Completeness and schema

What this checks: every expected output file is present for every group on both
sides, with the same columns.

- **Inventory:** `<N groups × M file types; list anything missing on either
  side, or state none missing>`.
- **Groups skipped for a metric** (from `skipped_groups.tsv`): `<list any group
  excluded from the viral or kraken comparison because a required input was
  present on only one side, or state none were skipped>`.
- **Largest row-count changes** (shared files):

  | Group | File type | rows (main) | rows (dev) | change | % |
  |---|---|---|---|---|---|
  | ... | | | | | |

- **Column conformance:** `<state whether every output matches its schema and
  matches across sides; note any added/removed columns; note empty outputs such
  as bracken>`.

### Viral assignments

What this measures: a read-by-read comparison of the pipeline's viral taxon call
(`aligner_taxid_lca`), matched between runs on `(group, sample, seq_id)`. The
**vertebrate-viral subset** is reads whose assigned taxon is annotated as
vertebrate-infecting in the dev index (see the Methodology appendix for the exact
definition and the excluded "likely-infecting" status, and for the per-read /
taxid-comparison caveats that apply to this whole section).

#### Lost / gained / reassigned reads (vertebrate-viral subset)

Different denominators: % lost = lost ÷ main, % gained = gained ÷ dev,
% reassigned = reassigned ÷ shared.

| Group | Platform | main | dev | shared | reassigned | lost | gained | % lost | % gained | % reassigned |
|---|---|---|---|---|---|---|---|---|---|---|
| ... | | | | | | | | | | |

`<Totals across the subset. Then, for each flagged group, the concentration —
distinct taxid pairs and the top-pair share, from viral_reassignment_concentration.tsv —
so a high % driven by one systematic remap is distinguishable from broad
instability. Name the recurring pairs with taxon names.>`

#### Reassignment severity (how far taxa moved)

| Divergence bucket | Reassigned reads (vertebrate) | (all) |
|---|---|---|
| same-species | | |
| same-genus | | |
| ... | | |
| shared-higher-taxon | | |
| cross-root | | |
| unresolved-taxid *(versioning artifact — not a severity level)* | | |

`<Call out any cross-root or shared-higher-taxon reads (a viral read no longer
placed within a specific viral clade) with named example pairs — take the pairs
from viral_reassignment_pairs.tsv (per group/scope/taxid-pair/bucket counts).
Report unresolved-taxid separately — it is a taxonomy-versioning artifact, not a
biological reassignment; do not rank it as most severe.>`

#### Clade-share breakdown (Illumina only)

`<Per-family and per-order view of viral reads, main vs dev, from
clade_rank_shares.tsv: raw read counts (reads_main, reads_dev, delta_reads) plus
each clade's share of the group's TOTAL viral reads (share_main, share_dev,
delta_pp), for both reads_clade_total and reads_clade_dedup. Flag large count or
share shifts; name clades with taxids. Name any whole families that
appear/disappear and give the number of groups affected. See the Methodology
appendix on the fixed total-viral denominator and dev-taxonomy re-ranking before
reading a disappearance as a biological loss.>`

#### BLAST-validation agreement (secondary)

`<Per-group validated fraction and agreement rate, main vs dev, reported
together. "Agreement" = the aligner call is an ancestor of or equal to the BLAST
call (validation distance 0), not necessarily identical. Secondary signal: BLAST
runs on cluster representatives and is propagated to reads.>`

| Group | frac validated (main) | agree (main) | frac validated (dev) | agree (dev) | Δ agree |
|---|---|---|---|---|---|
| ... | | | | | |

#### Vertebrate-status flips between indexes

`<Count of taxa that gained vs lost the vertebrate-infecting annotation between
the main and dev indexes, with named examples. A possible driver of subset-
membership changes above — hypothesis only.>`

### Kraken abundances

What this measures: a whole-community sanity check. Per group, split by ribosomal
read set, the Bray-Curtis dissimilarity (0 = identical, 1 = disjoint) between the
two runs' relative-abundance profiles at genus and species rank, plus the taxa
that moved most. Dominated by abundant, mostly non-viral taxa — not a viral-
signal detector. See the Methodology appendix for the pooling/subsampling note.

| Rank | Ribosomal | mean BC | median BC | max BC |
|---|---|---|---|---|
| ... | | | | |

`<Highest-dissimilarity group/rank/read-set rows and the top movers, named, with
whether they are viral or environmental.>`

### Quality metrics

What this measures: per group/sample, the raw→cleaned read-survival fraction
compared across runs (in percentage points — a QC/screen change, distinct from a
change in absolute read count), plus mean sequence length, GC%, duplication%,
base count, and FASTQC flag transitions.

`<Survival summary (mean/median/max change), numeric-metric summary, and FASTQC
flag transitions (or none).>`

### Output-file overview (schema-driven)

`<Generic per-group presence and row-count changes across every output file
type, derived from schemas + expected-outputs (no per-file logic). Largest
row-count changes and any structural surprises. Cross-reference findings above
where a row-count change tracks a finding (e.g. a change in validation-hit rows
that moves with a read-level lost/gained finding).>`

---

## Appendices

### Appendix A — Methodology and caveats

> **Author instructions.** Keep only the caveats that change how a number in THIS
> report should be read, and only for analyses that actually appear here (drop a
> caveat whose analysis was skipped or had nothing to report); phrase each in one
> or two plain sentences. These are referenced by name from the
> Detailed-investigation sections so the body stays readable.

- **Vertebrate-viral subset & excluded status.** The subset is taxa annotated
  "affirmatively infecting" (status 1) in the dev index, rolled up to species;
  "likely-infecting" (status 3) reads are excluded by design, so a regression
  confined to status-3 taxa would not trip the vertebrate flags. State the
  status-3 read share if it can be computed; if not, say so (missing-data rule).
  **Union rule for reassigned reads:** a shared read whose taxid differs between
  the two runs is in the subset if **either** side's taxid is vertebrate-infecting
  in the dev index, so a read moving into or out of a vertebrate taxon is retained
  for the comparison rather than silently dropped.
- **Per-read counts & taxid comparison.** Lost/gained/reassigned counts are
  per-read (PCR duplicates included) and taxids are compared as-is. The index
  workflow does not currently publish `taxonomy-merged.dmp`, so taxid
  canonicalization is skipped; in principle a merged/renumbered taxid could
  appear as a spurious reassignment. **Do not invoke this as the driver of
  same-species reassignments** — a same-species move (a child↔parent pair within
  one species) is a genuine LCA-specificity change, not a versioning artifact.
  The `unresolved-taxid` bucket counts the cases where a taxid is genuinely
  absent from the dev taxonomy.
- **Clade-share denominator & dev-taxonomy re-ranking.** Each clade's share uses
  a fixed denominator — the group's total viral reads (the Viruses-root clade
  total) — so a family dropping to 0 does NOT mechanically inflate the others'
  shares. Read `delta_reads` (raw count change) alongside the share: the total
  viral-read count can itself differ between sides (reads reassigned to/from
  higher ranks), so a share move can be driven by the total rather than the
  family, which `delta_reads` disambiguates. A clade-share flag fires on the
  share change alone, so a clade with identical read counts on both sides
  (`delta_reads == 0`) can still be flagged purely as a denominator effect — read
  `delta_reads` before treating a flagged share move as a real change in that
  clade. Rank is resolved from the full dev taxonomy, so a clade that appears in
  the table at all is present in the dev taxonomy: a row with `reads_dev == 0` is
  a genuine read-level drop, NOT a re-ranking/deletion artifact. The re-ranking
  or taxon-deletion explanation applies only to a clade that is absent from the
  table entirely; before invoking it, confirm the taxid is actually missing from
  the dev `taxonomy-nodes.dmp` rather than present with zero reads.
- **BLAST agreement on a shifting subset.** Agreement rate and validated fraction
  are reported together because a rate change on a different validated subset is
  ambiguous.
- **Kraken pooling.** Abundances are pooled across a group's samples
  (depth-weighted) and computed on subsampled reads; Bray-Curtis equals total
  variation distance only for vectors that each sum to 1.

### Appendix B — Consolidated flags

> **Author instructions.** Reproduce the flag table from `flags.tsv` and state
> the thresholds used. Flags are fixed-threshold only (a value exceeding a
> documented absolute threshold). Give counts per category.

Thresholds used: `<list them, e.g. lost >2%, gained >25%, reassigned >10%, clade
share change >3pp, BLAST agreement drop >0.1, Bray-Curtis >0.15>`.

| Focus | Key | Metric | Value | Threshold |
|---|---|---|---|---|
| ... | | | | |

`<Flag totals by category.>`

### Appendix C — Large reference tables

> **Author instructions.** Place oversized tables here (e.g. the full per-group
> clade-share table, the full reassignment severity-bucket table) so the body
> stays skimmable. Add one `###`-titled table per subject.

#### C.1 `<table subject>`

| header | header |
|---|---|
| | |
