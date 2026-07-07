# DOWNSTREAM comparison report

This report compares the DOWNSTREAM output of two pipeline runs — a candidate run
against a reference run, each a **(pipeline version, index) pair** — to
help a human decide whether the candidate is safe to adopt. With no ground truth
to compare against, a difference is neither good nor bad on its face.

> **How to fill this in (instructions for the report author; delete this block
> and every `> **Author instructions**` block in the final REVIEW.md).** Replace
> every `<placeholder>`. Every number must trace to a TSV produced by
> `bin/compare_downstream_runs.py` — never fabricate or estimate. **Report what
> this comparison's data actually shows** — the examples and illustrative shapes
> throughout this template are format guides, not expected results; do not carry
> their specific taxa, groups, directions, or magnitudes into your report, and do
> not assume a finding exists until the TSVs show it. **Missing-data rule:** if an
> input needed for a metric is absent (e.g. no `--candidate-index` → viral analysis skipped;
> empty bracken), say so plainly in that spot and move on; do not invent values.
> **Follow the report-writing principles in SKILL.md** (name every taxon; plain
> language; no tool-internal jargon) throughout — they apply to every section.

## Run identity

> **Author instructions.** Take the DOWNSTREAM roots, index roots, and pipeline
> versions from `run_identity.tsv`. Use the **specific** pipeline version (e.g.
> `3.2.2.0-dev`), not a branch label; if a version reads `unknown`, supply
> `--candidate-version` / `--reference-version` and re-run (SKILL.md Step 1)
> rather than writing "dev branch".

| | Candidate (`--candidate`) | Reference (`--reference`) |
|---|---|---|
| DOWNSTREAM output | `s3://path/to/candidate/...` | `s3://path/to/reference/...` |
| Index | `s3://nao-mgs-index/<DATE>` | `s3://nao-mgs-index/<DATE>` |
| Pipeline version | `<exact version from run_identity.tsv>` | `<exact version>` |

- **What differs between the runs:** `<list which of {pipeline code, index, QC
  parameters} actually differ — this sets how far any difference can be
  attributed (see the attribution statement below). If only one differs, say so.
  Only the pipeline version and index identity are observable here; QC parameters
  are not emitted, so list them as "not confirmed unchanged", NOT as differing.>`
- **Comparison scope:** `<N>` groups (`<X>` Illumina + `<Y>` ONT), matched by
  name. `<note any group or file type missing on one side; if none, say so>`.
- **Report generated:** `<YYYY-MM-DD HH:MM>`

> **Author instructions.** Print the one attribution statement that matches what
> differs between these two runs (see SKILL.md "Attribution"); delete the other.
> A dimension you cannot verify is **"not confirmed unchanged"**, never "differs":
> QC parameters are not emitted by the comparison, so unless the user states
> otherwise, list QC as unconfirmed — do not claim it differs.

`<Attribution statement for THIS comparison — one of: "These runs differ in
<list ≥2 of code / index / QC parameters, or any dimension that cannot be
confirmed unchanged>, so a difference here cannot be attributed to a single
cause." OR "These runs differ only in <the one dimension> (the others confirmed
unchanged), so a difference is attributable to that dimension more directly.">`

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
> threshold. **Coverage is deterministic: a subsection is required for (a) every
> dimension with a flag in `flags.tsv`, and (b) two enumerated non-flag triggers a
> flag can miss — any cross-root or shared-higher-taxon reassignment, and any
> clade reaching zero candidate-side share.** Don't drop one as "minor" — that
> fixed set is what makes two independent reviewers surface the same findings.
> Dimensions checked but within threshold and with neither trigger go in
> "Checked, no action needed" below, not here. The
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
> matching Detailed investigation section and the Methodology appendix, not here.
>
> **Fold the mechanism in (no separate section).** When a Step-4 cross-check
> points to a suspected cause, add a short `**Likely mechanism:**` clause **within
> the finding** — there is no separate "Likely drivers" section. Tag it with one
> of three evidence-anchored confidence levels — **Strongly supported** /
> **Consistent** / **Speculative** (criteria in SKILL.md Step 4) — and cite the
> evidence that earns the level; if nothing supports a mechanism, omit the clause
> entirely. The level rates the evidence for the cause, not the finding's
> severity, and even "Strongly supported" is an inference, not a verdict (no
> ground truth). Attach the clause to the causal claim only — do not label a
> deterministic number (straight from a TSV) as a mechanism. In particular, when a
> clade/taxon drops out of the viral counts, check `vertebrate_status_flips.tsv`
> first: a `lost_vertebrate` re-annotation removes it from the index's
> vertebrate-infecting set and is the most likely benign cause — say so rather
> than guessing at a genome removal.
>
> **End each subsection with a brief reviewer-facing line.** Usually a
> `**To confirm:**` line — one or two plain sentences asking the human to confirm
> whether the change reflects a real change or an artifact, naming the
> taxa/groups. But **do not manufacture an action when nothing specific is
> actionable**: a self-explanatory finding (e.g. a new detection from a genome
> added to the index) can close with a one-line `**Note:**` flagging it for
> awareness instead. Either way use no verdict words ("over-calling",
> "legitimate", "wrong", "caused by").
>
> Candidate dimensions (each maps to a Detailed investigation section; cross-
> reference it). For each, the kind of thing to report if present:
>
> - **Read-level viral assignment changes** — lost / gained / reassigned reads in
>   the vertebrate-viral subset: which groups cross a threshold, the rate range,
>   the highest group, and (for reassignment) whether the moves concentrate in a
>   few recurring taxid pairs (name them, give the top-pair share) or are broad.
>   Split group counts by platform (in the form "<n> Illumina + <m> ONT"); the
>   flag key does not carry platform, so do not assume every flagged group is
>   Illumina. Note the denominators (% lost ÷ reference, % gained ÷ candidate,
>   % reassigned ÷ shared) and, for gains, that the metric is a fraction of the
>   candidate total so turnover can trip it without net growth. State what is and
>   is NOT known: the
>   reads differ on the matched key; whether real or an index/annotation effect is
>   not established here.
> - **Reassignment taxonomic severity** — how far reassigned reads moved: any
>   cross-root or shared-higher-taxon moves (viral reads no longer within a
>   specific viral clade), with named example pairs. Report `unresolved-taxid`
>   separately — it is a versioning artifact, not a severity level.
> - **Clade-share shifts (family / order, Illumina)** — clades whose share of
>   total viral reads moved materially, including any family/order that drops to
>   zero candidate share or newly appears; give taxids and the largest per-group
>   share moves in percentage points. State two distinct counts without conflating
>   them: how many groups *flagged* (share change past the threshold), and
>   separately in how many groups the clade reached zero candidate share (a
>   collapse can occur below the flag threshold, and a flagged move need not be a
>   collapse). The flag fires at BOTH family and order rank, so a family and its
>   parent order are two flags for one underlying clade — break the count down by
>   rank (family rows vs order rows) rather than reporting a single conflated
>   total. The same family can also flag in OPPOSITE directions across groups (a
>   drop in some, a rise in others, often a denominator effect) — say so rather
>   than implying one direction. Before calling a share drop a collapse, check the raw read counts
>   (`reads_reference`/`reads_candidate`, i.e. `delta_reads`): a clade's share can
>   fall purely because the group's total viral reads grew, so confirm the clade's
>   own reads actually dropped. For any clade that drops to zero candidate share,
>   cross-check `vertebrate_status_flips.tsv` first: if its taxa appear as
>   `lost_vertebrate`, the family was re-annotated out of the index's
>   vertebrate-infecting set (the most likely benign cause) — fold that in as the
>   hypothesis. State the alternatives (a real change vs. a re-annotation or
>   reference-genome effect) as alternatives — but note a clade present in the
>   table with `reads_candidate == 0` is still in the candidate-index taxonomy, so
>   a re-ranking/deletion artifact does not explain it (that applies only to a
>   clade missing from the table entirely).
> - **BLAST-validation agreement** — groups whose agreement rate moved past
>   threshold, with both the validated fraction and the agreement rate (a rate
>   change on a shifting validated subset is ambiguous — note which groups have a
>   stable validated fraction). For a flagged group, break the change down by
>   taxon from `viral_validation_agreement_by_taxon.tsv`: name the taxa with the
>   most validated reads driving the move, their per-side agreement rate and
>   `delta_agreement`, and use `mean_distance_disagree` (the mean distance over the
>   *disagreeing* reads only) to say how far off the new disagreements are (a
>   one-edge offset is mild; a large distance is not). Do not use `mean_distance`
>   for this — it is over all validated reads and dilutes toward 0 when agreement
>   is high.
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
or **Note:** line. Delete the dimensions with nothing to report.>`

---

## Checked, no action needed

> **Author instructions.** One short bullet per dimension that was checked and
> showed no flagged difference, so the reader knows it was examined rather than
> skipped — the counterpart to Main findings. Include quality metrics and
> schema/inventory here whenever they were computed (unless they shifted, in which
> case they are a Main finding instead); if an input was missing so a metric could
> not be computed, say that here per the missing-data rule rather than asserting it
> was stable. State the result plainly with the bounding number (e.g. "survival
> within X pp, no FASTQC flag changes"). No recommendations here — these need no
> action.

- `<dimension: result, with the bounding number — e.g. quality metrics, schema/
  inventory, and any metric dimension checked but within threshold>`

---

## Detailed investigation

> **Author instructions.** This is the reference layer behind the Main findings:
> the per-group tables and the numbers each finding summarizes. Show the rows the
> per-section table asks for (typically the largest/flagged rows) and state the
> total count; park any oversized full table in Appendix C rather than inlining it
> here. Keep prose minimal — let the tables carry it. Each subsection opens with a
> one-line,
> jargon-free statement of what it measures; deeper method notes and statistical
> caveats live in the Methodology appendix. Report Illumina and ONT separately
> where they differ, and note where ONT has no data (no clade counts, no
> duplicate marking) rather than leaving a blank.

### Completeness and schema

What this checks: every expected output file is present for every group on both
sides, with the same columns. (Row-count *changes* are covered separately in
Output-file overview.)

- **Inventory:** `<total file-type rows in file_inventory.tsv and how they
  decompose — do NOT state a uniform "N groups × M types" grid, since platforms
  have different expected sets (ONT lacks clade_counts / duplicate_stats / fastp);
  give it per-platform (e.g. "X Illumina groups × A types + Y ONT × B types").
  List anything missing on either side, or state none missing.>`.
- **Groups skipped for a metric** (from `skipped_groups.tsv`): `<list any group
  excluded from the viral or kraken comparison because a required input was
  present on only one side, or state none were skipped>`.
- **Column conformance:** `<state whether every output matches its schema and
  matches across sides; note any added/removed columns; note empty outputs such
  as bracken>`.

### Viral assignments

What this measures: a read-by-read comparison of the pipeline's viral taxon call
(`aligner_taxid_lca`), matched between runs on `(group, sample, seq_id)` when a
`sample` column is present, else `(group, seq_id)`. The
**vertebrate-viral subset** is reads whose assigned taxon is annotated as
vertebrate-infecting in the candidate index (see the Methodology appendix for the exact
definition and the excluded "likely-infecting" status, and for the per-read /
taxid-comparison caveats that apply to this whole section).

#### Lost / gained / reassigned reads (vertebrate-viral subset)

Different denominators: % lost = lost ÷ reference, % gained = gained ÷ candidate,
% reassigned = reassigned ÷ shared. (This table is the `scope == vertebrate` rows
of `viral_read_status.tsv`; the `scope == all` rows are the all-viral counterpart,
not shown here.)

| Group | Platform | reference | candidate | shared | reassigned | lost | gained | % lost | % gained | % reassigned |
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

`<Per-family and per-order view of viral reads, reference vs candidate, from
clade_rank_shares.tsv: raw read counts (reads_reference, reads_candidate,
delta_reads) plus each clade's share of the group's TOTAL viral reads
(share_reference, share_candidate, delta_pp). Flags are computed on the
`reads_clade_total` basis only; the
`reads_clade_dedup` rows are context, not a flag source. Report large count or
share shifts; name clades with taxids. Name any whole families that
appear/disappear and give the number of groups affected. See the Methodology
appendix on the fixed total-viral denominator and candidate-index re-ranking before
reading a disappearance as a biological loss.>`

#### BLAST-validation agreement (secondary)

`<Per-group validated fraction and agreement rate, reference vs candidate,
reported together. "Agreement" = the aligner call is an ancestor of or equal to
the BLAST call (validation distance 0), not necessarily identical. Secondary
signal: BLAST runs on cluster representatives and is propagated to reads.>`

| Group | frac validated (reference) | agree (reference) | frac validated (candidate) | agree (candidate) | Δ agree |
|---|---|---|---|---|---|
| ... | | | | | |

`<For each flagged group, break the change down by taxon from
viral_validation_agreement_by_taxon.tsv: the aligner taxa (named, with taxid) that
carry the most validated reads, their per-side agreement_rate and delta_agreement,
and mean_distance_disagree (the mean distance over the disagreeing reads only — how
far off the new disagreements are; NOT mean_distance, which is over all validated
reads and dilutes toward 0 when agreement is high). This answers which taxa are
most affected and how bad the disagreements are. Park the full per-taxon table in
Appendix C if long.>`

| Group | Taxon (taxid) | n validated (ref → cand) | agree (ref → cand) | Δ agree | mean disagree dist (ref → cand) |
|---|---|---|---|---|---|
| ... | | | | | |

#### Vertebrate-status flips between indexes

`<Count of taxa that gained vs lost the vertebrate-infecting annotation between
the reference and candidate indexes. The table can hold hundreds of rows; do NOT
enumerate them — name only the flips that drive a finding above (e.g. the taxa
behind a clade collapse or a read gain), and give the totals for the rest. A
possible driver of subset-membership changes above — hypothesis only.>`

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
whether they are viral or environmental. If you say the top movers contain no
viral taxa, verify it against the rows first (check each top-mover taxid's
annotation) — do not assert a category is absent unless the data confirm none;
"the largest movers are environmental" is safer than "no viral taxa".>`

### Quality metrics

What this measures: per group/sample, the raw→cleaned read-survival fraction
compared across runs (in percentage points — a QC/screen change, distinct from a
change in absolute read count), plus mean sequence length, GC%, duplication%,
base count, and FASTQC flag transitions.

`<Survival summary (mean/median/max change), numeric-metric summary, and FASTQC
flag transitions (or none).>`

### Output-file overview (schema-driven)

What this measures: per-group row-count changes across every output file type,
derived from schemas + expected-outputs (no per-file logic). (Presence and column
conformance are covered in Completeness and schema; this section owns the
quantitative row-count deltas.)

| Group | File type | rows (reference) | rows (candidate) | change | % |
|---|---|---|---|---|---|
| ... | | | | | |

`<Largest row-count changes and any structural surprises. Cross-reference findings
above where a row-count change tracks a finding (e.g. a change in validation-hit
rows that moves with a read-level lost/gained finding).>`

---

## Appendices

### Appendix A — Methodology and caveats

> **Author instructions.** Keep only the caveats that change how a number in THIS
> report should be read, and only for analyses that actually appear here (drop a
> caveat whose analysis was skipped or had nothing to report); phrase each in one
> or two plain sentences. These are referenced by name from the
> Detailed investigation sections so the body stays readable.

- **Vertebrate-viral subset & excluded status.** The subset is taxa annotated
  "affirmatively infecting" (status 1) in the candidate index, rolled up to species;
  "likely-infecting" (status 3) reads are excluded by design, so a regression
  confined to status-3 taxa would not trip the vertebrate flags. State the
  status-3 read share if it can be computed; if not, say so (missing-data rule).
  **Union rule for reassigned reads:** a shared read whose taxid differs between
  the two runs is in the subset if **either** side's taxid is vertebrate-infecting
  in the candidate index, so a read moving into or out of a vertebrate taxon is retained
  for the comparison rather than silently dropped.
- **Per-read counts & taxid comparison.** Lost/gained/reassigned counts are
  per-read (PCR duplicates included) and taxids are compared as-is. The index
  workflow does not currently publish `taxonomy-merged.dmp`, so taxid
  canonicalization is skipped; in principle a merged/renumbered taxid could
  appear as a spurious reassignment. **Do not invoke this as the driver of
  same-species reassignments** — a same-species move (a child↔parent pair within
  one species) is a genuine LCA-specificity change, not a versioning artifact.
  The `unresolved-taxid` bucket counts the cases where a taxid is genuinely
  absent from the candidate-index taxonomy.
- **Clade-share denominator & candidate-index re-ranking.** Each clade's share uses
  a fixed denominator — the group's total viral reads per side (the Viruses-root
  clade total). This avoids the *within-rank* inflation artifact (dividing by a
  sum over only the family rows, where one family vanishing would inflate the
  rest), but it does NOT eliminate denominator effects: the per-side total viral
  reads itself differs between runs whenever reads are added or removed (a clade
  re-annotated out, a new taxon gained), so a family with unchanged raw counts can
  still gain or lose share purely from the shrinking/growing total. Always read
  `delta_reads` (raw count change) alongside the share: a share move can be driven
  by the total rather than the family, which `delta_reads` disambiguates. A
  clade-share flag fires on the share change alone, so a clade with identical read
  counts on both sides (`delta_reads == 0`) can still be flagged purely as a
  denominator effect — read
  `delta_reads` before treating a flagged share move as a real change in that
  clade. Rank is resolved from the full candidate-index taxonomy, so a clade that appears in
  the table at all is present in the candidate-index taxonomy: a row with
  `reads_candidate == 0` is a genuine read-level drop, NOT a re-ranking/deletion
  artifact. The re-ranking
  or taxon-deletion explanation applies only to a clade that is absent from the
  table entirely; before invoking it, confirm the taxid is actually missing from
  the candidate index's `taxonomy-nodes.dmp` rather than present with zero reads.
- **BLAST agreement on a shifting subset.** Agreement rate and validated fraction
  are reported together because a rate change on a different validated subset is
  ambiguous.
- **Kraken pooling.** Abundances are pooled across a group's samples
  (depth-weighted) and computed on subsampled reads; Bray-Curtis equals total
  variation distance only for vectors that each sum to 1.

### Appendix B — Consolidated flags

> **Author instructions.** Reproduce the flag table from `flags.tsv` and state
> the thresholds used. Flags are fixed-threshold only (a value exceeding a
> documented absolute threshold). Give counts per category. For the clade-share
> category, break the total down by rank (family rows vs order rows): the flag
> fires at both family and order rank, so the raw count double-counts each
> underlying clade once per rank — report both so the number is not mistaken for
> distinct clades.

Thresholds used: `<list them, e.g. lost >2%, gained >25%, reassigned >10%, clade
share change >3pp, BLAST agreement drop >0.1, Bray-Curtis >0.15>`.

| Focus | Key | Metric | Value | Threshold |
|---|---|---|---|---|
| ... | | | | |

`<Flag totals by category.>`

### Appendix C — Large reference tables

> **Author instructions.** Place oversized tables here (e.g. the full per-group
> clade-share table, the full reassignment severity-bucket table) so the body
> stays skimmable. Add one `####`-titled table per subject.

#### C.1 `<table subject>`

| header | header |
|---|---|
| | |
