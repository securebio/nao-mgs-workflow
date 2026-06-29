# DOWNSTREAM release comparison report

This report compares the DOWNSTREAM output of a release-candidate pipeline run
(the `dev` branch) against the current production run (the `main` branch), to
help a human decide whether the candidate is safe to release. It is a holistic
diff: the two runs usually differ in **code, reference index, AND QC parameters
all at once**, so any single difference here cannot be attributed to one cause,
and — with no ground truth to compare against — is neither good nor bad on its
face. **This report flags differences as questions for a human, never as
verdicts.** Where a likely driver is obvious, it is named only as a hypothesis.

> **How to fill this in (instructions for the report author; delete this block
> in the final REVIEW.md).** Replace every `<placeholder>`. Every number must
> trace to a TSV produced by `bin/compare_downstream_runs.py` — never fabricate
> or estimate. **Report what this comparison's data actually shows** — the
> examples and illustrative shapes throughout this template are format guides,
> not expected results; do not carry their specific taxa, groups, directions, or
> magnitudes into your report, and do not assume a finding exists until the TSVs
> show it. **Missing-data rule:** if an input needed for a metric is absent
> (e.g. no `--index` → viral analysis skipped; empty bracken), say so plainly in
> that spot and move on; do not invent values. **Naming rule:** every time a
> taxid appears, give its name and rank too, in the form
> `<taxon name> (<rank>, taxid <id>)`; for a reassignment pair, name both, in the
> form `<taxon> (<id>) → <taxon> (<id>)`. Keep the writing plain and explicit —
> prefer saying fewer things clearly over many dense hedges. Spell out shorthand:
> write "the viral-assignment analysis" not "Focus 1", and refer to a detail
> section by its name, not "§1.1".

## Run identity

| | Candidate (`dev`) | Reference (`main`) |
|---|---|---|
| DOWNSTREAM output | `s3://path/to/dev/...` | `s3://path/to/main/...` |
| Index | `s3://nao-mgs-index/<DATE>` | `s3://nao-mgs-index/<DATE>` |
| Code version | `<ver, if known>` | `<ver, if known>` |

- **Comparison scope:** `<N>` groups (`<X>` Illumina + `<Y>` ONT), matched by
  name. `<note any group or file type missing on one side; if none, say so>`.
- **Report generated:** `<YYYY-MM-DD HH:MM>`

---

## Summary

> **Author instructions.** Write 4–8 short prose sentences (not telegraphic
> bullets) that a reader unfamiliar with this skill can follow. State the
> comparison scope first, then walk through the differences this comparison
> actually surfaced — driven by the flags in `flags.tsv` and the notable
> differences in the per-metric tables, ordered by how much they would matter to
> a human (breadth across groups/platforms × magnitude past threshold). Give each
> at most one representative number and a plain-English description of what
> changed. Cover every metric dimension that produced a flag (don't drop one
> because it seems minor), and state plainly which dimensions were stable or
> unflagged so the reader knows they were checked, not skipped. Do not link to
> file paths; the report stands alone. Keep language neutral — describe
> differences, never assert a cause or a good/bad verdict.

`<Prose summary of THIS comparison's findings. Illustrative shape only — your
content, taxa, directions, and counts will differ: "We compared N groups (X
Illumina + Y ONT). The differences that stand out are: (1) <dimension and what
changed, with one number>; (2) <…>; (3) <…>. <Dimensions that were checked and
showed no flagged difference, e.g. QC and schema, named explicitly>.">`

**Counts of flagged differences (full table in the Flags appendix):**
`<one short clause per flag category that has any flags in THIS run, with its
count; omit categories with zero flags. Do not carry over example counts.>`

---

## Recommendations

> **Author instructions — coverage is deterministic; concern is not pre-assigned.**
> Walk the category list below **in order**, and emit a recommendation for every
> category that has at least one flag or qualifying difference in THIS comparison
> (skip a category with none; the QC category is always emitted). Emitting one
> recommendation per flagged category — and no extras — is what makes two
> independent reviewers of the same data produce the same recommendation *set*;
> that coverage rule is the only thing fixed here.
>
> **Do not stamp a fixed high/medium/low verdict on a category.** Severity is not
> a property of the category — a 3-group reassignment cluster and a 14-group one
> are not equally concerning. Instead, annotate each recommendation with its
> objective **breadth and magnitude** (how many groups and platforms, and how far
> past threshold — in the form "<k> of <N> <platform> groups, up to <X> past
> threshold"); that is the prioritization signal, and the human applies judgment
> to it. If you choose
> to add a one-word relative-priority hint, derive it only from breadth ×
> magnitude and say so — never from an assumption that a given metric is
> inherently more serious.
>
> Phrase each recommendation as one plain sentence asking a human to confirm
> something ("Confirm whether X reflects a real change or an artifact"), name the
> taxa/groups involved, and keep any driver link explicitly hypothetical
> ("consistent with", "hypothesis only"). Never use verdict words ("over-calling",
> "legitimate", "wrong", "caused by"). If a Likely-drivers investigation (below)
> was done, you may add a trailing clause pointing to it, but keep the
> recommendation itself neutral.
>
> Categories, in emission order (each is one bullet unless noted; include only if
> the data has it):
>
> 1. Clade-share collapses or appearances — one bullet per family/order with a
>    clade-share-change flag that reaches `share_dev == 0` (collapse) or
>    `share_main == 0` (appearance); state in how many of that platform's groups.
> 2. Viral reads reassigned to a different taxon (groups over the reassignment
>    threshold) — one bullet; give the count, the rate range, and the
>    highest-rate group.
> 3. Cross-root or shared-higher-taxon reassignments (viral reads no longer placed
>    within a specific viral clade) — only if the count is > 0; report the bucket
>    counts.
> 4. Viral reads lost (groups over the lost threshold) — only if any.
> 5. Viral reads gained (groups over the gained threshold) — only if any. The
>    threshold is on gained reads *as a fraction of the dev total*, so high
>    turnover can trip it even without net growth — note this.
> 6. Vertebrate-infecting annotation flips between the two indexes — only if any.
> 7. BLAST-agreement-rate drops over threshold — only if any.
> 8. Kraken whole-community (Bray-Curtis) shifts over threshold — only if any.
> 9. Schema / inventory anomalies (files missing on one side, column changes,
>    empty outputs such as bracken, groups skipped for a metric) — only if any.
> 10. Quality metrics — ALWAYS one bullet; state the result either way (e.g.
>     survival and FASTQC checks within threshold, or the specific shift). Always
>     emitted so the minimum recommendation count is fixed.

`<Render the applicable bullets here, in the order above.>`

---

## Main findings

> **Author instructions.** Write one `###` subsection per metric dimension that
> showed a *substantial* difference in THIS comparison — a consolidated flag, or
> a difference large enough that a human should see it even if it didn't trip a
> threshold. The candidate dimensions are listed below so no class of finding is
> missed; **include a dimension only if its data shows something**, and delete the
> rest. Don't presume any of them happened — let the TSVs decide. **Title each
> subsection after what the data actually shows**, not after the dimension's
> name: a neutral, factual headline (e.g. for the clade dimension, "Family <Name>
> drops to zero share in N Illumina groups" if that is what occurred — not the
> generic label). Lead each with a one-sentence statement of *what changed*, then
> 2–4 plain sentences of supporting numbers. Where a difference is bidirectional
> or mixed (some groups up, some down), say so rather than forcing a single
> direction. Put method and statistical caveats in the matching
> Detailed-investigation section and the Methodology appendix, not here. Name
> every taxon. Do not assert causes; if a Likely-drivers investigation was done,
> point to it rather than restating it.
>
> Candidate dimensions (each maps to a Detailed-investigation section; cross-
> reference it). For each, the kind of thing to report if present:
>
> - **Read-level viral assignment changes** — lost / gained / reassigned reads in
>   the vertebrate-viral subset: which groups cross a threshold, the rate range,
>   the highest group, and (for reassignment) whether the moves concentrate in a
>   few recurring taxid pairs (name them, give the top-pair share) or are broad.
>   Note the denominators (% lost ÷ main, % gained ÷ dev, % reassigned ÷ shared)
>   and, for gains, that the metric is a fraction of the dev total so turnover can
>   trip it without net growth. State what is and is NOT known: the reads differ
>   on the matched key; whether real or an index/annotation effect is not
>   established here.
> - **Reassignment taxonomic severity** — how far reassigned reads moved: any
>   cross-root or shared-higher-taxon moves (viral reads no longer within a
>   specific viral clade), with named example pairs. Report `unresolved-taxid`
>   separately — it is a versioning artifact, not a severity level.
> - **Clade-share shifts (family / order, Illumina)** — clades whose share of
>   total viral reads moved materially, including any family/order that appears or
>   disappears across groups; give taxids, the number of groups, and the largest
>   per-group share moves in percentage points. State the alternatives (a real
>   change vs. a reference/classification/re-ranking effect) as alternatives.
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
> - **Quality metrics** — report this dimension's result either way (a specific
>   shift, or that survival/GC/duplication/FASTQC checks are within threshold), so
>   the reader knows QC was checked.

`<One ### subsection per dimension that THIS comparison shows a substantial
difference in, titled after the observed result. Delete the dimensions with
nothing to report; for QC, state the result either way.>`

---

## Likely drivers (optional — manual investigation, hypotheses only)

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
share shifts; name clades with taxids. Call out whole families that
appear/disappear across many groups. See the Methodology appendix on the
fixed total-viral denominator and dev-taxonomy re-ranking before reading a
disappearance as a biological loss.>`

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

> **Author instructions.** Keep the caveats that change how a number should be
> read; phrase each in one or two plain sentences. These are referenced from the
> Detailed-investigation sections so the body stays readable.

- **Holistic diff, no attribution.** main and dev differ in code, index, and QC
  parameters simultaneously; this report flags differences for a human and never
  attributes them to a single cause.
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
  family, which `delta_reads` disambiguates. Rank is resolved from the full dev
  taxonomy; a taxon deleted from it drops from the table, and a "disappearance"
  can be a re-ranking artifact rather than a biological loss.
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
