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
> or estimate. **Missing-data rule:** if an input needed for a metric is absent
> (e.g. no `--index` → viral analysis skipped; empty bracken), say so plainly in
> that spot and move on; do not invent values. **Naming rule:** every time a
> taxid appears, give its name and rank too, e.g. "Picobirnaviridae (family,
> taxid 585893)" or "Rotavirus A (taxid 10941)"; for a reassignment pair, name
> both, e.g. "Human rotavirus A (10941) → Rotavirus A (28875)". Keep the writing
> plain and explicit — prefer saying fewer things clearly over many dense
> hedges. Spell out shorthand: write "the viral-assignment analysis" not
> "Focus 1", and refer to a detail section by its name, not "§1.1".

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
> comparison scope, then the handful of differences that actually matter, each
> with at most one representative number and a plain-English description of what
> changed. If (and only if) any group is over the gained-reads threshold, describe
> that read-gain finding and the affected groups explicitly, saying what is and
> is not known about it; if no group crosses the threshold, omit it. End with one
> sentence on what was stable (QC, schema). Do not link to file paths;
> the report stands alone. Keep language neutral — describe differences, never
> assert a cause or a good/bad verdict.

`<Prose summary. Example shape: "We compared N groups .... The largest
differences are: (1) the family <Name> (taxid <X>) disappears from the dev
viral results across all Illumina groups; (2) <M> groups have a substantial
fraction of viral reads reassigned to a neighbouring taxon, dominated by one or
two recurring species-level pairs; (3) the <region> groups gain far more
vertebrate-viral reads than they lose, almost entirely on newly added reference
genomes. Whole-community kraken profiles and all QC metrics are stable.">`

**Counts of flagged differences (full table in the Flags appendix):**
`<one short clause per flag category with its count, e.g. "13 groups over the
reassignment threshold; 3 groups over the read-gain threshold; N clade
share-change flags; 3 BLAST-agreement drops; 5 kraken-community flags.">`

---

## Recommendations

> **Author instructions — this list is deterministic.** Emit exactly one bullet
> for each condition below that holds, **in this order**, and no others (omit a
> bullet whose condition is false; the QC bullet, #11, is always emitted). The
> determinism is intentional: two independent reviewers reading the same data
> must produce the same recommendation set. Each bullet is one plain sentence
> phrased as a question to confirm ("Confirm whether X reflects a real change or
> an artifact"), names its taxa, and carries a **fixed** concern level — do not
> re-level. Concern reflects magnitude and breadth only, never a verdict. Never
> use verdict words ("over-calling", "legitimate", "wrong", "caused by"); keep
> any driver link explicitly hypothetical ("consistent with", "hypothesis
> only"). If a Likely-drivers investigation (below) was done, you may add a
> trailing clause pointing to it, but keep the recommendation itself neutral.

1. **(high)** Each whole-clade collapse/appearance — one bullet per family or
   order that has a clade-share-change flag and reaches `share_dev == 0` (or
   `share_main == 0` for an appearance) in **at least half** of that platform's
   groups. (Fixed predicate; no subjective "many".)
2. **(high)** The single highest reassignment-rate group over the threshold,
   called out on its own bullet **only when its rate is ≥ 1.5× the next-highest
   over-threshold group**; otherwise omit this bullet and let it fall into the
   bullet below.
3. **(medium)** All remaining groups over the reassignment threshold, as ONE
   bullet.
4. **(medium)** Groups over the lost threshold, as ONE bullet — only if any
   exist.
5. **(high)** Groups over the gained threshold, as ONE bullet — only if any
   exist. (The threshold is on gained reads *as a fraction of the dev total*, so
   high turnover can trip it even without net growth — note this.)
6. **(medium)** Vertebrate-status flips between indexes, as ONE bullet.
7. **(medium)** BLAST agreement-rate drops over threshold, as ONE bullet — only
   if any.
8. **(medium)** Kraken Bray-Curtis flags over threshold, as ONE bullet — only if
   any.
9. **(low)** Any cross-root or shared-higher-taxon reassignments (viral reads no
   longer placed within a specific viral clade), as ONE bullet — only if the
   count is > 0.
10. **(low)** Empty outputs (e.g. bracken), as ONE bullet — only if any are
    empty.
11. **(low)** A QC note, ALWAYS as exactly ONE bullet: state the QC result
    either way — "QC stable: survival change within threshold, no FASTQC flag
    changes" or the specific shift. (Always emitted, so the recommendation count
    is deterministic across reviewers.)

`<Render the applicable bullets here.>`

---

## Main findings

> **Author instructions.** One `###` subsection per *substantial* finding —
> include only findings actually present in this comparison; delete the
> subsections that don't apply, and add one if a real finding doesn't fit the
> list below. Lead each with a one-sentence statement of *what changed* (the
> result), then 2–4 plain sentences of supporting numbers. Put method and
> statistical caveats in the matching Detailed-investigation section and the
> Methodology appendix, not here. Name every taxon. Do not assert causes; if a
> Likely-drivers investigation was done, point to it rather than restating it.

### A whole viral clade disappears

`<Name the family/order that goes to ~0% share in dev across many groups, with
taxids and ranks, the number of groups affected, and the largest per-group share
drops in percentage points (share of all viral reads). State the alternatives
plainly — a real loss vs. a reference/classification effect — as alternatives,
not a conclusion. Cross-reference the "Clade-share breakdown" detail section.>`

### Many groups have viral reads reassigned to a neighbouring taxon

`<How many groups exceed the reassignment threshold and the range. Name the
single highest group and its rate. State that the reassignments concentrate in
one or two recurring taxid pairs — name them with taxon names, e.g. "Human
rotavirus A (10941) → Rotavirus A (28875)" — and give the top-pair share so the
reader sees this is a systematic remap, not broad instability. Note the
taxonomic depth in one phrase (e.g. "nearly all same-species moves"). Cross-
reference the "Lost / gained / reassigned" and "Reassignment severity" sections.>`

### One or more groups gain viral reads heavily

`<Name the groups (e.g. the Iowa / IA_* groups) that gain far more vertebrate-
viral reads than they lose. For each, give dev vs main counts, the number
gained, and gained as a fraction of the dev total. Say explicitly what is and is
NOT known: the reads are present in dev and absent in main on the matched key;
whether this is a real detection change or an index/annotation effect is not
established here. If the gain co-occurs with downstream row-count growth
(validation hits, duplicate stats), note it as consistent. Cross-reference the
"Lost / gained / reassigned" and "Vertebrate-status flips" sections.>`

### BLAST-validation agreement drops for some groups

`<How many groups drop agreement past the threshold and by how much, named, with
both the validated fraction and the agreement rate for each (a rate change on a
shifting validated subset is ambiguous — say which groups have a stable
validated fraction and which don't). Cross-reference the "BLAST-validation
agreement" section.>`

### Vertebrate-infecting status flips between the two indexes

`<How many taxa gained vs lost the vertebrate-infecting annotation between the
main and dev indexes, with a couple of named examples. State the hypothesis
plainly: a read can enter or leave the vertebrate-viral subset purely because
its taxon's annotation flipped — hypothesis only. Cross-reference the
"Vertebrate-status flips" section.>`

### Whole-community (kraken) profiles shift in some groups

`<How many group/rank/read-set combinations exceed the Bray-Curtis threshold,
the range, and at which rank/read-set. Name the top-moving taxa and note whether
they are viral or environmental/non-viral. Cross-reference the "Kraken
abundances" section.>`

### Quality metrics are stable (or: QC changed as follows)

`<State the QC result plainly either way: e.g. "read survival, GC, duplication,
and FASTQC checks are essentially unchanged (max survival change <0.05 pp, 0
FASTQC flag transitions)", OR describe the specific shift. Cross-reference the
"Quality metrics" section.>`

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
where a row-count change tracks a finding (e.g. more validation hits where reads
were gained).>`

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
> clade-collapse table, the full severity-bucket table) so the body stays
> skimmable. Add one `###`-titled table per subject.

#### C.1 `<table subject>`

| header | header |
|---|---|
| | |
