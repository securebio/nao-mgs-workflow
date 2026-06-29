---
name: benchmark-downstream
description: Compare the DOWNSTREAM output of two pipeline runs — each a (pipeline version, reference index) pair, typically a release candidate vs current production — and flag large differences for human review. Use to vet a candidate's viral assignments, kraken abundances, and QC metrics against a reference run before adopting it.
---

# Compare DOWNSTREAM output of two runs

This skill diffs the DOWNSTREAM output of two pipeline runs across both platforms
and flags large differences for a human to adjudicate. Each run is a **(pipeline
version, reference index) pair**; the common case is a release diff (`dev` + its
index vs `main` + its index, which the CLI labels `--dev` / `--main`), but it
works for any two version/index pairs — e.g. the same code with only the index
rebuilt. `bin/compare_downstream_runs.py` does the deterministic data extraction
(munging + I/O); the per-metric calculations live in `bin/downstream_metrics.py`
so they can be reviewed and tested separately. You fill in `review-template.md`
from the script's TSV outputs.

**This is a difference-flagging diff, not a causal analysis.** There is no ground
truth, so no difference is "good" or "bad" on its face — the report surfaces and
flags large changes for human review, naming a likely driver only as a
hypothesis.

**Attribution.** How far a difference can be attributed depends on what differs
between the runs. First establish which of {pipeline code, reference index, QC
parameters} actually differ (from the version/index pairs and params diff), and
record it in the report's Run identity. Then print the single matching attribution
statement in the report intro (the template marks the spot), deleting the others:
- They differ in **more than one** dimension (the typical release diff) → a
  difference cannot be pinned on any single cause.
- They differ in **only one** dimension (e.g. same code, only the index rebuilt)
  → a difference is attributable to that dimension more directly.

Either way there is no ground truth, so still no good/bad verdict.

**`review-template.md` is the source of truth for report structure** — open it
and fill it in literally. `REVIEW.md` must stand alone: embed the needed tables
and numbers rather than pointing at output files.

**Do NOT commit `REVIEW.md` or any extracted tables, or paste real sample
names / taxids / abundances into a PR.** They originate from AWS data; keep
them local and (optionally) in the agent scratch bucket. The PR contains only
the skill, scripts, tests, and docs.

## Report-writing principles

These apply to **every section** of `REVIEW.md`, not just one. They are also the
general standard for editing this template and skill: prose here should obey the
same rules.

- **Name every entity.** Give taxa as `<name> (<rank>, taxid <id>)` and name both
  sides of a reassignment pair: `<taxon> (<id>) → <taxon> (<id>)`. The same goes
  for any other entity — never refer to "a group", "a sample", "a reference
  genome/accession", "a species", or "a clade" without naming it; an unnamed
  entity is not actionable. Avoid vague quantifiers ("many groups", "several
  taxa") — give the count and name the notable ones.
- **Plain language; no tool-internal jargon.** In the Summary, the findings, and
  the To-confirm lines, describe what happened observationally, not how the
  tooling detected it. Don't write "tripping the clade-share threshold", "in the
  shared-higher-taxon bucket", "Focus 1", or "§1.1"; say e.g. "its share of viral
  reads dropped by N points" or "no longer classified within any specific viral
  family", and refer to a section by its name. State a threshold as a plain fact
  only when the reader needs it. Defined technical terms (e.g. Bray-Curtis, the
  vertebrate-viral subset) are fine in the detail and appendix sections where they
  are introduced.
- **Results first.** Lead with what changed, then the supporting numbers; keep
  method and caveats in the detail sections and the appendix.
- **Each section has one job; don't repeat.** The Summary orients (2–3
  headlines), Main findings carries the full set, the detail sections hold the
  tables, the appendix holds method. Don't restate a finding across sections —
  cross-reference by section name instead.
- **Keep the recommendation with its finding.** The "to confirm" question lives on
  the finding it concerns, not in a separate distant section.
- **Print only what is true for this comparison.** The report carries
  case-specific, data-backed content. Generic "how this works" explanation, and
  the rules for choosing between alternative statements, live here in the skill —
  not copied into every report. Where the template offers conditional text (e.g.
  the attribution statement), print the one that applies and delete the rest.
- **Trim headings and hedges.** No unnecessary parentheticals or qualifiers in a
  heading; prefer fewer words. (A scope qualifier that tells the reader what is or
  isn't covered, like "(Illumina only)", is fine.)
- **Enumerate with lists.** When listing two or more items, use a markdown list,
  not inline "(1) … (2) …" prose.

## When to use

- A release candidate exists on `dev` and you want to vet its DOWNSTREAM output
  against `main` before merging.
- The user has two DOWNSTREAM output trees (or `s3://` URIs) to compare.
- The user references "downstream comparison", "release diff", "regression
  check", or similar.

If the user only wants raw numbers (no written review), run the script and point
them at the output directory; do not write `REVIEW.md`.

## Inputs

- `candidate` (required): the candidate run's DOWNSTREAM output root (the parent
  of `results_downstream/`), `s3://` URI or local path → `--dev`.
- `reference` (required): the reference run's DOWNSTREAM output root → `--main`.
- `candidate_index` (required for the viral-assignment analysis): the candidate
  run's index root (`s3://nao-mgs-index/<DATE>`), for taxonomy + vertebrate
  annotation → `--index`.
- `reference_index` (optional): the reference run's index root → `--old-index`.
  Used for the vertebrate-status-flip side-table (taxa whose vertebrate annotation
  changed between the two indexes). Clade rank and names come from the candidate
  index; a taxon deleted from the candidate-index taxonomy drops from the
  clade-share table.
- `out_dir` (required): absolute path for tables and the report → `--out`.

If a required input is missing, ask; do not guess. Without `--index`, the
viral-assignment analysis is skipped and the report must say so — never fabricate
it.

## Procedure

### Step 1 - Run the script

```bash
python bin/compare_downstream_runs.py \
  --main  <main-downstream-output-root> \
  --dev   <dev-downstream-output-root> \
  --index <dev-index-root> \
  --old-index <main-index-root> \
  --out   <output-dir>
```

Use an **absolute** path for `--out`. The script stages each run's
`results_downstream/` tree and the candidate index's `taxonomy-nodes.dmp` +
`total-virus-db-annotated.tsv.gz` under `<out>/_staged` and `<out>/_index`. It
takes a couple of minutes (most of it staging + taxonomy parsing).

Flag thresholds are tunable via `--thresholds '{"bray_curtis": 0.2, ...}'`;
defaults are documented in `bin/downstream_metrics.py` (`DEFAULT_THRESHOLDS`).

### Step 2 - Read the outputs

The script writes these TSVs to `--out`:

- `flags.tsv` — consolidated flags (focus, key, metric, value, threshold,
  flag_type). Read this first; it drives the Flags appendix, the Summary, and
  which Main-findings subsections must appear.
- `file_inventory.tsv`, `column_conformance.tsv` — Completeness and schema /
  Output-file overview.
- `skipped_groups.tsv` — any group excluded from a metric because a required
  input was present on only one side (empty if none); surface it in Completeness
  and schema.
- `viral_read_status.tsv` — per group × scope (all|vertebrate) read-status
  counts, with pct_lost (/main), pct_gained (/dev), pct_reassigned (/shared) —
  note the different denominators (Lost / gained / reassigned section).
- `viral_reassignment_concentration.tsv` — per group: distinct (taxid_main,
  taxid_dev) pairs and the top pair's share, so a high reassignment % driven by
  one systematic remap is visible.
- `viral_reassignment_buckets.tsv` — divergence-bucket counts (all buckets,
  incl. zero and unresolved-taxid) for the Reassignment-severity section.
- `viral_reassignment_pairs.tsv` — per (group, scope, taxid_main, taxid_dev,
  bucket) read counts, so cross-root / shared-higher-taxon example pairs can be
  named even when they are not a group's top pair.
- `clade_rank_shares.tsv` — per family/order: raw read counts (reads_main,
  reads_dev, delta_reads) plus each clade's share of the group's total viral
  reads (share_main, share_dev, delta_pp), main vs dev (Clade-share section).
  Note: `share_*` are fractions (0–1) but `delta_pp` is already in percentage
  points; and the clade-share flag is computed on the `reads_clade_total` basis
  only (not `reads_clade_dedup`), so flag counts reconcile against the
  `reads_clade_total` rows.
- `viral_validation_agreement.tsv` — BLAST-validation agreement.
- `vertebrate_status_flips.tsv` — taxa whose vertebrate status flipped.
- `kraken_bray_curtis.tsv`, `kraken_top_movers.tsv` — Kraken abundances.
- `qc_survival.tsv` — raw->cleaned read-survival fraction per side + delta
  (Quality metrics).
- `qc_numeric.tsv`, `qc_flag_changes.tsv` — Quality metrics.

### Step 3 - Fill in the template

Copy `review-template.md` to `<out>/REVIEW.md` and fill it in, following the
template's per-section instructions literally. Embed the actual tables/numbers
from the TSVs (large tables: show the top rows the template asks for and state
the total count). State the thresholds used in the Flags appendix. The template's
examples, illustrative shapes, and candidate-dimension lists are format guides,
not expected results — report only what this comparison's TSVs show, and don't
carry an example's taxa, directions, or counts into the report (the template's
"How to fill this in" block states this rule in full). Follow the
report-writing principles above throughout.

The report leads with a short Summary (scope + the 2–3 broadest differences),
then **Main findings** — one subsection per metric dimension that produced a flag,
each ending in a `**To confirm:**` line — then **Checked, no action needed** for
the dimensions that stayed within threshold. There is no separate recommendations
section; the per-finding `To confirm:` lines are the recommendations.

Key reminders:

- **Missing-data rule.** If an input needed for a metric is absent, say so in
  that section and move on — never fabricate or mis-compute. (E.g. no `--index`
  → the viral-assignment analysis is "not computed"; empty bracken → note it,
  don't invent abundances.)
- **Platform split.** Report Illumina and ONT separately under each dimension;
  ONT has no clade counts or duplicate marking — note the omission rather than
  leaving a blank.
- **Coverage is deterministic.** Every metric dimension with a flag (and any
  other difference large enough that a human should see it — e.g. a clade that
  appears or disappears, a cross-root reassignment) gets its own Main-findings
  subsection; never drop one as "minor". Dimensions checked but within threshold
  go under "Checked, no action needed". Annotate each finding with its breadth and
  magnitude rather than a fixed concern level; let the human prioritize. What is
  guaranteed identical across reviewers is the *set* of subsections and To-confirm
  lines (it is fixed by `flags.tsv`); their wording and grouping are author
  judgment, not a reproducible string.

### Step 4 - Optionally investigate likely drivers

The script flags differences but does not explain them. For whichever findings
this comparison surfaces, a short by-hand investigation often pins down the
likely mechanism cheaply, and is worth doing when a human will act on the report.
This stays **hypothesis-only** (there is no ground truth) and goes in the
report's optional "Likely drivers" section, kept separate from the deterministic
findings.

Cheap, high-yield query patterns, each conditional on observing the difference it
addresses (run against the staged data under `<out>/_staged` and the index dumps
under `<out>/_index` / `<out>/_old_index`):

- **A clade collapsed, or reads were gained/lost** → check whether reference
  genomes changed. Count alignments per reference accession on each side (in the
  `*_validation_hits.tsv.gz` files): an accession with many hits on one side and
  zero on the other points to a reference added to / removed from the aligner DB,
  not a code change. Before attributing a collapse to taxonomy re-ranking or
  taxon deletion instead, verify it: look the clade's taxid up in the candidate index's
  `taxonomy-nodes.dmp`. A clade that still appears in `clade_rank_shares.tsv` is
  by construction present in the candidate-index taxonomy, so re-ranking/deletion is already
  excluded for it — do not assert that explanation without confirming the taxid
  is genuinely absent.
- **A reassignment pair dominates** → look up both taxids in the candidate index's
  `taxonomy-nodes.dmp`. If one is the direct parent of the other (same species),
  it is an LCA-specificity move — the mildest reassignment, not a renumbering.
- **Gained reads entered the vertebrate subset** → join the gained reads' taxids
  against `vertebrate_status_flips.tsv` to see what fraction is explained by an
  annotation flip rather than a new detection.
- **BLAST agreement dropped** → tabulate `(aligner_taxid_lca,
  validation_staxid_lca)` pairs for the affected group; a single recurring offset
  (e.g. the aligner call one edge below a restructured parent taxon) localizes
  the drop to a taxonomy change.

Record, per finding: the suspected mechanism in one sentence, then the concrete
evidence (named taxa with taxids, accessions, counts) and the one-line query
that produced it. Frame every conclusion as a hypothesis. Skip this step if no
finding warrants it.

### Step 5 - Review and iterate

Re-read `REVIEW.md` for clarity and accuracy against the TSVs (a sub-agent is
useful here). Correct any number that doesn't trace back to an output, any
causal claim that slipped into the deterministic findings, and any flag that
lacks its underlying numbers.

### Step 6 - Hand off

Print the `REVIEW.md` path to the user. Optionally copy the report and tables to
`s3://sb-det-agent-scratch-general/...` for durability. Do **not** open a PR or
commit `REVIEW.md`/tables — the recommendations need human judgment, and the
contents are AWS-derived data.

## Glossary

**Read-status categories** (viral-assignment analysis, per shared read; joined on
`(group, sample, seq_id)` when a `sample` column is present, else `(group, seq_id)`):
- `same` — present both sides, same `aligner_taxid_lca`.
- `reassigned` — present both sides, different `aligner_taxid_lca`.
- `lost` — in main only (no longer a viral hit in dev).
- `gained` — in dev only.

**Divergence buckets** (reassignment severity, against the candidate-index taxonomy):
- `identical` — equal taxids (not counted as reassigned).
- `same-<rank>` — lowest standard rank (species…superkingdom) at which the two
  assignments still share an ancestor.
- `shared-higher-taxon` — share an ancestor only above the standard ranks (e.g.
  both under `Viruses` but different realms, or one reassigned up to `Viruses`).
- `cross-root` — share only the tree root (e.g. a viral read reassigned to a
  cellular organism). The most severe *biological* reassignment.
- `unresolved-taxid` — one of the taxids is absent from the candidate-index taxonomy
  (merged/deleted across index versions). A versioning artifact of unknown
  biological severity, **not** part of the same-species→cross-root gradient;
  assess it separately rather than treating it as more severe than cross-root.

**Bray-Curtis** (kraken analysis) — total variation distance between two relative-
abundance vectors at a rank; 0 = identical profiles, 1 = disjoint.

**Flag type** — flags are `fixed`: a metric value exceeds a documented absolute
threshold. Defaults are in `DEFAULT_THRESHOLDS` in `bin/downstream_metrics.py`
and are tunable via `--thresholds`.
