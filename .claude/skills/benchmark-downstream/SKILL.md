---
name: benchmark-downstream
description: Compare the DOWNSTREAM output of two pipeline runs (dev vs main) before a release and flag large differences for human review. Use to vet a release candidate's viral assignments, kraken abundances, and QC metrics against the current main output.
---

# Compare DOWNSTREAM output before a release

Before promoting `dev` to `main`, this skill diffs the DOWNSTREAM output of the
two runs across both platforms and flags large differences for a human to
adjudicate. `bin/compare_downstream_runs.py` does the deterministic data
extraction (munging + I/O); the per-metric calculations live in
`bin/downstream_metrics.py` so they can be reviewed and tested separately. You
fill in `review-template.md` from the script's TSV outputs.

**This is a holistic release diff, not a causal analysis.** main and dev
usually differ in code AND reference index AND QC parameters at once, so a
difference cannot be pinned on any single cause. There is no ground truth, so
no difference is "good" or "bad" on its face — the report's job is to surface
and flag large changes for human review, naming a likely driver only as a
hypothesis.

**`review-template.md` is the source of truth for report structure** — open it
and fill it in literally. `REVIEW.md` must stand alone: embed the needed tables
and numbers rather than pointing at output files.

**Do NOT commit `REVIEW.md` or any extracted tables, or paste real sample
names / taxids / abundances into a PR.** They originate from AWS data; keep
them local and (optionally) in the agent scratch bucket. The PR contains only
the skill, scripts, tests, and docs.

## When to use

- A release candidate exists on `dev` and you want to vet its DOWNSTREAM output
  against `main` before merging.
- The user has two DOWNSTREAM output trees (or `s3://` URIs) to compare.
- The user references "downstream comparison", "release diff", "regression
  check", or similar.

If the user only wants raw numbers (no written review), run the script and point
them at the output directory; do not write `REVIEW.md`.

## Inputs

- `dev` (required): candidate DOWNSTREAM output root (the parent of
  `results_downstream/`), `s3://` URI or local path → `--dev`.
- `main` (required): reference DOWNSTREAM output root → `--main`.
- `dev_index` (required for Focus 1): the dev run's index root
  (`s3://nao-mgs-index/<DATE>`), for taxonomy + vertebrate annotation → `--index`.
- `main_index` (optional): the main run's index root, only for the
  vertebrate-status-flip side-table → `--old-index`.
- `out_dir` (required): absolute path for tables and the report → `--out`.

If a required input is missing, ask; do not guess. Without `--index`, Focus 1
(viral assignments) is skipped and the report must say so — never fabricate it.

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
`results_downstream/` tree and the dev index's `taxonomy-nodes.dmp` +
`total-virus-db-annotated.tsv.gz` under `<out>/_staged` and `<out>/_index`. It
takes a couple of minutes (most of it staging + taxonomy parsing).

Flag thresholds are tunable via `--thresholds '{"bray_curtis": 0.2, ...}'`;
defaults are documented in `bin/downstream_metrics.py` (`DEFAULT_THRESHOLDS`).

### Step 2 - Read the outputs

The script writes these TSVs to `--out`:

- `flags.tsv` — consolidated flags (focus, key, metric, value, threshold,
  flag_type). Read this first; it drives the §Flags section and Summary.
- `file_inventory.tsv`, `column_conformance.tsv` — Focus 4 / §0.
- `viral_read_status.tsv` — per group × scope (all|vertebrate) read-status
  counts (§1.1).
- `viral_reassignment_buckets.tsv`, `viral_reassignment_detail.tsv` —
  divergence-bucket counts and per-read detail (§1.2).
- `clade_rank_shares.tsv` — family/order shares main vs dev (§1.3).
- `viral_validation_agreement.tsv` — BLAST agreement (§1.4).
- `vertebrate_status_flips.tsv` — taxa whose vertebrate status flipped (§1.5).
- `kraken_bray_curtis.tsv`, `kraken_top_movers.tsv` — Focus 2 / §2.
- `qc_numeric.tsv`, `qc_flag_changes.tsv` — Focus 3 / §3.

### Step 3 - Fill in the template

Copy `review-template.md` to `<out>/REVIEW.md` and fill it in, following the
template's per-section instructions literally. Embed the actual tables/numbers
from the TSVs (large tables: show the top rows the template asks for and state
the total count). State the thresholds used in §Flags.

Key reminders:

- **Missing-data rule.** If an input needed for a metric is absent, say so in
  that section and move on — never fabricate or mis-compute. (E.g. no `--index`
  → §1 is "not computed"; empty bracken → note it, don't invent abundances.)
- **Platform split.** Report Illumina and ONT separately under each focus; ONT
  has no clade counts or duplicate marking — note the omission rather than
  leaving a blank.
- **Err toward inclusion** in §Recommendations: every large or anomalous
  difference (any consolidated flag, any whole-family share collapse, any
  cross-root reassignment cluster) should appear as something for a human to
  review, even at low concern.

### Step 4 - Review and iterate

Re-read `REVIEW.md` for clarity and accuracy against the TSVs (a sub-agent is
useful here). Correct any number that doesn't trace back to an output, any
causal claim that slipped in, and any flag that lacks its underlying numbers.

### Step 5 - Hand off

Print the `REVIEW.md` path to the user. Optionally copy the report and tables to
`s3://sb-det-agent-scratch-general/...` for durability. Do **not** open a PR or
commit `REVIEW.md`/tables — the recommendations need human judgment, and the
contents are AWS-derived data.

## Glossary

**Read-status categories** (Focus 1, per shared `(group, seq_id)`):
- `same` — present both sides, same `aligner_taxid_lca`.
- `reassigned` — present both sides, different `aligner_taxid_lca`.
- `lost` — in main only (no longer a viral hit in dev).
- `gained` — in dev only.

**Divergence buckets** (reassignment severity, against the dev taxonomy):
- `identical` — equal taxids (not counted as reassigned).
- `same-<rank>` — lowest standard rank (species…superkingdom) at which the two
  assignments still share an ancestor.
- `shared-higher-taxon` — share an ancestor only above the standard ranks (e.g.
  both under `Viruses` but different realms, or one reassigned up to `Viruses`).
- `cross-root` — share only the tree root (e.g. a viral read reassigned to a
  cellular organism). The most severe.

**Bray-Curtis** (Focus 2) — total variation distance between two relative-
abundance vectors at a rank; 0 = identical profiles, 1 = disjoint.

**Flag types** — `fixed` (exceeds an absolute threshold), `cohort-outlier`
(a robust-MAD outlier versus sibling groups, with a magnitude floor so trivial
differences in near-constant cohorts are not flagged), or `fixed+cohort-outlier`.
