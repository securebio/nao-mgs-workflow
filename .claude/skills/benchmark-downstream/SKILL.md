---
name: benchmark-downstream
description: Compare the DOWNSTREAM output of two pipeline runs - each a (pipeline version, index) pair, typically a release candidate vs current production - and flag large differences for human review. Use to vet viral assignments, kraken abundances, and QC metrics before adopting a candidate.
---

# Compare DOWNSTREAM output

This skill compares a candidate DOWNSTREAM run with a reference run. The common
case is `dev` plus its index against `main` plus its index, but any two
**(pipeline version, index) pairs** work.

`bin/compare_downstream_runs.py` handles staging and I/O;
`bin/downstream_metrics.py` contains the calculations. The tool flags differences
for human review. It has no ground truth and must not label a difference good,
bad, correct, or regressive.

**Do not commit `REVIEW.md`, generated tables, or real sample names/taxids.** They
derive from AWS data. Keep them local or in the agent scratch bucket.

## Inputs

- `candidate`: candidate DOWNSTREAM output root, passed as `--candidate`.
- `reference`: reference output root, passed as `--reference`.
- `candidate_index`: candidate index root, passed as `--candidate-index`; required
  for viral-assignment analyses.
- `reference_index`: optional reference index root, passed as
  `--reference-index`; enables vertebrate-status changes.
- `out_dir`: absolute output directory, passed as `--out`.
- `candidate_version` / `reference_version`: optional overrides when version
  auto-detection returns `unknown`.

If a required input is missing, ask rather than guessing. Without a candidate
index, run the non-viral comparisons and state that the viral analysis was not
computed.

## Run The Comparison

```bash
python bin/compare_downstream_runs.py \
  --reference <reference-output-root> \
  --candidate <candidate-output-root> \
  --candidate-index <candidate-index-root> \
  --reference-index <reference-index-root> \
  --out <absolute-output-dir>
```

The script accepts local paths or `s3://` roots and stages data under the output
directory. If `run_identity.tsv` reports an `unknown` pipeline version, read the
matching RUN output's `logging/pyproject.toml` and rerun with
`--candidate-version` or `--reference-version`.

The report must name taxa. Fetch the candidate index's `taxonomy-names.dmp` into
`<out>/_index/` when a generated table has taxids without names. Always copy a
name and taxid from the same data row; never supply an id from memory.

Default flag thresholds are in `DEFAULT_THRESHOLDS`; override them with
`--thresholds '<json>'` only when the user requests different thresholds.

## Read The Outputs

Read `findings.tsv` first: it is the required-coverage manifest. Each row is one
required Main finding, already enumerated for you — threshold flags plus the
non-threshold triggers (clades reaching zero candidate share, severe
reassignments, output/schema anomalies, skipped groups). Walk it rather than
re-scanning the other tables to decide what to cover. Its columns:

- `finding_type`: the dimension; write one Main-finding subsection per distinct
  value present.
- `trigger`: `threshold` or the rule that surfaced it (`reaches_zero`,
  `shared-higher-taxon`, `cross-root`, `fastqc_worsening`, `missing_file`,
  `missing_both_sides`, `platform_mismatch`, `row_count_collapse`,
  `column_mismatch`, `one_sided_input`).
- `group`, `scope`, `rank`, `entity_taxid`, `entity_name`: the named subject,
  copied from a source row — use these names/ids directly.
- `value`, `threshold`, `direction`, `rank_in_type`: the magnitude, the threshold
  it crossed, which way it moved, and its size rank within the finding_type (lead
  each subsection with `rank_in_type == 1`).
- `detail_source`: the TSV (and filter) holding that finding's drivers and caveats.

`findings_summary.tsv` has one row per `finding_type` with the aggregates a
subsection's topic sentence needs — `n_findings`, `n_distinct_groups` (the
group count to cite; it dedupes the family/order double-listing),
`n_distinct_groups_over_threshold` (the "M of N exceeded the threshold"
sub-count), and `value_min`/`value_max` (the magnitude range). Cite these rather
than counting `findings.tsv` rows by hand.

`bounding_numbers.tsv` drives the **Checked** section: one row per checked metric
with its largest deviation (`max_abs_value`), where it occurred (`max_abs_group`),
the `threshold`, and `n_flagged`. `flags.tsv` is retained as a flat threshold list.

Then read the detail tables a finding's `detail_source` points to:

- `run_identity.tsv`: roots, indexes, and pipeline versions.
- `file_inventory.tsv`, `column_conformance.tsv`, `skipped_groups.tsv`: output
  completeness, row-count changes, and schema checks.
- `viral_read_status.tsv`: lost, gained, and reassigned read counts for all viral
  reads and the vertebrate subset. Percent lost uses the reference count;
  percent gained the candidate count; percent reassigned the shared count. The
  `dominant_gained_*` / `dominant_lost_*` columns name the single taxon driving a
  group's gain/loss and its read count — cite these for a concentrated turnover.
- `viral_reassignment_buckets.tsv`: reassignment counts by taxonomic divergence.
- `viral_reassignment_pairs.tsv`: every group/scope/taxid pair with divergence
  bucket, read count, and fraction. `is_dominant` marks each group's headline
  remap; `is_severe` marks cross-root / shared-higher-taxon pairs.
- `clade_rank_shares.tsv`: family/order raw counts and shares of total viral reads,
  for total and deduplicated counts. Total-count rows drive flags; deduplicated
  rows show whether a shift persists after duplicate removal; `reaches_zero` marks
  a clade gone from the candidate.
- `viral_validation_agreement.tsv` and
  `viral_validation_agreement_by_taxon.tsv`: group and per-taxon BLAST agreement;
  use `mean_distance_disagree` for disagreement distance and `is_agreement_driver`
  to name the taxon behind a group's agreement change.
- `vertebrate_status_flips.tsv`: true annotation changes
  (`gained_vertebrate`/`lost_vertebrate`) versus taxa present on only one side
  (`added_vertebrate`/`removed_vertebrate`).
- `kraken_bray_curtis.tsv`, `kraken_top_movers.tsv`: whole-community abundance
  shifts and the taxa driving them; the dominant mover per group/rank/read-set is
  `mover_rank == 1` (ranked by `abs_delta_pp`, so a 0→present jump is not missed).
- `qc_survival.tsv`, `qc_numeric.tsv`, `qc_flag_changes.tsv`: QC changes.

The TSVs are the drill-down artifact. Do not reproduce their full tables in
`REVIEW.md`.

## Write The Report

Copy `review-template.md` to `<out>/REVIEW.md` and replace every placeholder.
The report ends after these sections:

1. **Run identity**
2. **Summary**
3. **Main findings**
4. **Checked, no action needed**

Do not add detailed-investigation or appendix sections. They duplicate the TSVs,
lengthen review, and encourage findings to be restated. Put only the caveat needed
to interpret a finding beside that finding.

### Writing Rules

- **Results first.** Lead with what changed, then supporting numbers.
- **Name every entity.** Give taxa as `<name> (<rank>, taxid <id>)`, both sides of
  every reassignment, and exact group counts. Avoid "many groups" or "a species."
- **Plain language.** Do not write "Focus 1," "bucket triggered," or section
  numbers in reader-facing prose.
- **No repetition.** Summary gives 2-3 headlines; Main findings gives the full
  actionable set; stable dimensions appear once under Checked.
- **Separate platforms.** Report Illumina and ONT counts independently where
  their available outputs differ.
- **Report missing data.** "Not computed" is different from "no change."
- **Keep conclusions neutral.** A mechanism is a hypothesis, not a verdict.

### Attribution

Record which of pipeline code, index, and QC parameters differ. Pipeline version
and index are in `run_identity.tsv`; QC parameters are not emitted, so treat them
as **not confirmed unchanged** unless the user confirms otherwise.

- More than one dimension differs, or any dimension is unconfirmed: differences
  cannot be attributed to one cause.
- Exactly one differs and the other two are confirmed unchanged: attribution to
  that dimension is more direct.

### Finding Coverage

Write one Main-finding subsection per distinct `finding_type` in `findings.tsv`,
grouping that type's rows into it (lead with `rank_in_type == 1`). Every required
finding is already a row there — including the sub-threshold triggers
(`reaches_zero`, `shared-higher-taxon`/`cross-root`, output/schema anomalies,
skipped groups) — so do not re-derive the coverage list by hand. Minor instances
of one type (e.g. several small clades reaching zero) may share one concise
subsection. Any `finding_type` absent from the manifest belongs under Checked, no
action needed. Each finding ends with a specific `**To confirm:**` question, or a
short `**Note:**` when there is no concrete action.

For each finding include only the decision-relevant fields, reading them from the
row's `detail_source`:

- **Lost/gained/reassigned reads:** affected groups by platform, rate range,
  highest group. When concentrated, name the driver from `dominant_gained_*` /
  `dominant_lost_*` (read status) or the `is_dominant` pair (reassignment pairs).
  Remember that a high gained fraction can occur without net growth.
- **Reassignment severity:** the `is_severe` pairs (cross-root/shared-higher),
  named with counts; report `unresolved-taxid` separately because it is a
  versioning artifact, not a biological severity level.
- **Clade shares:** family/order, raw read change, share change, number of flagged
  groups, and number reaching zero. Use deduplicated shares to check whether a
  total-count shift is duplicate-driven. Family and parent-order flags can
  describe one underlying event; do not imply they are distinct.
- **BLAST agreement:** agreement and validated fractions together, then the
  `is_agreement_driver` taxon and `mean_distance_disagree`.
- **Kraken:** flagged group/rank/read-set rows and the `mover_rank == 1` movers
  (by `abs_delta_pp`), named.
- **QC/schema:** only anomalous dimensions go in Main findings; otherwise give the
  `bounding_numbers.tsv` figure under Checked.

## Check Mechanisms

Run the matching cheap check before naming a cause. Two general rules apply:

1. Do not carry a cause from one metric to another without recomputing its
   contribution inside the second metric's subset and denominator.
2. For a relationship metric, determine which side moved. Localization alone is
   not causation.

Use these checks:

- **Clade reaches zero:** check `vertebrate_status_flips.tsv` first.
  `lost_vertebrate` means the taxon remains in both indexes but was re-annotated
  out of the vertebrate-scoped output. `removed_vertebrate` means it is absent
  from the candidate annotated DB. Only investigate reference accessions after
  ruling those out. Read raw counts beside share changes so denominator effects
  are not mistaken for clade loss.
- **Vertebrate reads lost:** this subset excludes taxa re-annotated
  non-vertebrate. Compare all-scope and vertebrate-scope lost counts, then inspect
  the taxa in the residual vertebrate loss.
- **Dominant reassignment pair:** check both taxids in the candidate taxonomy. A
  child-to-parent move is loss of specificity, not taxid renumbering.
- **Vertebrate reads gained:** join gained taxids to status changes. An
  `added_vertebrate` taxon is consistent with a new genome, but taxids are compared
  as-is, so confirm it is genuinely new before calling that strongly supported.
- **BLAST agreement changes:** use the per-taxon table, then join affected reads
  across runs to determine whether the aligner assignment or validation target
  moved. Agreement loss on unchanged aligner calls is not caused by reassignment.
- **Kraken shifts:** a database-update explanation is speculative unless
  per-taxon reference membership was checked.

When supported, add a short `**Likely mechanism:**` clause with concrete evidence
and one confidence label:

- **Strongly supported:** a deterministic check directly accounts for the change.
- **Consistent:** evidence fits and explains much of the change but leaves an
  alternative.
- **Speculative:** plausible only; no targeted check confirmed it.

If no evidence supports a mechanism, omit the clause. Confidence rates the causal
evidence, not the severity of the finding.

## Final Check

Before handoff:

- Every number in the report must come from a TSV cell, not mental arithmetic.
  Taxids and names come from a source row; per-`finding_type` counts and ranges
  (group tallies, "M of N over threshold", magnitude ranges) come from
  `findings_summary.tsv`; bounding figures from `bounding_numbers.tsv`. For any
  count or range not already in a summary cell (e.g. a sub-tally over a custom
  cutoff, or a sum of reads), re-derive it explicitly from the relevant TSV
  (filter and count/sum) — do not eyeball it off a table. Recompute, do not
  estimate.
- Confirm every `finding_type` in `findings.tsv` appears as exactly one Main
  subsection, and each manifest row is covered by its subsection.
- Confirm stable bullets cite a `bounding_numbers.tsv` figure and skipped analyses
  say "not computed."
- Remove all template instructions and placeholders.
- Re-read every cross-metric attribution and relationship metric for the two
  mechanism traps above.

Print the local `REVIEW.md` path. Optionally copy the output directory to
`s3://sb-det-agent-scratch-general/...`. Do not commit the report or tables.
