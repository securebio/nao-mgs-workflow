---
name: benchmark-downstream
description: Compare the DOWNSTREAM output of two pipeline runs — each a (pipeline version, index) pair, typically a release candidate vs current production — and flag large differences for human review. Use to vet a candidate's viral assignments, kraken abundances, and QC metrics against a reference run before adopting it.
---

# Compare DOWNSTREAM output of two runs

This skill diffs the DOWNSTREAM output of two pipeline runs across both platforms
and flags large differences for a human to adjudicate. Each run is a **(pipeline
version, index) pair**; the common case is a release diff (the candidate
+ its index vs the reference + its index, which the CLI labels `--candidate` /
`--reference`), but it works for any two version/index pairs — e.g. the same code
with only the index rebuilt. `bin/compare_downstream_runs.py` does the
deterministic data extraction
(munging + I/O); the per-metric calculations live in `bin/downstream_metrics.py`
so they can be reviewed and tested separately. You fill in `review-template.md`
from the script's TSV outputs.

**This is a difference-flagging diff, not a causal analysis.** There is no ground
truth, so no difference is "good" or "bad" on its face — the report surfaces and
flags large changes for human review, naming a likely driver only as a
hypothesis.

**Attribution.** How far a difference can be attributed depends on what differs
between the runs. Establish which of {pipeline code, index, QC
parameters} differ from the Run-identity inputs: the pipeline version and the
index identity (date/path) are given; QC parameters are **not** emitted by the
script, so unless the user states they are unchanged, treat that dimension as not
confirmed. Record the conclusion in Run identity, then print the single matching
attribution statement just below the Run-identity table (the template marks the
spot), deleting the others:
- **More than one** dimension differs, OR any dimension cannot be confirmed
  unchanged (the typical release diff) → a difference cannot be pinned on any
  single cause.
- **Exactly one** dimension differs and the others are confirmed unchanged (e.g.
  same code and same QC params, only the index rebuilt) → a difference is
  attributable to that dimension more directly.

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
  cross-reference by section name instead. The Summary's 2–3 headline lines are
  the one intended exception: a deliberate at-a-glance orientation, not a repeat.
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

- A release candidate exists and you want to vet its DOWNSTREAM output against the
  current reference run before merging.
- The user has two DOWNSTREAM output trees (or `s3://` URIs) to compare.
- The user references "downstream comparison", "release diff", "regression
  check", or similar.

If the user only wants raw numbers (no written review), run the script and point
them at the output directory; do not write `REVIEW.md`.

## Inputs

- `candidate` (required): the candidate run's DOWNSTREAM output root (the parent
  of `results_downstream/`), `s3://` URI or local path → `--candidate`.
- `reference` (required): the reference run's DOWNSTREAM output root → `--reference`.
- `candidate_index` (required for the viral-assignment analysis): the candidate
  run's index root (`s3://nao-mgs-index/<DATE>`), for taxonomy + vertebrate
  annotation → `--candidate-index`.
- `reference_index` (optional): the reference run's index root → `--reference-index`.
  Used for the vertebrate-status-flip side-table (taxa whose vertebrate annotation
  changed between the two indexes). Clade rank and names come from the candidate
  index; a taxon deleted from the candidate-index taxonomy drops from the
  clade-share table.
- `out_dir` (required): absolute path for tables and the report → `--out`.
- `candidate_version` / `reference_version` (optional): the pipeline version
  string for each run → `--candidate-version` / `--reference-version`. The script
  auto-detects the version from the run's `logging*/pyproject.toml`; supply these
  only when auto-detection comes back `unknown` (see Step 1).

If a required input is missing, ask; do not guess. Without `--candidate-index`,
the viral-assignment analysis is skipped and the report must say so — never
fabricate it.

## Procedure

### Step 1 - Run the script

```bash
python bin/compare_downstream_runs.py \
  --reference <reference-downstream-output-root> \
  --candidate <candidate-downstream-output-root> \
  --candidate-index <candidate-index-root> \
  --reference-index <reference-index-root> \
  --out   <output-dir>
```

Use an **absolute** path for `--out`. The script stages each run's
`results_downstream/` tree and the candidate index's `taxonomy-nodes.dmp` +
`total-virus-db-annotated.tsv.gz` under `<out>/_staged` and `<out>/_index`. It
takes a couple of minutes (most of it staging + taxonomy parsing).

**Pipeline versions.** The script writes `run_identity.tsv` with each run's exact
pipeline version, auto-detected from the run's `logging*/pyproject.toml`. The
DOWNSTREAM output root does **not** always carry that file (a DOWNSTREAM-only run
publishes only sentinels under `logging_downstream/`); when the version comes back
`unknown`, read it from the matching **RUN** output's `logging/pyproject.toml`
(its `[project] version`) and pass `--candidate-version` / `--reference-version`
so the report names the specific version (e.g. `3.2.2.0-dev`) rather than a branch
label. Re-run the script after supplying them so `run_identity.tsv` is correct.

The script does **not** stage `taxonomy-names.dmp`, but the report-writing
"name every entity" rule needs it (the reassignment TSVs carry only taxids). Fetch
it once from the candidate index alongside the staged dumps, e.g.
`aws s3 cp s3://nao-mgs-index/<DATE>/output/results/taxonomy-names.dmp <out>/_index/`,
and use it to resolve every taxid to a name.

Flag thresholds are tunable via `--thresholds '{"bray_curtis": 0.2, ...}'`;
defaults are documented in `bin/downstream_metrics.py` (`DEFAULT_THRESHOLDS`).

### Step 2 - Read the outputs

The script writes these TSVs to `--out`:

- `run_identity.tsv` — per side (reference/candidate): downstream_root, index_root,
  pipeline_version (auto-detected or from `--*-version`). Drives the Run-identity
  table; an `unknown` version is the cue to supply the override (Step 1).
- `flags.tsv` — consolidated flags (focus, key, metric, value, threshold,
  flag_type). Read this first; it drives the Flags appendix, the Summary, and
  which Main-findings subsections must appear.
- `file_inventory.tsv`, `column_conformance.tsv` — Completeness and schema /
  Output-file overview.
- `skipped_groups.tsv` — any group excluded from a metric because a required
  input was present on only one side (empty if none); surface it in Completeness
  and schema.
- `viral_read_status.tsv` — per group × scope (all|vertebrate) read-status
  counts, with pct_lost (/reference), pct_gained (/candidate), pct_reassigned
  (/shared) — note the different denominators (Lost / gained / reassigned section).
- `viral_reassignment_concentration.tsv` — per group: distinct (taxid_reference,
  taxid_candidate) pairs and the top pair's share, so a high reassignment % driven
  by one systematic remap is visible.
- `viral_reassignment_buckets.tsv` — divergence-bucket counts (all buckets,
  incl. zero and unresolved-taxid) for the Reassignment-severity section.
- `viral_reassignment_pairs.tsv` — per (group, scope, taxid_reference,
  taxid_candidate, bucket) read counts, so cross-root / shared-higher-taxon
  example pairs can be named even when they are not a group's top pair.
- `clade_rank_shares.tsv` — per family/order: raw read counts (reads_reference,
  reads_candidate, delta_reads) plus each clade's share of the group's total viral
  reads (share_reference, share_candidate, delta_pp), reference vs candidate
  (Clade-share section).
  Note: `share_*` are fractions (0–1) but `delta_pp` is already in percentage
  points; and the clade-share flag is computed on the `reads_clade_total` basis
  only (not `reads_clade_dedup`), so flag counts reconcile against the
  `reads_clade_total` rows.
- `viral_validation_agreement.tsv` — BLAST-validation agreement, per group.
- `viral_validation_agreement_by_taxon.tsv` — the same agreement broken down per
  (group, aligner taxon): per-side validated counts, agreement rate, mean
  validation distance, and `delta_agreement`. Use it to say which taxa drove a
  flagged group's agreement change and how far off the disagreements are.
- `vertebrate_status_flips.tsv` — taxa whose vertebrate membership changed, with
  `change` distinguishing a true re-annotation flip (`gained_vertebrate` /
  `lost_vertebrate`, taxon present in both index annotations) from a presence
  change (`added_vertebrate` / `removed_vertebrate`, taxon in only one).
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
then **Main findings** — one subsection per metric dimension that produced a flag —
then **Checked, no action needed** for the dimensions that stayed within
threshold. There is no separate recommendations section and no separate
"likely drivers" section: each finding ends with a brief reviewer-facing line
(the recommendation), and any suspected cause from a Step-4 cross-check is stated
inline within the finding it concerns as a calibrated `**Likely mechanism:**`
clause (see Step 4).

That closing line is usually a `**To confirm:**` question, but **do not
manufacture an action when there is nothing specific to confirm.** A finding that
is self-explanatory once stated (e.g. a new detection enabled by a genome added
to the index) can close with a one-line `**Note:**` that flags it for awareness
instead. Keep the line free of verdict words ("over-calling", "legitimate",
"wrong", "caused by").

Key reminders:

- **Missing-data rule.** If an input needed for a metric is absent, say so in
  that section and move on — never fabricate or mis-compute. (E.g. no
  `--candidate-index` → the viral-assignment analysis is "not computed"; empty
  bracken → note it, don't invent abundances.)
- **Platform split.** Report Illumina and ONT separately under each dimension;
  ONT has no clade counts or duplicate marking — note the omission rather than
  leaving a blank.
- **Coverage is deterministic.** A Main-findings subsection is required for (a)
  every metric dimension with a flag in `flags.tsv`, and (b) two enumerated
  non-flag triggers that a flag can miss: any cross-root or shared-higher-taxon
  reassignment (the reassignment flag is per-group, so a few severe moves inside
  an otherwise-unflagged group would slip past it), and any clade (family OR order)
  reaching zero candidate-side share (a collapse can sit below the share-change
  threshold). For that second trigger, enumerate **every** such clade from
  `clade_rank_shares.tsv`, however small the reference count — not just the large
  ones; a one-line list of the minor ones is enough, but they must not be silently
  dropped. Never drop a trigger as "minor"; a dimension checked but within threshold and with
  neither trigger goes under "Checked, no action needed". The *set* of subsections
  (each closing with a `**To confirm:**` or `**Note:**` line) is fixed by
  `flags.tsv` plus those two triggers — that is what is identical across reviewers;
  their wording and grouping are author judgment, not a reproducible string.
  Annotate each finding with breadth and
  magnitude rather than a fixed concern level; let the human prioritize.

### Step 4 - Run the standard mechanism cross-checks (fold into the findings)

The script flags differences but does not explain them. For each finding, run the
matching cheap cross-check below **before** writing it up, and fold the result
into that finding as a one-clause `**Likely mechanism:**` statement (see the
closing paragraph for how to phrase it) — there is **no** separate "likely
drivers" section. These checks are cheap, high yield, and keep a
plausible-but-wrong guess (e.g. "genomes removed from the aligner DB" when the
real cause is a re-annotation) from reaching the reader.
Run them against the staged data under `<out>/_staged` and the index dumps under
`<out>/_index` / `<out>/_reference_index`. Each is conditional on observing the
difference it addresses; skip a check whose difference did not occur.

**Two traps to avoid when assigning a mechanism — both are about not asserting a
cause the data doesn't actually pin down:**

- **Don't carry a cause across metrics without re-checking it in the second
  metric.** Each metric has its own subset, denominator, and matching rule (e.g.
  the read-level lost/gained/reassigned counts use the vertebrate-viral subset
  defined by *candidate-index* annotation; the clade-share view uses a different
  denominator; the all-scope counts include everything). A taxon or clade that
  drives one metric may be *excluded by construction* from another, so a cause
  established for finding A can be irrelevant to finding B even when they look
  related. Before writing "X explains flag B", confirm X's reads actually fall
  inside B's subset/denominator — recompute the contribution, don't infer it from
  a sibling finding.
- **When a signal is a relationship between two quantities, find out which side
  moved.** A metric defined as a relation (e.g. BLAST agreement = the aligner call
  vs. the BLAST/validation call; or any "A relative to B" rate) can shift because
  either side changed. Localizing *where* the shift sits (which taxon, which
  group) does not tell you *which side* moved. Determine that — e.g. join the
  affected reads across runs and check whether the aligner call actually changed —
  before naming a cause; a shift concentrated on *unchanged* aligner calls points
  to the other side (the validation reference) moving, not a reassignment.

- **A clade dropped out of the clade-share view** → **check
  `vertebrate_status_flips.tsv` first.** The DOWNSTREAM viral output is scoped to
  the index's vertebrate-infecting set, so a clade whose members were re-annotated
  *non*-vertebrate (they appear with `change == lost_vertebrate` — a TRUE status
  flip, present in both index annotations) drops out of the clade-share counts
  entirely. This is the most common benign cause of a clade vanishing and far more
  likely than a genome removal; it supports a **Strongly supported** mechanism for
  the *clade-share* finding when the clade's members all carry that flip. (A
  `removed_vertebrate` value instead means the taxon was dropped from the candidate
  annotated DB altogether — a different mechanism.) Only after ruling the flip out
  should you look at whether reference genomes changed: count alignments per
  reference accession on each side (`prim_align_genome_id_all` in the
  `*_validation_hits.tsv.gz` files); an accession with many hits on one side and
  zero on the other points to a reference added to / removed from the aligner DB. A
  clade that still appears in `clade_rank_shares.tsv` is by construction present in
  the candidate-index taxonomy, so do not attribute its drop to taxonomy
  re-ranking/deletion without confirming the taxid is genuinely absent from
  `taxonomy-nodes.dmp`.
- **Vertebrate-viral reads were lost (the lost-flag)** → this is a DIFFERENT
  metric from the clade-share collapse above; do not assume the same cause (first
  trap). The lost count is over the vertebrate-viral subset (candidate-index
  status 1, union rule), so a clade re-annotated *non*-vertebrate is **excluded
  from this metric** — its disappearance shows up in the all-scope and clade-share
  views but contributes **zero** to the vertebrate-scope lost count. Confirm by
  comparing the all-scope vs vertebrate-scope `n_lost` for the group in
  `viral_read_status.tsv`: the vertebrate-loss flag is the *residual* after the
  non-vertebrate (incl. re-annotated) losses are removed. Explain it from the taxa
  of the vertebrate-subset lost reads themselves (look them up in the staged
  reference `validation_hits` for that group), not from the re-annotated clade.
- **A reassignment pair dominates** → look up both taxids in the candidate index's
  `taxonomy-nodes.dmp`. If one is the direct parent of the other (same species),
  it is an LCA-specificity move — the mildest reassignment, not a renumbering.
- **Vertebrate-viral reads were gained** → join the gained reads' taxids against
  `vertebrate_status_flips.tsv` to see what fraction is a `gained_vertebrate` true
  re-annotation flip versus an `added_vertebrate` taxon present only in the
  candidate annotation, versus an existing taxon called more often (no flip row at
  all). Treat `added_vertebrate` as *consistent with* a genome newly added +
  annotated vertebrate-infecting — but not proof: taxid canonicalization is not
  applied (no `taxonomy-merged.dmp`), so a merged/renumbered taxid can surface as
  one `added_vertebrate` paired with a `removed_vertebrate` rather than a genuine
  addition. So this mechanism is at most **Consistent** unless you confirm the
  taxon is genuinely new (e.g. against the index build), not **Strongly
  supported**.
- **BLAST agreement moved in a group** → read that group's rows from
  `viral_validation_agreement_by_taxon.tsv` (the deterministic per-taxon
  breakdown): report the taxa with the largest validated-read counts and their
  per-side agreement rate and `delta_agreement`, and use `mean_distance_disagree`
  (mean taxonomic distance over only the *disagreeing* reads) to say how far off
  the new disagreements are — a value near 1 is a one-edge offset; a large value
  is a gross mis-call. Do **not** use `mean_distance` for this: it is over all
  validated reads and dilutes toward 0 when agreement is high. This localizes the
  move to a taxon — but agreement is a two-sided signal (aligner call vs BLAST
  call), so localization is **not** the cause (second trap). Before linking the
  drop to a reassignment finding, check **which side moved**: join the affected
  group's reads across runs and compare `aligner_taxid_lca`. If the bulk of the
  agreement loss sits on reads whose aligner call is *unchanged*, the BLAST/
  validation reference moved (e.g. it now resolves to a new or split species near
  the aligner's taxon), and a reassignment is **not** the cause — cap that link at
  **Consistent** (or drop it) rather than **Strongly supported**.

For each finding investigated, fold in a short `**Likely mechanism:**` clause —
the suspected cause plus the concrete evidence (named taxa with taxids, counts) —
and tag it with one of **three confidence levels**, chosen by what the cross-check
actually showed (not by how worried you are):

- **Strongly supported** — a deterministic cross-check directly accounts for the
  difference: the cause mechanistically entails the observed change and the link
  is named and checkable (e.g. a clade collapse where every member taxon flips
  `lost_vertebrate` and the output is vertebrate-scoped, so the family must drop
  out).
- **Consistent** — the cross-check is compatible with the difference and explains
  much of it, but does not account for all of it or does not exclude an
  alternative (e.g. a re-annotation that explains most lost reads but not a
  residual few).
- **Speculative** — no targeted cross-check confirmed it; only a plausible
  association or domain reasoning (e.g. new environmental taxa "consistent with a
  Kraken DB update", with no per-taxon check run).

Two rules: **(a)** cite the evidence that earns the level — "Strongly supported"
without a named, checkable link is not allowed; **(b)** if nothing supports a
mechanism, **omit the clause** — do not default to "Speculative" to fill the slot.
The level rates only how well the evidence supports the CAUSE; it is **not** a
severity/concern verdict on the finding (a strongly-supported mechanism can be
benign and a speculative one worrying — independent axes), and even "Strongly
supported" stays an inference, not a verdict, because there is no ground truth.
Attach the clause to the causal claim only — a deterministic number straight from
a TSV belongs in the finding's body, not under a mechanism tag. Skip a check whose
finding did not occur.

### Step 5 - Review and iterate

Re-read `REVIEW.md` for clarity and accuracy against the TSVs (a sub-agent is
useful here). Correct any number that doesn't trace back to an output, any
`Likely mechanism:` clause that overstates its confidence or isn't backed by a
cross-check, any deterministic observation mislabeled as a mechanism, and any flag
that lacks its underlying numbers. Specifically guard the two traps above:
- **Re-derive every count, total, and taxid you cite directly from the TSV** —
  group counts, read totals, per-pair counts, taxids — rather than transcribing
  from a sibling finding or from memory; an off-by-one group count or a wrong
  taxid is a common slip.
- **Check each cross-metric attribution**: where a finding for metric A is named
  as the cause of a flag in metric B, recompute the cause's contribution *within
  metric B's own subset/denominator* (e.g. confirm the reads are in the
  vertebrate-viral subset before blaming a vertebrate-loss flag on them), and
  confirm a relation-based signal's cause by checking which side actually moved.

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
- `lost` — in the reference only (no longer a viral hit in the candidate).
- `gained` — in the candidate only.

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
