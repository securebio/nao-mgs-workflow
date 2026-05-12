---
name: bench
description: Top-level entry point for benchmarking a perf PR. Routes to `bench-module` (single-process via local Docker) and/or `bench-workflow` (full pipeline via AWS Batch), then composes their outputs into the structured PR-description section used in this repo's perf-PR examples (`.claude/pr-examples/pipeline-bench.md`). The synthesis step is where the agent adds the cohort context, interpretation paragraphs, critical-path framing, and honest scope caveats that the underlying scripts don't produce on their own.
---

# Bench: from perf change to PR writeup

This skill is the canonical entry point for "I have a perf change; produce the bench section of the PR description." It dispatches to the right underlying tool(s) and composes their outputs into a reviewer-ready block matching the structure in `.claude/pr-examples/pipeline-bench.md` (the exemplar to mirror).

## Step 1 — pick the bench mode(s)

Match the perf claim to the right tool. Most PRs use one mode; some use both.

| Claim shape | Tool | What it gives you |
|---|---|---|
| A single module is faster | `bench-module` skill | Per-task wall/cpu-h on controlled inputs, ~minutes per cycle |
| The change preserves outputs | `bench-workflow` skill | Per-sample S3 hash diff with known-noise classification baked in |
| End-to-end cohort wall changes | `bench-workflow` skill | Production-realistic cohort wall + per-process trace breakdown |
| Both per-process precision and cohort realism | Both | Local for tight per-process Δ; workflow for production-scale validation |

**Worked patterns from past PRs:**

- **#774** (BBDuk → Nucleaze swap in one module): process-scoped local bench gave the headline `−72 % wall / −81 % cpu-h` on the k-mer screen alone; the subworkflow-level numbers were derived from a trace slice on a separate larger run; output equality wasn't binary (read content changes were the entire point), so the PR added a sensitivity table instead.
- **#775** (combining SUBSET_PAIRED + INTERLEAVE_FASTQ into one process): per-process numbers + per-subworkflow numbers + cohort-level numbers, all drawn from the same workflow bench by trace-slicing.

If you don't yet have bench artifacts, dispatch to the matching skill (`bench-module` and/or `bench-workflow`) first. Each emits a JSON payload and a markdown summary at known paths; this skill takes those as input.

## Step 2 — locate the bench artifacts

**`bench-module-local`** agent output:
- Returns inline a Target-module callout, a Notes paragraph, the markdown from `summary.md`, and the JSON.
- The agent's `OUT_DIR` is reported in its return; trace files at `$OUT_DIR/{branch_a,branch_b}/trace.tsv`.

**`bench-workflow-batch`** agent output (per branch):
- Returns inline `trace_path:` and `results_prefix:` lines plus an optional Notes paragraph.
- The `bench-workflow` skill that fans these out aggregates into `<run_dir>/trace-comparison.{md,json}` and `<run_dir>/output-equality.{md,json}`.

## Step 3 — compose the PR description

Follow `.claude/pr-examples/pipeline-bench.md` for top-level structure. The synthesis breaks down as:

### `# Summary`

Two or three short paragraphs. What the change is, what problem it solves, the key design decision. Lead with the change and motivation, *not* with numbers — let the bench section do that work.

### `# Backwards compatibility`

Only when the change might affect downstream results. Use the output-equality block from `bench-workflow`:

- **`N/N byte-identical`** is the goal. If hit, one sentence and move on.
- **`M/N byte-identical, K diff_known_noise`** — name the known-noise files (Kraken HLL, FastQC `percent_duplicates`) and explain why the drift is expected. Don't bury this.
- **Any `diff_unexpected`** — investigate before the PR ships. Don't paper over a real result change with bench numbers.

For sensitivity-changing PRs (e.g. #774), this section becomes a sensitivity analysis instead — per-sample hits-lost table, per-genus impact, an honest "is this losing real signal or borderline filter-outs?" paragraph.

### `# Benchmarking`

Start with **setup** in prose: cohort name, sample count, host configuration, whether `process.maxForks = 1` was used (matters for clean local wall numbers), whether index/inputs were pre-staged. Without this context the numbers don't transfer.

Then per-scope tables, ordered narrow → broad:

1. **Per-process** (from `parse_bench_trace.py`'s output) — the table the perf claim is *about*. Lift verbatim from the script's markdown output. For a single-process PR, trim to that process plus any immediate upstream context. For a bundled multi-PR cohort comparison, take the top ~12 rows by `max(branch_cpu_h)` — that surfaces every meaningful effect without burying the reader in single-cpu-second rows.
2. **Per-subworkflow / per-cohort** — when the change might affect multiple processes, add the broader aggregation. The `bench-workflow` trace gives this directly when sliced by subworkflow prefix (e.g. `RUN:EXTRACT_VIRAL_READS_SHORT:*`).

After each table, write a short interpretation paragraph. Reviewer's eye lands on a table; the paragraph tells them what to make of it. Useful patterns from #774:

- "**−63 %** aggregate cpu-hours saving comes almost entirely from the kmer step itself" — identifies which row in the table is doing the work.
- "Wall savings are remarkably consistent across samples (70–75 % on every one). The match-count drop varies more (−14 % to −77 % per sample)" — pairs aggregate with per-sample variance.
- "BOWTIE2_VIRUS aggregates +28 s wall on PR despite a smaller input, but per-sample variance is large… with N=4 this could just be sample-level noise" — honestly flags noise vs signal.

## Step 4 — what the synthesis must add

The scripts (`parse_bench_trace.py`, `bench_output_equality.py`) emit tables and structured JSON. They don't produce the *interpretation* parts. The agent's synthesis adds:

- **Cohort context** at the top of `# Benchmarking` — name the cohort, sample count, host, `maxForks` setting, whether inputs were pre-staged.
- **Construction caveats** from each bench's `Notes:` — if local-bench used synthesis or a production-fidelity upstream chain, a reviewer needs to know whether the bench's input content was representative.
- **Critical-path framing** — cross-reference issue #785 / `project_critical_path_illumina`. If the per-process Δ is on the workflow critical path, say it shortens cohort wall. If off the critical path, say it reduces cpu-hours but not time-to-results. Don't conflate the two; this is a recurring reviewer gotcha. The signature pattern: cohort wall flat + cpu-hours dropped means most wins are off the critical path.
- **Variance caveats** — SPOT preemption noise on Batch is real; parallel `bench-workflow-batch` invocations trade some wall-time variance for parallelism. Note in prose when an Δ is "within noise."
- **Honest scope** — if the cohort isn't production-scale, say so. If the local bench used a partial chain that confounds Δ with upstream changes, flag it. If results pass output-equality on tiny-test but the production cohort wasn't tested, say that too.

## Step 5 — investigate apparent regressions before reporting them

A perf PR's bench table almost always has at least one row trending the "wrong" way. Before writing a regression into the prose, run this checklist:

1. **Per-task spread.** Pull `max_realtime_s` vs `sum_realtime_s/n` from the trace JSON (see "JSON role keys" in the Notes section below for the column conventions). If `max ≫ mean`, the regression is a single slow task dragging the aggregate (typical signature: cold container pull, slow SPOT node, noisy neighbor). Sum cpu-h on Batch is contention-immune in aggregate but a single slow task still bumps the total.
2. **Did the branch actually touch this process?** `git log <main>..<branch> -- modules/local/<process>` and `git log <main>..<branch> -- subworkflows/local/...`. If neither shows changes, the regression is unrelated to the bench's target.
3. **Correlated outliers across related processes?** If two or more related processes (e.g. KRAKEN_RIBO and KRAKEN_NORIBO, or BOWTIE2_HUMAN and BOWTIE2_OTHER) show correlated single-task outliers on the same cohort, suspect a shared host/preemption event affecting one sample, not two independent regressions.
4. **Critical-path impact?** A regression off the critical path (per #785) costs cpu-hours but not workflow wall — significantly weakens the reviewer concern.

If all four say "noise," frame the row in prose as a single-task outlier or scheduling artifact, not a code regression. If any say "real," investigate further before pushing the bench; a real regression in a perf PR is reviewer-blocking.

**A common subtlety**: a process whose source code the PR didn't touch can still shift materially if an upstream PR change altered its input shape (e.g. SUBSET_PAIRED's FIFO merge in #775 changes the input pipeline FASTP sees, so SUBSET_TRIM:FASTP can shift even though FASTP itself wasn't modified). Attribute carefully: in the prose, name the upstream PR as the cause and the downstream process as the locus of the savings.

## Notes on the script outputs

- **JSON role keys are positional, not branch-named.** `parse_bench_trace.py --names main,dev` produces a `compare` block whose keys are `dev_runtime_s`, `pr_runtime_s`, `delta_runtime`, etc. The `dev_*` keys hold the *first-named* branch (here: `main`), and `pr_*` holds the *second-named* (here: `dev`). The names are role-based, not branch-based. Easy footgun for sign-of-delta interpretations.
- **Markdown tables use the actual branch names** in column headers (via `--names`), so the markdown view is unambiguous. Use it for paste-in; reach for the JSON only when slicing programmatically.

## Worked example — cohort-level synthesis (Illumina_100M, multi-PR bundle)

For a bundled perf PR (several merged perf changes on `dev` vs `main`) on the standard Illumina_100M cohort, the synthesis block has roughly this shape:

```markdown
# Benchmarking

Run via `bench-workflow` on the Illumina_100M benchmark cohort (19 samples) on
AWS Batch (`coding-agent-batch-jq` SPOT queue), comparing `origin/main` vs
`origin/dev`. Both cohorts ran concurrently on the same queue, so cpu-hours
is the contention-immune metric and runtime carries cross-cohort scheduling
noise. Output equality is verified via `bench_output_equality.py` (see
Backwards compatibility below).

## Cohort
<lifted verbatim from parse_bench_trace.py --md>

(then a 2-3 sentence interpretation: where the cpu-h saving comes from, and
why cohort wall is flat if it is — critical-path framing per #785)

## Per-process
<lifted from parse_bench_trace.py --md, top ~12 rows by max(branch cpu-h)>

(then a paragraph attributing each win to its merged PR and noting any
regressions, with each regression triaged per Step 5's checklist)

# Backwards compatibility
<lifted from bench_output_equality.py --md>

(then a paragraph: if all DIFFs are known-noise, name the order-sensitive
estimators and the source of the reordering)
```

The user-facing dispatch is: ask the agent invoking this skill to compose `# Benchmarking` + `# Backwards compatibility` from artifact paths you supply (the markdown + JSON files from the underlying scripts), giving cohort context as additional prompt input.

## Step 6 — sanity-check before pushing

- Do the numbers in the prose match the table? Quick way to catch a typo: copy a Δ from the prose, grep for it in the table.
- Does the Δ direction make sense given the change? A perf regression sometimes hides in a column you weren't reading.
- Run the `pr-preflight` agent on the branch — it checks version-bump / CHANGELOG / linting.

## Worked invocation shape (for the top-level agent)

A typical full flow from a perf-PR working directory:

```
# 1. Run the bench(es). The `bench-module` / `bench-workflow` skills are
#    each a dispatcher into the corresponding underlying agent(s).
#
#    For a single-module claim:
#      bench-module → bench-module-local agent (one invocation, two branches)
#
#    For a cohort claim:
#      bench-workflow → bench-workflow-batch agents (one per branch, parallel),
#                       then parse_bench_trace + bench_output_equality
#
# 2. Read each tool's emitted markdown into context. Lift the tables verbatim
#    — the scripts use the canonical metric definitions and column conventions
#    so you don't have to re-derive numbers.
#
# 3. Compose the PR description as outlined above. Don't paraphrase tables;
#    do write fresh interpretation paragraphs.
#
# 4. Validate before push: `pr-preflight`, cross-check numbers, sanity-read
#    the bench section as a reviewer would.
```

## Cross-references

- `.claude/benchmarking.md` — metric definitions and reporting conventions.
- `.claude/pr-examples/pipeline-bench.md` — the worked example to mirror.
- `.claude/skills/bench-module/SKILL.md` — module-level local bench dispatcher.
- `.claude/skills/bench-workflow/SKILL.md` — workflow-level Batch bench dispatcher.
- `.claude/scripts/parse_bench_trace.py` — trace aggregator (both modes use it).
- `.claude/scripts/bench_output_equality.py` — S3 result hash comparison (workflow mode).
- Issue #785 / `project_critical_path_illumina` — critical-path map for the Illumina RUN workflow.
