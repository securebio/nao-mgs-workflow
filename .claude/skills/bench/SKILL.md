---
name: bench
description: Benchmark performance changes in the `securebio/nao-mgs-workflow` pipeline. Dispatches to one of two subagents — `bench-module-local` for a single Nextflow process via local Docker, or `bench-workflow-batch` for the full pipeline on AWS Batch — and integrates their returned tables into a PR description. Use whenever a PR has a perf claim or "preserves results" claim that needs empirical validation against real data.
---

# Pipeline benchmarking

This skill is a dispatcher: pick a mode, invoke the matching subagent, fold its output into your PR description. The subagents own the procedural details; this file owns mode selection and PR composition.

## Decision: which agent

Two agents, picked by what you're measuring:

- **`bench-module-local`** — a single Nextflow `process` in isolation, via local Docker, many samples cheap. Use when the perf claim is about one module *and* the module is fast enough that running it tens of times locally is cheap. The Nextflow + Docker wrapper matches production execution (container start, real I/O patterns), so per-task numbers transfer to where the module will run.
- **`bench-workflow-batch`** — full pipeline run on AWS Batch via `bin/chain_workflows.py`. Use when the change spans multiple processes, when the module under test is too slow for many-sample local iteration, or when you want headline cohort numbers. The returned trace, sliced by process or grouped by subworkflow prefix, gives module- and subworkflow-level numbers from the same run.

If you're unsure, default to `bench-workflow-batch` — its trace gives you everything `bench-module-local` produces plus more, at the cost of an extra ~40 min cohort wall.

## How to invoke

Pass the agents inputs explicitly. They do not infer `branch_a`/`branch_b` from PR context, the repo from cwd, or scratch locations from environment.

For `bench-module-local`, the caller writes the thin `.nf` entrypoint (because module signatures vary). For `bench-workflow-batch`, the caller supplies the cohort samplesheet, ref-dir, and a writable S3 scratch prefix.

```
Agent({
  subagent_type: "bench-workflow-batch",
  description: "Workflow bench: dev vs feature branch on Illumina_100M",
  prompt: """
  repo_path: /path/to/nao-mgs-workflow
  branch_a: dev
  branch_b: coding-agent/<feature-branch>
  samplesheet: s3://.../benchmarks/Illumina_100M/samplesheet.csv
  ref_dir: s3://.../mgs-workflow-test/index-latest/output/
  scratch_base: s3://.../bench/
  """
})
```

The agent returns a markdown block plus a fenced JSON block with raw script output. Copy the markdown into your PR description under the appropriate heading (see structure below); keep the JSON out of the PR description (it would re-trigger CI on every edit per [[feedback_ci_thrash_docs]]) but reference it if a reviewer asks for the underlying numbers.

## Integrating into the PR description

Follow the structure in `.claude/pr-examples/pipeline-bench.md`. The bench tables from the agent go under a top-level `# Benchmarking` heading, after the prose summary + backwards-compatibility section.

The agent's output drops in directly:

- `## Cohort` and `## Per-process` tables — keep as-is, optionally trim per-process to processes the PR actually affects (≥10% Δ or processes whose name appears in the diff).
- `## Output equality` block (workflow agent only) — goes under the PR's `# Backwards compatibility` heading, not `# Benchmarking`.
- Critical-path framing — the agents don't produce this; you produce it. For each materially affected process, note whether it sits on the workflow critical path (cross-ref issue #785 / [[project_critical_path_illumina]]). If a per-process improvement is off the critical path, say "reduces cpu-hours but not workflow wall."
- `Notes:` section from the agent — surface anything reviewer-visible in the PR description's narrative.

## Cross-references

The agents follow the metric and recipe conventions in `.claude/benchmarking.md`. They use the scripts under `.claude/scripts/` for the deterministic work (trace parsing, output equality, shared bench config).

## Updating

When `bin/chain_workflows.py`, `.claude/benchmarking.md`, `.claude/scripts/`, or the bench agent definitions change in ways that affect how this skill should be used, update this file in the same PR.
