---
name: bench
description: Benchmark performance changes in the `securebio/nao-mgs-workflow` pipeline. Dispatches to one of two subagents — `bench-module-local` for a single Nextflow process on the sandbox, or `bench-workflow-batch` for the full pipeline on AWS Batch — and integrates their returned tables into a PR description. Use whenever a PR has a perf claim or "preserves results" claim that needs empirical validation against real data.
---

# Pipeline benchmarking

This skill is a dispatcher: pick a mode, invoke the matching subagent, fold its output into your PR description. The subagents own the procedural details; this file owns the mode-selection and PR-composition guidance.

## Decision: which mode

Two modes, picked by what you're measuring:

- **Module-level (Mode A)** — a single Nextflow `process` in isolation, local Docker on the sandbox, many samples cheap. Invoke `bench-module-local`. Use when the perf claim is about one module *and* the module is fast enough that running it tens of times locally is cheap. The Nextflow + Docker wrapper matches production execution (container start, real I/O patterns), so per-task numbers transfer to where the module will run.
- **Workflow-level (Mode B)** — full pipeline run on AWS Batch via `bin/chain_workflows.py`. Invoke `bench-workflow-batch`. Use when the change spans multiple processes, when the module under test is too slow for many-sample local iteration, or when you want headline cohort numbers. The returned trace, sliced by process or grouped by subworkflow prefix, gives module- and subworkflow-level numbers from the same run.

If you find yourself unsure, default to Mode B — its trace gives you everything Mode A produces plus more, at the cost of an extra ~40 min cohort wall.

## How to invoke

For either mode, supply the agent with at minimum: `dev_branch`, `pr_branch`. Mode A additionally requires the module path and an n_samples hint; Mode B requires the cohort samplesheet and ref-dir (defaults exist for the standard Illumina cohort — see the agent doc).

```
Agent({
  subagent_type: "bench-workflow-batch",
  description: "Batch bench for PR #777",
  prompt: """
  Benchmark dev vs coding-agent/count-reads-rapidgzip on the standard
  Illumina_100M cohort. Standard ref-dir and samplesheet.
  """
})
```

The agent returns a markdown block plus a fenced JSON block with raw script output. Copy the markdown block into your PR description under the appropriate heading (see structure below); keep the JSON block out of the PR description (it would re-trigger CI on every edit per [[feedback_ci_thrash_docs]]) but reference it if a reviewer asks for the underlying numbers.

## Integrating into the PR description

Follow the structure in `.claude/pr-examples/pipeline-bench.md`. The bench block from the agent goes under a top-level `# Benchmarking` heading, after the prose summary + backwards-compatibility section.

The agent's output is structured to drop in directly:

- `## Cohort` table → keep as-is.
- `## Per-process` table → trim to processes the PR actually affects (typically ≥10% Δ or processes whose name appears in the diff).
- `## Output equality` block → goes under the PR's `# Backwards compatibility` heading, not `# Benchmarking`.
- `## Critical-path framing` paragraph → integrate into the prose summary or the bench section's discussion, depending on the PR's framing.

If the agent's output includes a "Notes" section (Wave cache bust, branch-merge fix, etc.), surface anything reviewer-visible in the PR description's narrative.

## Cross-references

The agents follow the metric and recipe conventions in `.claude/benchmarking.md`. They use the scripts under `.claude/scripts/` for the deterministic work (trace parsing, output equality). Critical-path framing references issue #785 / [[project_critical_path_illumina]].

## Updating

When `bin/chain_workflows.py`, `.github/actions/run-benchmark/action.yml`, `.claude/benchmarking.md`, `.claude/scripts/`, or the bench agent definitions change in ways that affect how this skill should be used, update this file in the same PR.
