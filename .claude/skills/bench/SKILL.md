---
name: bench
description: Benchmark performance or "preserves results" claims in the `securebio/nao-mgs-workflow` pipeline. Dispatches to `bench-module` (a single Nextflow process via local Docker) or `bench-workflow` (full pipeline on AWS Batch), each of which fans out a single-branch agent in parallel per branch under comparison.
---

# Pipeline benchmarking (dispatcher)

Pick the mode that matches what you're measuring, then invoke the corresponding skill:

- **`bench-module`** — single Nextflow process, local Docker, many samples cheap. Use when the perf claim is scoped to one module *and* the module is fast enough for many-sample local iteration. The Nextflow + Docker wrapper matches production execution (container start, real I/O), so per-task numbers transfer to where the module will run.
- **`bench-workflow`** — full pipeline on AWS Batch via `bin/chain_workflows.py`. Use when the change spans multiple processes, when the module under test is too slow for local iteration, or when you want headline cohort numbers + output equality. The trace, sliced by process, gives module- and subworkflow-level numbers as a byproduct.

If you're unsure, default to `bench-workflow` — its trace gives you everything `bench-module` produces plus output equality, at the cost of an extra ~40 min cohort wall.

## Integrating results into a PR description

Both skills return markdown ready to drop into a PR description. Follow the structure in `.claude/pr-examples/pipeline-bench.md`:

- Trace comparison tables go under `# Benchmarking`.
- Output equality block (from `bench-workflow` only) goes under `# Backwards compatibility`.
- Critical-path framing — the skills don't produce this; you do. For each materially affected process, note whether it sits on the workflow critical path (cross-ref issue #785 / [[project_critical_path_illumina]]). If a per-process improvement is off the critical path, say "reduces cpu-hours but not workflow wall."

Keep the JSON blocks returned by the skills out of the PR description (they would re-trigger CI on every edit per [[feedback_ci_thrash_docs]]) but cite them if a reviewer asks for the underlying numbers.

## Cross-references

- `.claude/benchmarking.md` — metric conventions (`runtime = complete - start`, `cpu-hours = realtime × cpus / 3600`).
- `.claude/scripts/` — deterministic aggregation scripts the skills call.
- `.claude/pr-examples/pipeline-bench.md` — worked PR-writeup example.
