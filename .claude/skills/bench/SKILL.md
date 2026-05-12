---
name: bench
description: Benchmark performance or "preserves results" claims in the `securebio/nao-mgs-workflow` pipeline. Dispatches to the `bench-module-local` agent (a single Nextflow process via local Docker) for module-scoped claims, or to the `bench-workflow` skill (full pipeline on AWS Batch) for cohort-scale claims.
---

# Pipeline benchmarking (dispatcher)

Pick the mode that matches what you're measuring, then invoke the corresponding agent or skill:

- **`bench-module-local`** (agent) — A/B benchmark of a single Nextflow process via local Docker. The agent inspects the module's input signature, picks the shortest construction path that produces matching-shape inputs from the samplesheet (identity, inline transformation, or upstream Nextflow processes as needed), and runs both branches. Use when the perf claim is scoped to one module and its inputs can be reproduced locally from raw reads.

- **`bench-workflow`** (skill) — full-pipeline A/B benchmark on AWS Batch. Fans out one `bench-workflow-batch` agent per branch in parallel, then aggregates. Use when the change spans multiple processes, when local construction would effectively reimplement most of the pipeline, or when you want cohort-scale numbers + output equality. The trace, sliced by process, also gives module-level numbers as a byproduct.

If you're unsure or `bench-module-local` escalates, fall through to `bench-workflow`.

## Invoking `bench-module-local`

The agent compares two branches in a single call. Inputs:

```
Agent({
  subagent_type: "bench-module-local",
  description: "Module bench: <module>, <branch_a> vs <branch_b>",
  prompt: """
  repo_path: <repo_path>
  branch_a: <branch_a>
  branch_b: <branch_b>
  module: <module include path, e.g. ./modules/local/countReads>
  samplesheet: <local samplesheet.csv path>
  """
})
```

The agent returns the comparison tables directly. Copy the markdown into the PR description; keep the JSON block out of the description (it would re-trigger CI on every edit per [[feedback_ci_thrash_docs]]) but cite it if a reviewer asks for the underlying numbers.

## Invoking `bench-workflow`

See `.claude/skills/bench-workflow/SKILL.md`. The skill fans out parallel single-branch agents and runs the aggregation scripts itself.

## Integrating results into a PR description

Follow `.claude/pr-examples/pipeline-bench.md`. Trace comparison tables go under `# Benchmarking`; output equality (from `bench-workflow`) goes under `# Backwards compatibility`.

Critical-path framing is not produced by the agents or skills — you produce it. For each materially affected process, note whether it sits on the workflow critical path (cross-ref issue #785 / [[project_critical_path_illumina]]). If a per-process improvement is off the critical path, say "reduces cpu-hours but not workflow wall."

## Cross-references

- `.claude/benchmarking.md` — metric conventions.
- `.claude/scripts/` — deterministic aggregation scripts the agents and skills call.
- `.claude/pr-examples/pipeline-bench.md` — worked PR-writeup example.
