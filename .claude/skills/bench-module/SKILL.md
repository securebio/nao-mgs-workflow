---
name: bench-module
description: A/B benchmark a single Nextflow process across two branches via local Docker. Thin wrapper that dispatches to the `bench-module-local` subagent. Use when a perf claim is scoped to one module — the agent constructs an entrypoint that reproduces the module's input shape from a samplesheet, runs both branches in worktrees, and returns a comparison table ready to drop into a PR description.
---

# Module benchmarking dispatcher

For a single-process perf comparison, dispatch to the `bench-module-local` agent. The agent inspects the module's `input:` declaration and picks the shortest construction path that produces matching-shape inputs from a samplesheet, runs both branches in parallel via local Docker, and returns the comparison.

## When to invoke

- A PR's perf claim is scoped to one Nextflow process and you want a tight A/B comparison locally.
- You're iterating on a perf change and want fast turnaround (~2-5 min/cycle on tiny test data, longer on production-scale).

Skip for full-pipeline claims, output-equality claims, or claims that span multiple subworkflows — those want a Batch-cohort bench instead.

## Inputs to gather from the caller

- `repo_path` (default: the current repo root if cwd is the repo).
- `process_name` (e.g. `COUNT_READS`, `BBDUK_HITS_INTERLEAVE`).
- Two branches to compare (`branch_a`, `branch_b`).
- `samplesheet` — a samplesheet of raw input reads. The repo's `test-data/samplesheet.csv` points at the tiny-test cohort and is a reasonable default for fast iteration.

## Canonical invocation

```
Agent({
  subagent_type: "bench-module-local",
  description: "Module bench: <process_name>, <branch_a> vs <branch_b>",
  prompt: """
  repo_path: <repo_path>
  branch_a: <branch_a>
  branch_b: <branch_b>
  process_name: <process_name>
  samplesheet: <samplesheet>
  """
})
```

The agent returns:
- `Target module:` callout
- `Notes:` paragraph (construction strategy, caveats)
- `## Cohort` + `## Per-process` markdown tables (drop into PR description under `# Benchmarking`)
- A fenced JSON block with the raw `parse_bench_trace.py` output

## Integrating into a PR description

Follow `.claude/pr-examples/pipeline-bench.md` for structure. The agent's markdown tables drop in directly. Strip column-name `origin/` prefixes if both branches share it (e.g. `origin/main` / `origin/dev` → `main` / `dev`) for compactness.

If the agent's `Notes:` flag a construction caveat (production-fidelity chain → upstream-Δ confound; or tiny inputs → Δ not representative of production-scale), surface that in the PR's prose. Don't let it sit only in the bench block.

## Cross-references

- `.claude/agents/bench-module-local.md` — the agent invoked here.
- `.claude/scripts/parse_bench_trace.py` — the aggregator the agent calls.
- `.claude/benchmarking.md` — metric conventions (`runtime = complete - start`, `cpu-hours = realtime × cpus / 3600`).
- `.claude/pr-examples/pipeline-bench.md` — worked PR-writeup example.
