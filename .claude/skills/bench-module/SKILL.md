---
name: bench-module
description: Bench a single Nextflow process across two or more branches via local Docker. Fans out parallel `bench-module-local` agent calls (one per branch), then aggregates traces into a comparison table via `.claude/scripts/parse_bench_trace.py`. Use when a perf claim is scoped to one module that's fast enough for many-sample local iteration.
---

# Module benchmarking

For perf claims scoped to a single Nextflow process, this skill runs N parallel cohorts (one per branch) via the `bench-module-local` agent, then aggregates the traces into a comparison table.

## Inputs to gather from the caller

- `repo_path`: absolute path to the repo root.
- `branches`: list of two or more branches to compare.
- `module`: Nextflow include path for the module under test (e.g. `./modules/local/countReads`).
- `samplesheet`: local samplesheet.csv path. If the inputs need staging from S3, the caller pre-stages and supplies a samplesheet pointing at local paths.
- (Optional) `extra_config`: a path to an additional `-c` config to pass through to each agent (e.g. tier overrides for a constrained host).

## Procedure

### 1. Pick a run-namespaced root for this comparison

```bash
RUN_DIR="./tmp/bench-module-$(date +%Y%m%dT%H%M%S)-$$"
mkdir -p "$RUN_DIR"
```

Each per-branch agent will get its own subdirectory under `$RUN_DIR`.

### 2. Fan out parallel `bench-module-local` invocations

Launch one agent per branch in a single message (multiple Agent tool calls in parallel). Each gets an explicit `out_dir` under `$RUN_DIR` so they never collide:

```
Agent({
  subagent_type: "bench-module-local",
  description: "Module bench: <branch>",
  prompt: """
  repo_path: <repo_path>
  branch: <branch>
  module: <module>
  samplesheet: <samplesheet>
  out_dir: <RUN_DIR>/<branch-slug>
  [extra_config: <extra_config>]
  """
})
```

`<branch-slug>` should be the branch name with `/` replaced by `-` so it's a valid path segment.

Each agent returns a `## <branch>` markdown block, a `trace_path:` line, and a fenced JSON block. Hold onto the trace paths from all of them.

### 3. Aggregate via `parse_bench_trace.py`

Once all agents return successfully:

```bash
python3 "$repo_path/.claude/scripts/parse_bench_trace.py" \
    <trace_path_a> <trace_path_b> [...] \
    --names "<branch_a>,<branch_b>[,...]" --format md
```

The script emits `## Cohort` and `## Per-process` tables in the format from `.claude/benchmarking.md`.

For exactly two branches, the script also computes Δ runtime and Δ cpu-hours. For three or more, the table compares against the first-listed branch.

### 4. Compose the result

Concatenate any `Notes:` paragraphs from the agents (deduplicating if any are identical) followed by the comparison markdown.

## Output

The combined `Notes:` + comparison markdown, ready to drop into a PR description under `# Benchmarking`. Critical-path framing is not produced here — the caller adds that during PR composition (cross-ref issue #785).

## Escalation

If any per-branch agent returns `ESCALATE: <reason>`, surface the reason to the caller and stop. Do not attempt to recover by re-running. Do not produce a partial comparison from the agents that did succeed — a benchmark missing one branch is not a benchmark.

## Cross-references

- `.claude/benchmarking.md` — metric conventions.
- `.claude/pr-examples/pipeline-bench.md` — PR-writeup structure.
- `.claude/agents/bench-module-local.md` — the agent invoked here.
