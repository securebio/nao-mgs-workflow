---
name: bench-workflow
description: Bench the full mgs-workflow pipeline across two or more branches via AWS Batch. Fans out parallel `bench-workflow-batch` agent calls (one per branch), then aggregates traces and verifies output equality via `.claude/scripts/parse_bench_trace.py` and `.claude/scripts/bench_output_equality.py`. Use for cohort-scale perf or "preserves results" claims.
---

# Workflow benchmarking

For perf or output-equality claims at full-pipeline scale, this skill runs N parallel Batch cohorts (one per branch) via the `bench-workflow-batch` agent, then aggregates traces and (for pairwise comparisons) verifies output equality.

## Inputs to gather from the caller

- `repo_path`: absolute path to the repo root.
- `branches`: list of two or more branches to compare.
- `samplesheet`: S3 URI of the cohort samplesheet.
- `ref_dir`: S3 URI of a production-equivalent index directory.
- `scratch_base`: S3 prefix under which to write cohort scratch and outputs.
- (Optional) `platform`: pass `ont` for ONT; defaults to Illumina.
- (Optional) `nextflow_args`: extra args forwarded to `chain_workflows.py --nextflow-args`.

## Procedure

### 1. Fan out parallel `bench-workflow-batch` invocations

Launch one agent per branch in a single message (parallel Agent tool calls):

```
Agent({
  subagent_type: "bench-workflow-batch",
  description: "Workflow bench: <branch>",
  prompt: """
  repo_path: <repo_path>
  branch: <branch>
  samplesheet: <samplesheet>
  ref_dir: <ref_dir>
  scratch_base: <scratch_base>
  [platform: <platform>]
  [nextflow_args: <nextflow_args>]
  """
})
```

Each agent returns `trace_path:` and `results_prefix:` lines plus an optional `Notes:` paragraph. Hold onto the trace paths and result prefixes.

### 2. Aggregate traces

```bash
python3 "$repo_path/.claude/scripts/parse_bench_trace.py" \
    <trace_path_a> <trace_path_b> [...] \
    --names "<branch_a>,<branch_b>[,...]" --top 15 \
    --md <run_dir>/trace-comparison.md \
    > <run_dir>/trace-comparison.json
```

The script writes the markdown tables (`## Cohort` and `## Per-process` with Δ columns when exactly two traces are passed) to the `--md` path and emits the JSON payload to stdout.

### 3. Output equality (pairwise)

For exactly two branches:

```bash
python3 "$repo_path/.claude/scripts/bench_output_equality.py" \
    <results_prefix_a> <results_prefix_b> \
    --md <run_dir>/output-equality.md \
    > <run_dir>/output-equality.json
```

For more than two branches, run the equality check on each pair you want to compare (typically each non-baseline branch against the baseline).

### 4. Compose the result

Concatenate any `Notes:` paragraphs from the agents (deduplicated), the comparison markdown from step 2, and the output-equality markdown from step 3.

## Output

The combined block, ready to drop into a PR description. The trace comparison goes under `# Benchmarking`; the output-equality block goes under `# Backwards compatibility`. Critical-path framing is not produced here — the caller adds that during PR composition (cross-ref issue #785 / [[project_critical_path_illumina]]).

## Escalation

If any per-branch agent returns `ESCALATE: <reason>`, surface the reason to the caller and stop. Do not attempt to recover. Do not produce a partial comparison from successful agents — a benchmark missing one branch is not a benchmark.

If `bench_output_equality.py` reports `diff_unexpected > 0` (real DIFFs on files outside the known-noise list), surface the report and let the caller decide whether to investigate.

## Cross-references

- `.claude/benchmarking.md` — metric conventions, output-equality semantics.
- `.claude/pr-examples/pipeline-bench.md` — PR-writeup structure.
- `.claude/agents/bench-workflow-batch.md` — the agent invoked here.
- Issue #785 / [[project_critical_path_illumina]] — critical-path map for the Illumina RUN workflow.
