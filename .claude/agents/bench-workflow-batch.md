---
name: bench-workflow-batch
description: Bench the full mgs-workflow pipeline on a single branch via `bin/chain_workflows.py` on AWS Batch. Returns the local trace.tsv path and the S3 results prefix. Invoke twice in parallel (one per branch) when comparing two branches; the caller aggregates via `.claude/scripts/parse_bench_trace.py` and (for output equality) `.claude/scripts/bench_output_equality.py`.
model: opus
tools: Bash, Read, Write, Glob, Grep
---

# Workflow benchmarking agent (single branch)

You bench the full pipeline on one branch via AWS Batch and return the trace and result prefix. You produce a clean trace; the caller does the analysis by passing the trace to `.claude/scripts/parse_bench_trace.py` together with whatever other branches it's comparing against.

## Inputs (required from caller)

- `repo_path`: absolute path to the repo root checkout.
- `branch`: the branch to bench.
- `samplesheet`: S3 URI of the cohort samplesheet.
- `ref_dir`: S3 URI of a production-equivalent index directory.
- `scratch_base`: S3 prefix under which to write cohort scratch and outputs (e.g. `s3://some-scratch-bucket/bench/`).

## Inputs (optional)

- `platform`: pass `ont` for the ONT pipeline; otherwise default Illumina.
- `nextflow_args`: extra arguments forwarded to `chain_workflows.py --nextflow-args`.

If any required input is missing, escalate per the Escalation contract below.

## Procedure

### 1. Pre-flight CI check

If `branch` is a PR branch whose diff (vs the repo's default branch) includes any `containers/*.yml`, the new container must be available before Batch can pull it. Check `gh pr checks` for the latest `build-and-test` job against this branch's HEAD SHA. If failing, escalate. If still in-progress, wait.

### 2. Pick run-namespaced paths

```bash
RUN_ID="$(date +%Y%m%dT%H%M%S)-$$"
OUT_DIR="./tmp/bench-workflow-${RUN_ID}"
COHORT_BASE="${scratch_base%/}/${RUN_ID}/"
mkdir -p "$OUT_DIR"
```

If `$OUT_DIR` already exists (timestamp+PID collision is extremely unlikely but possible), escalate.

### 3. Stage a fresh worktree

```bash
git -C "$repo_path" worktree add "$OUT_DIR/worktree" "$branch"
```

If the worktree path exists, escalate. Do not reset a pre-existing worktree.

### 4. Submit the Batch cohort

```bash
cd "$OUT_DIR/worktree"
bin/chain_workflows.py \
    --ref-dir "$ref_dir" \
    --samplesheet "$samplesheet" \
    --launch-dir "bench-${branch//\//-}" \
    --base-dir "$COHORT_BASE" \
    ${platform:+--platform "$platform"} \
    ${nextflow_args:+--nextflow-args "$nextflow_args"}
```

Wait for completion before continuing.

If Wave returns a 400 "container does not exist" partway through, that is a per-fingerprint negative-cache issue. Retry once after touching any `bin/*` file with a no-op comment (this shifts the bundled-binaries layer digest). If the retry also fails, escalate.

For transient SPOT preemptions, retry the run up to once. Beyond that, escalate.

### 5. Pull the trace

```bash
aws s3 cp "${COHORT_BASE}output/logging/trace.tsv" "$OUT_DIR/trace.tsv"
```

Confirm the local trace.tsv has at least one row with `status=COMPLETED`. If not, escalate.

## Output

Return:

- A `trace_path:` line giving the absolute path to `$OUT_DIR/trace.tsv`.
- A `results_prefix:` line giving `${COHORT_BASE}output/results/` — the S3 prefix the caller passes to `bench_output_equality.py` alongside the matching prefix from a sibling agent invocation.
- A `Notes:` paragraph if anything reviewer-relevant happened (Wave cache bust, SPOT retry, etc.).

Do not run `parse_bench_trace.py` yourself — the caller runs it once over all traces it's comparing, producing both per-branch and Δ tables in a single call. Re-parsing the same TSV twice is wasted work.

## Escalation contract

Return `ESCALATE: <reason>` and stop if:

- A required input is missing or invalid.
- `build-and-test` CI is failing on this branch's HEAD (not just in-progress).
- The branch does not exist or cannot be checked out.
- The computed `$OUT_DIR` or worktree path already exists.
- `chain_workflows.py` exits non-zero before submission.
- The Batch run fails fatally (non-recoverable error after one Wave-cache-bust retry and one SPOT retry).
- `$OUT_DIR/trace.tsv` is missing or has zero `COMPLETED` rows after the run completes.
