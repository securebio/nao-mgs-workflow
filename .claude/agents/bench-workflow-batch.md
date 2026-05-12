---
name: bench-workflow-batch
description: Bench the full mgs-workflow pipeline on a single branch via `bin/chain_workflows.py` on AWS Batch. Returns the local trace.tsv path and the S3 results prefix. Invoke twice in parallel (one per branch) when comparing two branches; the caller aggregates via `.claude/scripts/parse_bench_trace.py` and (for output equality) `.claude/scripts/bench_output_equality.py`.
model: opus
tools: Bash, Read, Write, Glob, Grep
---

# Workflow benchmarking agent (single branch)

You bench the full pipeline on one branch via AWS Batch and return the trace and result prefix. You produce a clean trace; the caller does the analysis by passing the trace to `.claude/scripts/parse_bench_trace.py` together with whatever other branches it's comparing against.

**Important: this is a long-running task.** A full Illumina_100M cohort run via AWS Batch takes 30-60 minutes wall. Your job spans one long wait — you submit `chain_workflows.py` to run in the background (via Bash's `run_in_background=true`), and the harness will resume your context with a completion notification when it exits. You do *not* return your final output until the trace has been pulled from S3 (step 5). Don't declare "I'll wait passively" and exit; instead, issue the background Bash call and trust the harness to wake you. See step 4 for the exact pattern.

## Inputs (required from caller)

- `repo_path`: absolute path to the repo root checkout. Note: avoid using a path that is itself a git worktree (e.g. don't bench from a `git worktree add`'d checkout). A worktree-of-worktree arrangement has surfaced fragility in dogfooding; bench from the primary clone.
- `branch`: the branch to bench.
- `samplesheet`: S3 URI of the cohort samplesheet. Verify the path exists before invoking (`aws s3 ls <path>`); the `Illumina_100M` cohort's samplesheet lives at `s3://nao-testing/benchmarks/Illumina_100M/metadata/samplesheet.csv` (not the top-level), so check the directory layout if you get a 404.
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

`OUT_DIR` must be absolute and uniquely random across concurrent invocations (timestamp+PID can collide under heavy parallelism). Put it under `$HOME`, *not* under any git worktree — putting OUT_DIR's sub-worktrees inside a parent worktree is fragile (Nextflow's stage dirs collide with git metadata in ways that have wiped worktrees mid-run during dogfooding).

```bash
OUT_DIR="$(mktemp -d "$HOME/bench-workflow-XXXXXXXX")"
RUN_ID="$(basename "$OUT_DIR")"
COHORT_BASE="${scratch_base%/}/${RUN_ID}/"
```

### 3. Stage a fresh worktree

```bash
git -C "$repo_path" worktree add --detach "$OUT_DIR/worktree" "$branch"
```

`--detach` is required when the branch is already checked out elsewhere (e.g. `dev` is open in another worktree). If the worktree path already exists, escalate.

### 4. Submit the Batch cohort

This is the long-running step (30-60 min cohort wall). The `Bash` tool's max foreground timeout is 10 minutes — a foreground call to `chain_workflows.py` will time out before the cohort finishes. **You must use `run_in_background=true`** so the bash command runs asynchronously; the harness resumes your context with a completion notification when the background process exits.

Two known environment-specific overrides are required on the sandbox:

- `NXF_VER=25.10.5` — `configs/profiles.config` pins Nextflow ≥ 25.10.5, but the system default may be newer (26.04+) whose v2 parser rejects `configs/resources.config`'s `import nextflow.util.MemoryUnit`. Forcing the matched version avoids the parse failure.
- `process.queue` + `batch_job_role` override — the `test_run` / `standard` profiles default to a queue the sandbox `CodingAgentRole` cannot submit to. Override to `coding-agent-batch-jq` with `CodingAgentBatchJobRole`.

Concrete invocation — a single Bash tool call with `run_in_background=true`:

```bash
cd "$OUT_DIR/worktree" && \
NXF_VER=25.10.5 \
bin/chain_workflows.py \
    --ref-dir "$ref_dir" \
    --samplesheet "$samplesheet" \
    --launch-dir "bench-${branch//\//-}" \
    --base-dir "$COHORT_BASE" \
    --nextflow-args "-process.queue=coding-agent-batch-jq --batch_job_role=arn:aws:iam::058264081542:role/CodingAgentBatchJobRole ${nextflow_args:-}" \
    ${platform:+--platform "$platform"} \
    > "$OUT_DIR/chain.log" 2>&1
```

When you invoke this with `run_in_background=true`:

- The Bash tool returns immediately with a shell ID.
- Your turn pauses until the harness wakes you with a completion notification.
- Do **not** declare "I'll wait passively" or otherwise return control voluntarily. Issue the background Bash call as your last tool call before the wait, and the harness will resume you when it exits.

When the notification arrives, check the exit status. Non-zero exit → escalate with the error from `chain.log`. Zero exit → proceed to step 5.

If Wave returns a 400 "container does not exist" partway through, that is a per-fingerprint negative-cache issue. Retry once after touching any `bin/*` file with a no-op comment (this shifts the bundled-binaries layer digest), again with `run_in_background=true`. If the retry also fails, escalate.

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
