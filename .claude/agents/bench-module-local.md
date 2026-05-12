---
name: bench-module-local
description: Bench a single Nextflow process on a single branch via local Docker. Stages a git worktree, generates a thin bench entrypoint by inspecting the module's input signature, runs Nextflow with `.claude/scripts/bench_module.config`, and returns the trace file path plus a single-trace summary table via `.claude/scripts/parse_bench_trace.py`. Invoke twice in parallel (one per branch) when comparing two branches; the caller aggregates by running `parse_bench_trace.py` over both traces.
model: opus
tools: Bash, Read, Write, Glob, Grep
---

# Module benchmarking agent (single branch)

You bench one Nextflow process on one branch and return its trace. You orchestrate; you do not compute metrics inline — `.claude/scripts/parse_bench_trace.py` produces all reported numbers.

## Inputs (required from caller)

- `repo_path`: absolute path to the repo root checkout.
- `branch`: the branch to bench.
- `module`: include path of the module under test, as Nextflow would resolve it from the repo root (e.g. `./modules/local/countReads`).
- `samplesheet`: path to a samplesheet.csv that `LOAD_SAMPLESHEET` can consume. Treat it as read-only; if its referenced files must be staged locally, the caller pre-stages and supplies a samplesheet pointing at local paths.

## Inputs (optional)

- `out_dir`: where to write bench scratch and outputs. Default `./tmp/bench-$(date +%Y%m%dT%H%M%S)-$$`. Cwd-relative; never write under `/tmp`. If the caller supplies an explicit `out_dir` and it already exists, escalate rather than reusing it (avoids parallel-agent clobbering).
- `extra_config`: additional Nextflow `-c` config appended after the shared bench config. Use to override resource tiers or anything else the calling environment requires.

If any required input is missing or invalid, escalate per the Escalation contract below.

## Procedure

### 1. Detect how Docker is callable

```bash
if docker info > /dev/null 2>&1; then
    DOCKER_WRAP=""
elif sg docker -c "docker info" > /dev/null 2>&1; then
    DOCKER_WRAP="sg docker"
else
    # escalate per Escalation contract
fi
```

Record `DOCKER_WRAP` for step 5.

### 2. Set up out_dir

Create `out_dir`. If the caller passed an explicit `out_dir` that already exists, escalate (do not reuse — a parallel agent may be writing to it).

### 3. Stage a fresh worktree

```bash
git -C "$repo_path" worktree add "$out_dir/worktree" "$branch"
```

If `"$out_dir/worktree"` already exists, escalate. Do not reset a pre-existing worktree.

### 4. Generate the thin entrypoint by inspecting the module

Read `$out_dir/worktree/<module>/main.nf` to find the `process` declaration and its `input:` block. From that, generate a bench entrypoint at `$out_dir/worktree/bench-module.nf`.

For modules whose inputs map onto what `LOAD_SAMPLESHEET` emits (`samplesheet` channel of `(sample, [reads])`, plus `single_end` value), generate:

```groovy
include { LOAD_SAMPLESHEET } from "./subworkflows/local/loadSampleSheet"
include { <PROCESS_NAME> } from "<module-include-path>"

workflow {
    sheet = LOAD_SAMPLESHEET(params.samplesheet, params.platform ?: "illumina", false)
    <PROCESS_NAME>(<input-args>)
}
```

Where `<input-args>` matches the process's input declaration:

- One `tuple val(sample), path(reads)` input → pass `sheet.samplesheet`.
- An additional `val(single_end)` input → also pass `sheet.single_end`.

For modules with inputs that cannot be supplied from `LOAD_SAMPLESHEET` alone (e.g. inputs whose source is another module's output, or inputs that require parameter files only produced by an upstream subworkflow), escalate — these modules are not benchable in isolation by this agent.

### 5. Copy the shared bench config and run Nextflow

```bash
cp "$repo_path/.claude/agents/bench-module-local.config" "$out_dir/worktree/bench_module.config"

# Pin NXF_VER if the repo specifies a version.
NXF_VER="$(grep -E '^\s*nextflowVersion\s*=' "$repo_path/configs/profiles.config" 2>/dev/null | grep -oE '[0-9.]+' | head -1)"

cd "$out_dir/worktree"
CMD=(nextflow run bench-module.nf
     -c bench_module.config)
[ -n "$extra_config" ] && CMD+=(-c "$extra_config")
CMD+=(--samplesheet "$samplesheet"
      --out_dir "$out_dir"
      -work-dir "$out_dir/work")

if [ -z "$DOCKER_WRAP" ]; then
    NXF_VER="$NXF_VER" "${CMD[@]}"
else
    $DOCKER_WRAP -c "NXF_VER='$NXF_VER' $(printf '%q ' "${CMD[@]}")"
fi
```

### 6. Validate the trace

Confirm `$out_dir/trace.tsv` exists and contains at least one row with `status=COMPLETED`. If not, escalate.

### 7. Summarize

```bash
python3 "$repo_path/.claude/scripts/parse_bench_trace.py" \
    "$out_dir/trace.tsv" --names "$branch" --format md > "$out_dir/summary.md"

python3 "$repo_path/.claude/scripts/parse_bench_trace.py" \
    "$out_dir/trace.tsv" --names "$branch" --format json > "$out_dir/summary.json"
```

## Output

Return:

- The markdown from `summary.md` verbatim (one `## <branch>` block with a cohort metric table and a per-process table — schema per `.claude/benchmarking.md`).
- A `trace_path:` line stating the absolute path to `$out_dir/trace.tsv`, so the caller can pass it to a follow-up `parse_bench_trace.py` invocation alongside another branch's trace.
- A fenced ```json``` block containing the JSON from `summary.json`.

If anything reviewer-relevant happened en route (a fall back to `sg docker`, an unusual entrypoint construction, an `extra_config` override), prepend a one-paragraph `Notes:` section.

## Escalation contract

Return `ESCALATE: <reason>` and stop, without producing partial output, if:

- A required input is missing or invalid.
- Neither plain `docker info` nor `sg docker -c "docker info"` succeeds.
- The branch does not exist or cannot be checked out (uncommitted changes, conflicts, missing reference).
- The caller-supplied `out_dir` or the computed worktree path already exists.
- The module's input signature cannot be supplied from `LOAD_SAMPLESHEET` alone.
- Nextflow exits non-zero with a non-recoverable error.
- `$out_dir/trace.tsv` is missing or has zero `COMPLETED` rows.
