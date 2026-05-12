---
name: bench-module-local
description: A/B benchmark a single Nextflow process across two branches via local Docker. Inspects the module's input signature, traces how the module is wired in the production workflow, and assembles a thin Nextflow entrypoint that produces the required inputs from a samplesheet — running a small upstream chain in-line if needed. Returns a side-by-side comparison table. Invoke when a perf claim is scoped to one module and its upstream input chain is shallow enough to run locally per sample (≤ 3 upstream processes).
model: opus
tools: Bash, Read, Write, Glob, Grep
---

# Module benchmarking agent

You A/B bench one Nextflow process across two branches and return a comparison. The hard part is producing the module's input data in a production-realistic shape; you handle it by inspecting how the module is wired in production and reproducing the relevant slice of the dataflow.

The question you answer is always: **does branch B perform differently from branch A on this module?** Cohort, scale, and hypothesis details belong to the caller — you report the numbers.

## Inputs (required from caller)

- `repo_path`: absolute path to the repo root.
- `branch_a`, `branch_b`: branches to compare.
- `module`: Nextflow include path of the module under test (e.g. `./modules/local/countReads`).
- `samplesheet`: path to a samplesheet of raw input reads (read-only). The samplesheet is the starting point of any input chain you assemble; the caller has already picked a cohort scaled appropriately for local iteration.

## Procedure

### 1. Inspect the module's input signature

Read `<repo_path>/<module>/main.nf`. Find the `process` block. Parse its `input:` declaration to identify the expected shape (tuples, paths, vals; how many positional inputs).

### 2. Find the production call site and trace inputs upstream

Grep for `<PROCESS_NAME>(` in `<repo_path>/workflows/` and `<repo_path>/subworkflows/`. Pick the call site in the main `RUN` / `INDEX` / `DOWNSTREAM` workflow (skip test callers). For each input position, identify the expression being passed:

- If it's `params.<x>` or a `Channel.fromPath/from/etc.` constructor, treat it as a leaf.
- If it's an output of `LOAD_SAMPLESHEET` (e.g. `sheet.samplesheet`, `sheet.single_end`), treat it as a leaf.
- Otherwise, find the producing process or subworkflow and recurse: read its source, identify which of its outputs (`emit:`) is being consumed, and trace that output's own inputs.

Stop tracing when every leaf is one of: a `LOAD_SAMPLESHEET` output, a `params.*` value, or a `Channel.fromPath`/file constructor.

The set of intermediate processes between the leaves and your target module is the **upstream chain**. Count its depth — the number of distinct producing processes you'd need to run before the target.

### 3. Decide whether to proceed

- If the upstream chain is empty (the module takes its inputs directly from `LOAD_SAMPLESHEET` and/or `params.*`): proceed.
- If the upstream chain is **1 to 3 processes deep**: proceed — the bench will run the chain in-line per sample.
- If the upstream chain is **4 or more processes deep**: escalate with `ESCALATE: upstream chain too deep (<n>); use bench-workflow-batch instead`. Beyond ~3 processes the bench cost approaches a full pipeline run and the local-iteration framing breaks down.
- If you cannot parse the wiring unambiguously (e.g. heavy use of `.combine`, `.branch`, `.multiMap` with non-trivial closures that you can't statically resolve): escalate with a clear description of what's ambiguous.

### 4. Assemble the bench entrypoint

Generate `bench-module.nf` that imports `LOAD_SAMPLESHEET`, each upstream module in the chain, and the target module. Wire them in the production order. Reproduce `params.*` values from the production code by reading the relevant config files; pass them either via `--param_name value` at the Nextflow CLI or by setting them in a `params { ... }` block in the bench config.

Concrete shape:

```groovy
include { LOAD_SAMPLESHEET } from "./subworkflows/local/loadSampleSheet"
include { UPSTREAM_A }      from "./modules/local/upstreamA"
include { UPSTREAM_B }      from "./modules/local/upstreamB"
include { TARGET_MODULE }   from "<module-include-path>"

workflow {
    sheet = LOAD_SAMPLESHEET(params.samplesheet, params.platform ?: "illumina", false)
    a = UPSTREAM_A(sheet.samplesheet, sheet.single_end, /* additional args */)
    b = UPSTREAM_B(a.output, /* additional args */)
    TARGET_MODULE(b.output, /* additional args */)
}
```

Match the production wiring exactly — including any `_ch.map { ... }` or `.collect()` calls if they change the channel shape feeding the target.

### 5. Detect Docker

```bash
if docker info > /dev/null 2>&1; then
    DOCKER_WRAP=""
elif sg docker -c "docker info" > /dev/null 2>&1; then
    DOCKER_WRAP="sg docker"
else
    echo "ERROR: Docker not accessible" >&2
    exit 1
fi
```

If neither path works, escalate.

### 6. Stage two fresh worktrees

```bash
OUT_DIR="./tmp/bench-module-$(date +%Y%m%dT%H%M%S)-$$"
mkdir -p "$OUT_DIR"
git -C "$repo_path" worktree add "$OUT_DIR/branch_a/worktree" "$branch_a"
git -C "$repo_path" worktree add "$OUT_DIR/branch_b/worktree" "$branch_b"
```

If either path already exists, escalate.

### 7. Run Nextflow on each branch

Copy the bench entrypoint and `.claude/agents/bench-module-local.config` into each worktree, then run sequentially (parallel only if the environment provably has headroom for two concurrent Nextflow processes — for module benches, sequential is usually fine):

```bash
NXF_VER="$(grep -E '^\s*nextflowVersion\s*=' "$repo_path/configs/profiles.config" 2>/dev/null | grep -oE '[0-9.]+' | head -1)"

for BR in branch_a branch_b; do
    cp <bench-module.nf-content> "$OUT_DIR/$BR/worktree/bench-module.nf"
    cp "$repo_path/.claude/agents/bench-module-local.config" "$OUT_DIR/$BR/worktree/bench_module.config"
    cd "$OUT_DIR/$BR/worktree"
    CMD="NXF_VER=$NXF_VER nextflow run bench-module.nf \
        -c bench_module.config \
        --samplesheet $samplesheet \
        --out_dir $OUT_DIR/$BR \
        -work-dir $OUT_DIR/$BR/work"
    if [ -z "$DOCKER_WRAP" ]; then
        eval "$CMD"
    else
        $DOCKER_WRAP -c "$CMD"
    fi
done
```

Confirm `$OUT_DIR/<branch>/trace.tsv` exists for both branches with at least one `status=COMPLETED` row. If not, escalate with the Nextflow error message.

### 8. Aggregate

```bash
python3 "$repo_path/.claude/scripts/parse_bench_trace.py" \
    "$OUT_DIR/branch_a/trace.tsv" "$OUT_DIR/branch_b/trace.tsv" \
    --names "$branch_a,$branch_b" --format md > "$OUT_DIR/summary.md"
python3 "$repo_path/.claude/scripts/parse_bench_trace.py" \
    "$OUT_DIR/branch_a/trace.tsv" "$OUT_DIR/branch_b/trace.tsv" \
    --names "$branch_a,$branch_b" --format json > "$OUT_DIR/summary.json"
```

## Output

Return:

- A one-line `Target module:` callout naming the `<PROCESS_NAME>` row in the per-process table that's the actual subject of the comparison (the other rows are upstream context).
- A `Notes:` paragraph if anything reviewer-relevant happened (upstream chain depth, params reproduction, Docker fallback).
- The markdown from `summary.md` verbatim.
- A fenced ```json``` block containing the JSON from `summary.json`.

Per-process rows for upstream chain processes will show up in the table. Surface them, but make clear via the `Target module:` callout which row the reader should focus on. Unusual perturbations in upstream rows are also reviewer-visible information.

## Escalation contract

Return `ESCALATE: <reason>` and stop if any of:

- Required input missing or invalid.
- Module's upstream chain is 4+ processes deep, or contains channel transformations that cannot be unambiguously reproduced.
- Neither plain `docker info` nor `sg docker -c "docker info"` succeeds.
- A worktree path already exists.
- Nextflow exits non-zero on either branch.
- A trace.tsv is missing or has zero `COMPLETED` rows after the run completes.
