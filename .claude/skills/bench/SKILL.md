---
name: bench
description: Benchmark performance changes in the `securebio/nao-mgs-workflow` pipeline. Two modes — module-level (single Nextflow process, local sandbox Docker, many samples) and workflow-level (full pipeline on AWS Batch via `chain_workflows.py`, slice trace by process). Use whenever a PR has a perf claim or "preserves results" claim that needs empirical validation against real data. Both modes share the same metric conventions, output-equality recipe, and critical-path framing.
---

# Pipeline benchmarking

## Decision: which mode

Two modes, picked by what you're measuring:

- **Module-level (Mode A)** — local Nextflow on the sandbox, a single `process` in isolation, many samples cheap. Use when the perf claim is about a single module *and* the module is fast enough that running it tens or hundreds of times locally is cheap. The Nextflow + Docker wrapper matches the production execution environment (container start, real I/O patterns), so per-task numbers transfer to where the module will actually run. Useful for resource tiering, peak-RSS bounds, and head-to-head tool comparison at module scope.
- **Workflow-level (Mode B)** — full pipeline run on AWS Batch via `bin/chain_workflows.py`. Use when the change spans multiple processes, when the module under test is too slow for many-sample local iteration, or when you want headline cohort numbers. The resulting trace, sliced by `process` or grouped by subworkflow prefix (e.g. `EXTRACT_VIRAL_READS:*`), gives module- and subworkflow-level numbers from the same run.

## Required reading (both modes)

- `.claude/benchmarking.md` — metric definitions (`runtime = complete − start`, `cpu-hours = realtime × cpus / 3600`), trace-parsing recipe, output-equality verification, known-noise columns. Strictly follow the formulae; do not substitute alternatives.
- `.claude/pr-examples/pipeline-bench.md` — concrete example of the writeup format expected in PR bodies. Mirror the structure.

## Critical-path framing (both modes)

Before claiming a workflow-wall improvement, check whether the targeted process is actually on the RUN workflow's critical path. See issue #785 / `project_critical_path_illumina.md` for the current map. Per-process wall improvements *off* the critical path reduce cpu-hours but cannot reduce time-to-results; per-process improvements *on* the critical path only reduce workflow wall if the targeted process bounds the chain (i.e., if it's the slowest task at that stage).

PR write-ups should make this distinction explicit — don't conflate per-process Δ wall with workflow Δ wall.

## Output equality (both modes)

Any PR claiming to "preserve results" must verify empirically before the claim ships. Per `.claude/benchmarking.md`: decompress + sort + md5sum each per-sample output, dev-vs-PR. Known-noise columns (`kraken.tsv.gz` `n_minimizers_distinct`, `qc_basic_stats_cleaned.tsv.gz` `percent_duplicates`) can DIFF on results-preserving PRs due to order-sensitive estimators — document in the PR's backwards-compat section if those are the only DIFFs.

## Mode A: module-level local Nextflow

### Layout

```
~/<bench-name>/
  reads/         # pre-staged input fastq.gz
  ref/           # mirrored production index or symlinks
  bench.config
  samplesheet.csv
  out-dev/       # trace.txt + per-sample published outputs
  out-pr/
  work-dev/
  work-pr/
~/<repo>-dev/    # git worktree on dev; contains bench-module.nf
~/<repo>-pr/     # git worktree on feature branch; contains bench-module.nf
```

Use `/home/ssm-user/`, **not** `/tmp` — `/tmp` is tmpfs (~7.7 GB); `/home` has ~180 GB.

### Procedure

**1. Two worktrees, never branch-switching.**

```bash
git worktree add ../<repo>-dev dev
git worktree add ../<repo>-pr  <feature-branch>
```

Run each branch's bench from its own worktree directory. Branch-switching mid-bench is how you end up with mixed-version runs.

**2. Pre-stage inputs to local disk.** Both branches read from the same staged inputs — keeps S3 transfer out of the timing.

**3. Thin entrypoint per worktree.** Run the single module under test on a Channel of samples. Example for `COUNT_READS`:

```groovy
include { COUNT_READS } from "./modules/local/countReads"

workflow {
    samples = Channel.fromPath(params.samplesheet)
        .splitCsv(header: true)
        .map { row -> tuple(row.sample, [file(row.fastq_1), file(row.fastq_2)]) }
    COUNT_READS(samples, false)
}
```

The `include "./modules/..."` resolves against `projectDir`, so each worktree pulls its own version of the module.

**4. bench.config.**

```groovy
docker.enabled = true
wave.enabled = false
process.executor = 'local'
process.maxForks = 1                    // see below
nextflow.enable.moduleBinaries = true

includeConfig "${projectDir}/configs/containers.config"

// Scale tiers to sandbox (8 cores / 15 GB). Production tiers exceed available memory.
process {
    withLabel: small               { cpus = 8; memory = 14.GB }
    withLabel: large               { cpus = 8; memory = 14.GB }
    withLabel: max                 { cpus = 8; memory = 14.GB }
    withLabel: xsmall              { cpus = 1; memory = 4.GB  }
    withLabel: single              { cpus = 1; memory = 4.GB  }
    withLabel: single_cpu_16GB_memory { cpus = 1; memory = 14.GB }
    withLabel: single_cpu_32GB_memory { cpus = 1; memory = 14.GB }
}

trace {
    enabled = true
    file = "${params.out_dir}/trace.tsv"
    sep = '\t'
    fields = "task_id,hash,native_id,process,tag,name,status,exit,submit,start,complete,duration,realtime,cpus,%cpu,memory,%mem,vmem,rss,peak_rss,read_bytes,write_bytes"
    overwrite = true
}
```

**`process.maxForks = 1` is mandatory if you care about per-sample wall.** Without it, sibling tasks contend for the 8 cores and per-sample wall becomes contention-noisy. CPU-hours is contention-immune either way.

**5. Run via `sg docker`.** The sandbox's SSM session doesn't load supplementary groups, so docker requires `sudo` or `sg docker`:

```bash
sg docker -c "NXF_VER=25.10.5 nextflow run bench-module.nf -c bench.config \
    --samplesheet samplesheet.csv --out_dir out-dev/ -work-dir work-dev/"
```

Pin `NXF_VER` to the version in `configs/profiles.config`.

**6. Parse the trace.** Use the recipe in `.claude/benchmarking.md` (the canonical parser). Per-task fields you'll want: `realtime`, `%cpu`, `peak_rss`, `read_bytes`, `write_bytes`.

## Mode B: workflow-level on AWS Batch

### Prerequisite — container readiness

Most common stall point. If the PR adds a tool to a `containers/*.yml` file, the new container has to be available before Batch can pull it.

- **Wave 400 "container does not exist"**: misleading. Per-fingerprint negative cache. Workaround: touch any `bin/*` file (no-op comment edit) to change the bundled-binaries layer digest. See `feedback_wave_negative_cache.md`.
- **ECR image not yet published**: the `build-and-test` CI workflow publishes new containers to ECR when it succeeds against the PR's HEAD. Submitting the bench before CI finishes means Batch fails to pull. Wait for `build-and-test` green before kicking off.

If you don't have permission to trigger a container build/push (agent role is ECR pull-only), stop and ask the user. Don't try to circumvent.

### Procedure

**1. Stage both branches in scratch S3 with disjoint base-dirs.**

```bash
RUN_ID=$(date +%Y%m%dT%H%M%S)-$RANDOM
DEV_BASE=s3://sb-det-agent-scratch/bench/${RUN_ID}/dev/
PR_BASE=s3://sb-det-agent-scratch/bench/${RUN_ID}/pr/
```

Use `sb-det-agent-scratch` (28-day TTL, agent-default scratch).

**2. Submit two parallel Batch runs**, one per branch worktree:

```bash
# In dev worktree
bin/chain_workflows.py \
    --ref-dir s3://nao-testing/mgs-workflow-test/index-latest/output/ \
    --samplesheet s3://nao-testing/benchmarks/Illumina_100M/samplesheet.csv \
    --launch-dir illumina-100M-dev \
    --base-dir "$DEV_BASE" \
    --nextflow-args "--rust_tools_version dev"

# In PR worktree (parallel)
bin/chain_workflows.py \
    --ref-dir s3://nao-testing/mgs-workflow-test/index-latest/output/ \
    --samplesheet s3://nao-testing/benchmarks/Illumina_100M/samplesheet.csv \
    --launch-dir illumina-100M-pr \
    --base-dir "$PR_BASE" \
    --nextflow-args "--rust_tools_version dev"
```

Both submit to `coding-agent-batch-jq` (SPOT, 256 vCPU cap shared). Cohort wall in parallel is ~40 min for Illumina_100M.

Parallel-on-same-queue is fine for cpu-hours (contention-immune); runtime carries cross-cohort SPOT scheduling noise. Lead with cpu-hours.

**3. Pull traces from S3.**

```bash
aws s3 cp "${DEV_BASE}output/logging/trace.tsv" /tmp/trace-dev.tsv
aws s3 cp "${PR_BASE}output/logging/trace.tsv" /tmp/trace-pr.tsv
```

**4. Slice the trace.** Per-process Σ `runtime` + Σ `cpu-hours`, plus **max-task wall per process** (the critical-path bound). Use the parser in `.claude/benchmarking.md`.

Cohort wall = `max(complete) − min(submit)` across all `COMPLETED` tasks.

For subworkflow-level numbers, group processes by the subworkflow they belong to (e.g. `EXTRACT_VIRAL_READS:*`).

**5. Output equality** — same recipe as `.claude/benchmarking.md` but over S3 paths. Skeleton:

```bash
DEV=${DEV_BASE}output/results
PR=${PR_BASE}output/results
aws s3 ls "$DEV/" --recursive | awk '{print $4}' | while read key; do
    rel=${key#*output/results/}
    dev_hash=$(aws s3 cp "s3://${DEV#s3://}/$rel" - | gunzip 2>/dev/null | sort | md5sum | cut -d' ' -f1)
    pr_hash=$( aws s3 cp "s3://${PR#s3://}/$rel"  - | gunzip 2>/dev/null | sort | md5sum | cut -d' ' -f1)
    [[ "$dev_hash" == "$pr_hash" ]] && echo "OK   $rel" || echo "DIFF $rel"
done
```

## Cohort references

- **Illumina_100M**: 19 paired-end samples. `s3://nao-testing/benchmarks/Illumina_100M/samplesheet.csv`. Files ~500 MB R1; sample task max-walls are O(min). Production samples are 10-100× larger.
- **ONT**: pass `--platform ont` to `chain_workflows.py`; separate `benchmark-ont-100k.yml` workflow exists.

## Sandbox resources (Mode A)

- 8 vCPUs, ~15 GB RAM, ~180 GB free `/`, 7.7 GB tmpfs `/tmp`
- Docker needs `sg docker` or `sudo`; SSM session doesn't load supplementary groups.
- Production index dir mirrored at `s3://nao-testing/mgs-workflow-test/index-latest/output/`.

## Common traps

- **Mid-bench code changes.** Touching the code under test mid-run contaminates the comparison. Re-run the affected branch fresh.
- **Reusing `--base-dir` across cohorts (Mode B).** They'll clobber each other in S3. Always namespace by run ID.
- **Submitting Mode B before container CI finishes.** Batch fails to pull the new container and the whole bench has to be re-run.
- **`maxForks=1` left out of Mode A's bench config.** Wall times become contention-noisy.
- **`/tmp` instead of `/home/ssm-user/` in Mode A.** Disk fills up.
- **Forgetting to scale resource tiers down in Mode A.** Production tiers exceed sandbox memory; Nextflow rejects pre-flight.
- **Substituting metric definitions.** `(complete − start) × cpus` is not cpu-hours; `realtime × %cpu / 100` is not cpu-hours. Use `realtime × cpus / 3600`.
- **Treating per-process wall Δ as workflow wall Δ.** Cross-reference issue #785 / `project_critical_path_illumina.md` before claiming end-to-end improvement.
- **Forgetting the output-equality check.** "Preserves results" claims without dev-vs-PR hash comparison are a recurring miss.

## Updating this skill

When `bin/chain_workflows.py`, `.github/actions/run-benchmark/action.yml`, or `.claude/benchmarking.md` change in ways that affect the procedure here, update this skill in the same PR.
