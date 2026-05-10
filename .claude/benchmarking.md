# Benchmarking metrics and conventions

This file defines the metrics and reporting conventions used for performance PRs against this pipeline. **PR descriptions and writeups should follow these conventions strictly** so numbers are comparable across PRs and so reviewers don't have to re-derive definitions each time.

## Metrics

Every benchmarking writeup reports exactly **two metrics**, both derived from the per-task rows of Nextflow's `trace.tsv`:

### runtime

`runtime = complete − start` (timestamps), in seconds.

This is the slot wall time — container provisioning + command + teardown. It's what AWS Batch (or any scheduler) holds the slot for. Use it as the wall column in cohort tables.

```python
runtime_s = (parse_ts(row["complete"]) - parse_ts(row["start"])).total_seconds()
```

### cpu-hours

`cpu-hours = realtime × cpus / 3600`, where `realtime` is the inner command's wall time from the `realtime` column.

This excludes container overhead, so Δ cpu-hours tracks the underlying compute change directly while Δ runtime is diluted by per-task overhead that's constant between dev and PR.

```python
cpu_h = parse_realtime(row["realtime"]) * int(row["cpus"] or 1) / 3600
```

### Why both

- **runtime** is the cluster-cost metric (matches what the scheduler bills).
- **cpu-hours** is contention-immune and tracks the actual work change.

The Δ between dev and PR usually shows up more cleanly in cpu-hours; runtime Δ is muted because container overhead is constant. Report both so reviewers see both the realized slot savings and the underlying work change.

### Do not substitute

- Do **not** use `realtime` alone as the "wall" column — `runtime` (= complete − start) is the project convention for slot wall.
- Do **not** use `realtime × %cpu / 100` as cpu-hours — that measures *actual* CPU consumed, which under-counts multi-CPU allocations running single-threaded code. This project uses billed cpu-hours (`realtime × cpus`), not actual.
- Do **not** use `(complete − start) × cpus` as cpu-hours — that folds container overhead into compute billing.

## Parsing the trace

The `realtime` column formats wall durations as strings like `"1m 37s"`, `"30ms"`, `"2h 5m"`. Don't try `float()` — scan for all `(value, unit)` pairs and sum.

Units seen in this repo: `ms`, `s`, `m`, `h`. Conversion table: `{"ms": 1/1000, "s": 1, "m": 60, "h": 3600, "d": 86400}`.

Always filter to `status == "COMPLETED"` before aggregating.

```python
import csv, re
from datetime import datetime
from collections import defaultdict

UNITS = {"ms": 1/1000, "s": 1, "m": 60, "h": 3600, "d": 86400}
def parse_realtime(s):
    return sum(float(v) * UNITS[u] for v, u in re.findall(r"([\d.]+)\s*(ms|s|m|h|d)", s))
def parse_ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")

stats = defaultdict(lambda: {"runtime_s": 0.0, "cpu_h": 0.0, "n": 0})
for r in csv.DictReader(open(trace), delimiter="\t"):
    if r["status"] != "COMPLETED": continue
    proc = r["process"]
    runtime_s = (parse_ts(r["complete"]) - parse_ts(r["start"])).total_seconds()
    cpu_h     = parse_realtime(r["realtime"]) * int(r["cpus"] or 1) / 3600.0
    d = stats[proc]
    d["runtime_s"] += runtime_s
    d["cpu_h"]     += cpu_h
    d["n"]         += 1
```

## Reporting

PR-description bench tables use these column headers:

| Scope | dev runtime | PR runtime | Δ runtime | dev cpu-h | PR cpu-h | Δ cpu-h |
|---|---:|---:|---:|---:|---:|---:|

**Terminology in prose:** write "cpu-hours" (full hyphenated word). The compact form `cpu-h` is fine for table headers and other tight spaces, but in any sentence body use `cpu-hours`.

Per-process Δ note: cpu-hours is proportional to `realtime`. runtime is `realtime + container_overhead`. So Δ runtime can diverge from Δ cpu-hours even when `cpus` is unchanged — runtime is diluted by the constant overhead. For small fast tasks the divergence is wide; for long-running tasks the two metrics converge.

## Output equality verification

Any PR that claims to "preserve results" should run a cohort-wide diff of published outputs before the claim ships. Don't rely on principle.

For each per-sample published file in `${results_dir}`:
1. Decompress with `gunzip` (for `.gz` files).
2. Sort lines (`sort`) to canonicalize for non-deterministic write order from multi-threaded processes.
3. md5sum and compare.

```bash
DEV=s3://.../dev-run/output/results
PR=s3://.../pr-run/output/results
aws s3 ls "$DEV/" | awk '{print $4}' | while read fname; do
    if [[ "$fname" == *.gz ]]; then
        dev_hash=$(aws s3 cp "$DEV/$fname" - | gunzip 2>/dev/null | sort | md5sum | cut -d' ' -f1)
        pr_hash=$( aws s3 cp "$PR/$fname"  - | gunzip 2>/dev/null | sort | md5sum | cut -d' ' -f1)
    else
        dev_hash=$(aws s3 cp "$DEV/$fname" - | md5sum | cut -d' ' -f1)
        pr_hash=$( aws s3 cp "$PR/$fname"  - | md5sum | cut -d' ' -f1)
    fi
    [[ "$dev_hash" == "$pr_hash" ]] && echo "OK   $fname" || echo "DIFF $fname"
done
```

Known noise sources (these will show DIFFs even on results-preserving PRs):

- `kraken.tsv.gz` column 5 (`n_minimizers_distinct`): Kraken2's HyperLogLog sketch is order-sensitive across threads. Mask that column before comparing if reads upstream are reordered.
- `qc_basic_stats_cleaned.tsv.gz` `percent_duplicates`: FastQC samples the first ~100 k reads to estimate duplication, so the estimate depends on input order.

If either column DIFFs are the only DIFFs and read content (`virus_hits.tsv.gz`, `read_counts.tsv`, kraken classifications) is byte-identical, document the order-sensitivity in the PR's backwards-compatibility section rather than calling the PR results-changing.

## Backwards-compatibility framing

When a PR changes read order (multi-threaded outputs that previously back-pressured into input order) but preserves read content:

- "Pipeline outputs preserve read content but not stream order."
- Cite the empirical verification: which files were byte-identical after `sort | md5sum`, which had order-sensitive estimator drift.
- Identify the source of the reordering (which process / which threading change).

This framing avoids over-claiming "byte-identical" while honestly conveying that downstream consumers see the same reads.

## Benchmark cohort

For Illumina-pipeline performance PRs, run the **Illumina_100M benchmark** (19 samples) on AWS Batch via the `coding-agent-batch-jq` SPOT queue. See `bin/chain_workflows.py` and the workflows under `.github/actions/run-benchmark/`.

Both dev baseline and PR cohorts can run in parallel on the same queue; CPU-hours are contention-immune and are the cleaner read in that case. Per-process runtime carries some cross-cohort scheduling noise.
