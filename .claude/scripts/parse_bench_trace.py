#!/usr/bin/env python3
DESC = """
Aggregate a Nextflow trace.tsv (or two, for dev/PR comparison) into the standard
benchmarking tables used by this repo's perf PRs.

Metrics follow `.claude/benchmarking.md`:
    runtime    = complete - start (slot wall, seconds)
    cpu-hours  = realtime × cpus / 3600

JSON always goes to stdout. Pass `--md FILE` to also write a rendered
markdown table to that path — emitting both formats in one invocation
(useful for the bench subagents that need both).

With one trace: per-process aggregate. With two traces and `--names dev,pr`,
also emits a side-by-side comparison block.
"""

###########
# IMPORTS #
###########

import argparse
import csv
import json
import logging
import re
import sys
import time
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(UTCFormatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.handlers = [handler]


###############
# DATA SHAPES #
###############


class CohortStats(TypedDict):
    """Cohort-level summary computed from one trace."""

    wall_s: float
    total_cpu_h: float
    n_tasks_completed: int
    earliest_submit: str
    latest_complete: str


class ProcessStats(TypedDict):
    """Per-process aggregate stats from one trace."""

    process: str
    n: int
    sum_runtime_s: float
    sum_realtime_s: float
    sum_cpu_h: float
    max_runtime_s: float
    max_realtime_s: float
    cpus: int


class TraceAgg(TypedDict):
    """Single-trace aggregation: cohort + per-process."""

    cohort: CohortStats
    processes: list[ProcessStats]


class Delta(TypedDict):
    """Absolute and percent delta. `pct` is None when the baseline is zero."""

    abs: float
    pct: float | None


class CohortCompare(TypedDict):
    """Cohort-level dev/PR comparison."""

    dev_wall_s: float
    pr_wall_s: float
    delta_wall: Delta
    dev_total_cpu_h: float
    pr_total_cpu_h: float
    delta_total_cpu_h: Delta


class ProcessCompare(TypedDict):
    """Per-process dev/PR comparison."""

    process: str
    dev_n: int
    pr_n: int
    dev_runtime_s: float
    pr_runtime_s: float
    delta_runtime: Delta
    dev_cpu_h: float
    pr_cpu_h: float
    delta_cpu_h: Delta


class Comparison(TypedDict):
    """Full dev/PR comparison block."""

    cohort: CohortCompare
    processes: list[ProcessCompare]


class TraceEntry(TypedDict):
    """One trace's full aggregation, tagged with a name + source path."""

    name: str
    path: str
    cohort: CohortStats
    processes: list[ProcessStats]


class Payload(TypedDict, total=False):
    """Top-level JSON payload emitted to stdout."""

    traces: list[TraceEntry]
    compare: Comparison


############
# PARSING  #
############

_DURATION_UNITS = {"ms": 1 / 1000, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
_DURATION_RE = re.compile(r"([\d.]+)\s*(ms|s|m|h|d)")


def parse_duration(s: str) -> float:
    """Parse a Nextflow duration string into seconds.

    Nextflow emits durations like "1m 37s", "30ms", "1.5h", "2d 3h". Empty
    values and "-" (Nextflow's null marker) parse to 0.0.

    Args:
        s: Duration string from a trace.tsv `realtime` or `duration` column.

    Returns:
        Duration in seconds.
    """
    if not s or s == "-":
        return 0.0
    return sum(float(v) * _DURATION_UNITS[u] for v, u in _DURATION_RE.findall(s.strip()))


def parse_timestamp(s: str) -> datetime:
    """Parse a Nextflow trace timestamp.

    Nextflow trace.tsv emits ISO-format timestamps with a space separator and
    optional fractional seconds (e.g. "2026-04-19 14:23:45.678").

    Args:
        s: Timestamp string from a trace.tsv `submit`/`start`/`complete` column.

    Returns:
        A naive `datetime` (Nextflow trace timestamps are local-time, no tz).

    Raises:
        ValueError: if the timestamp is empty or unparseable.
    """
    if not s or s == "-":
        raise ValueError("empty timestamp")
    return datetime.fromisoformat(s)


def parse_int(s: str, default: int = 0) -> int:
    """Parse a trace column as int, falling back to `default` on missing/empty."""
    if not s or s == "-":
        return default
    return int(s)


###############
# AGGREGATION #
###############


def aggregate_trace(trace_path: Path) -> TraceAgg:
    """Aggregate one trace.tsv into cohort and per-process summaries.

    Filters to rows where `status == "COMPLETED"`. Computes:
        - Cohort wall = `max(complete) - min(submit)` across all COMPLETED tasks.
        - Total cpu-hours = `Σ realtime × cpus / 3600`.
        - Per process: n, Σ runtime_s, Σ cpu_h, max_runtime_s, max_realtime_s,
          cpus (max allocation observed, since `withLabel` can be input-aware).

    Args:
        trace_path: Path to a Nextflow trace.tsv.

    Returns:
        A `TraceAgg` with `cohort` and `processes` keys.

    Raises:
        ValueError: if the trace has no COMPLETED rows.
    """

    def _new_proc() -> ProcessStats:
        return ProcessStats(
            process="",
            n=0,
            sum_runtime_s=0.0,
            sum_realtime_s=0.0,
            sum_cpu_h=0.0,
            max_runtime_s=0.0,
            max_realtime_s=0.0,
            cpus=0,
        )

    proc: dict[str, ProcessStats] = defaultdict(_new_proc)
    earliest_submit: datetime | None = None
    latest_complete: datetime | None = None
    total_cpu_h = 0.0
    n_completed = 0

    with trace_path.open() as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row.get("status") != "COMPLETED":
                continue
            n_completed += 1
            submit = parse_timestamp(row["submit"])
            start = parse_timestamp(row["start"])
            complete = parse_timestamp(row["complete"])
            realtime_s = parse_duration(row["realtime"])
            cpus = parse_int(row.get("cpus", "") or "", default=1)
            runtime_s = (complete - start).total_seconds()
            cpu_h = realtime_s * cpus / 3600.0

            if earliest_submit is None or submit < earliest_submit:
                earliest_submit = submit
            if latest_complete is None or complete > latest_complete:
                latest_complete = complete
            total_cpu_h += cpu_h

            name = row["process"]
            d = proc[name]
            d["process"] = name
            d["n"] += 1
            d["sum_runtime_s"] += runtime_s
            d["sum_realtime_s"] += realtime_s
            d["sum_cpu_h"] += cpu_h
            d["max_runtime_s"] = max(d["max_runtime_s"], runtime_s)
            d["max_realtime_s"] = max(d["max_realtime_s"], realtime_s)
            d["cpus"] = max(d["cpus"], cpus)

    if n_completed == 0 or earliest_submit is None or latest_complete is None:
        raise ValueError(f"{trace_path}: no COMPLETED tasks found")

    return TraceAgg(
        cohort=CohortStats(
            wall_s=(latest_complete - earliest_submit).total_seconds(),
            total_cpu_h=total_cpu_h,
            n_tasks_completed=n_completed,
            earliest_submit=earliest_submit.isoformat(),
            latest_complete=latest_complete.isoformat(),
        ),
        processes=[proc[name] for name in sorted(proc)],
    )


def _delta(a: float, b: float) -> Delta:
    """Return absolute and percent delta from baseline `a` to value `b`.

    `pct` is None when the baseline is zero (delta is undefined).
    """
    diff = b - a
    pct = (100.0 * diff / a) if a > 0 else None
    return Delta(abs=diff, pct=pct)


def compare(dev: TraceAgg, pr: TraceAgg) -> Comparison:
    """Emit a dev-vs-PR comparison block.

    Computes Δ runtime and Δ cpu-hours for each process present in either trace,
    plus cohort-level Δ. Processes missing from one side appear with that side's
    values as zero.
    """
    dev_procs = {p["process"]: p for p in dev["processes"]}
    pr_procs = {p["process"]: p for p in pr["processes"]}
    all_names = sorted(set(dev_procs) | set(pr_procs))

    proc_compare: list[ProcessCompare] = []
    for name in all_names:
        d = dev_procs.get(name)
        p = pr_procs.get(name)
        d_runtime = d["sum_runtime_s"] if d else 0.0
        p_runtime = p["sum_runtime_s"] if p else 0.0
        d_cpu_h = d["sum_cpu_h"] if d else 0.0
        p_cpu_h = p["sum_cpu_h"] if p else 0.0
        proc_compare.append(
            ProcessCompare(
                process=name,
                dev_n=d["n"] if d else 0,
                pr_n=p["n"] if p else 0,
                dev_runtime_s=d_runtime,
                pr_runtime_s=p_runtime,
                delta_runtime=_delta(d_runtime, p_runtime),
                dev_cpu_h=d_cpu_h,
                pr_cpu_h=p_cpu_h,
                delta_cpu_h=_delta(d_cpu_h, p_cpu_h),
            )
        )

    dev_cohort = dev["cohort"]
    pr_cohort = pr["cohort"]
    return Comparison(
        cohort=CohortCompare(
            dev_wall_s=dev_cohort["wall_s"],
            pr_wall_s=pr_cohort["wall_s"],
            delta_wall=_delta(dev_cohort["wall_s"], pr_cohort["wall_s"]),
            dev_total_cpu_h=dev_cohort["total_cpu_h"],
            pr_total_cpu_h=pr_cohort["total_cpu_h"],
            delta_total_cpu_h=_delta(dev_cohort["total_cpu_h"], pr_cohort["total_cpu_h"]),
        ),
        processes=proc_compare,
    )


##############
# FORMATTING #
##############


def fmt_duration(seconds: float) -> str:
    """Format seconds as `Xh Ym` / `Xm Ys` / `Xs` for table display."""
    if seconds >= 3600:
        h, rem = divmod(seconds, 3600)
        m = rem / 60
        return f"{int(h)}h {int(m)}m"
    if seconds >= 60:
        m, s = divmod(seconds, 60)
        return f"{int(m)}m {int(s)}s"
    return f"{seconds:.1f}s"


def fmt_pct(pct: float | None) -> str:
    """Format a percent value with sign, or `n/a` when undefined."""
    if pct is None:
        return "n/a"
    return f"{pct:+.1f}%"


def render_markdown(payload: Payload, top: int | None = None) -> str:
    """Render the JSON payload as markdown tables.

    For single-trace input emits a cohort header + per-process table sorted by
    Σ cpu-hours descending. For dev/PR comparison emits the canonical PR-table
    schema from `.claude/benchmarking.md`.
    """
    lines: list[str] = []
    if "compare" in payload:
        cmp = payload["compare"]
        cohort = cmp["cohort"]
        lines += [
            "## Cohort",
            "",
            "| Scope | dev | PR | Δ |",
            "|---|---:|---:|---:|",
            (
                f"| Wall | {fmt_duration(cohort['dev_wall_s'])} "
                f"| {fmt_duration(cohort['pr_wall_s'])} "
                f"| {fmt_pct(cohort['delta_wall']['pct'])} |"
            ),
            (
                f"| Σ cpu-hours | {cohort['dev_total_cpu_h']:.2f} "
                f"| {cohort['pr_total_cpu_h']:.2f} "
                f"| {fmt_pct(cohort['delta_total_cpu_h']['pct'])} |"
            ),
            "",
            "## Per-process",
            "",
            "| Process | dev runtime | PR runtime | Δ runtime | dev cpu-h | PR cpu-h | Δ cpu-h |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
        procs = sorted(
            cmp["processes"],
            key=lambda p: max(p["dev_cpu_h"], p["pr_cpu_h"]),
            reverse=True,
        )
        if top is not None:
            procs = procs[:top]
        for p in procs:
            lines.append(
                f"| {p['process']} "
                f"| {fmt_duration(p['dev_runtime_s'])} "
                f"| {fmt_duration(p['pr_runtime_s'])} "
                f"| {fmt_pct(p['delta_runtime']['pct'])} "
                f"| {p['dev_cpu_h']:.2f} "
                f"| {p['pr_cpu_h']:.2f} "
                f"| {fmt_pct(p['delta_cpu_h']['pct'])} |"
            )
        return "\n".join(lines) + "\n"

    # Single-trace rendering (one or more traces, but no compare block).
    for entry in payload["traces"]:
        trace_cohort: CohortStats = entry["cohort"]
        trace_procs: list[ProcessStats] = sorted(
            entry["processes"], key=lambda x: x["sum_cpu_h"], reverse=True
        )
        if top is not None:
            trace_procs = trace_procs[:top]
        lines += [
            f"## {entry['name']}",
            "",
            "| Metric | Value |",
            "|---|---:|",
            f"| Cohort wall | {fmt_duration(trace_cohort['wall_s'])} |",
            f"| Σ cpu-hours | {trace_cohort['total_cpu_h']:.2f} |",
            f"| Tasks completed | {trace_cohort['n_tasks_completed']} |",
            "",
            "| Process | n | Σ runtime | Σ cpu-h | max task runtime |",
            "|---|---:|---:|---:|---:|",
        ]
        for ps in trace_procs:
            lines.append(
                f"| {ps['process']} | {ps['n']} "
                f"| {fmt_duration(ps['sum_runtime_s'])} "
                f"| {ps['sum_cpu_h']:.2f} "
                f"| {fmt_duration(ps['max_runtime_s'])} |"
            )
        lines.append("")
    return "\n".join(lines)


########
# MAIN #
########


def parse_arguments(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=DESC, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("traces", nargs="+", type=Path, help="One or two Nextflow trace.tsv files.")
    parser.add_argument(
        "--names",
        type=str,
        default=None,
        help='Comma-separated trace labels (e.g. "dev,pr"). Two names → emit comparison block.',
    )
    parser.add_argument(
        "--md",
        type=Path,
        default=None,
        help="If set, render a markdown table to this path. JSON always goes to stdout.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=None,
        help="In markdown output, limit per-process tables to top N processes by cpu-hours.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main() -> None:
    """Aggregate trace(s), emit JSON to stdout (and markdown to a file if `--md` given)."""
    t0 = time.monotonic()
    args = parse_arguments()
    if len(args.traces) > 2:
        raise ValueError("at most two traces supported (single or dev/PR pair).")
    names = args.names.split(",") if args.names else [p.stem for p in args.traces]
    if len(names) != len(args.traces):
        raise ValueError(f"--names count ({len(names)}) must match trace count ({len(args.traces)}).")

    aggregations = [aggregate_trace(p) for p in args.traces]
    traces: list[TraceEntry] = [
        TraceEntry(name=name, path=str(path), cohort=agg["cohort"], processes=agg["processes"])
        for name, path, agg in zip(names, args.traces, aggregations, strict=True)
    ]
    payload: Payload = Payload(traces=traces)
    if len(aggregations) == 2:
        payload["compare"] = compare(aggregations[0], aggregations[1])

    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")
    if args.md is not None:
        args.md.write_text(render_markdown(payload, top=args.top))

    logger.info("parse_bench_trace done in %.2fs", time.monotonic() - t0)


if __name__ == "__main__":
    main()
