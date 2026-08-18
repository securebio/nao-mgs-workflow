#!/usr/bin/env python3
DESC = """
Compare per-process timings between two Nextflow trace files and emit a
markdown cohort table. Intended for performance PRs, where a baseline run on
`dev` is compared against a candidate run on the PR branch.

Reports the two metrics defined in .claude/benchmarking.md:

  runtime   = complete - start, summed over tasks (the slot wall time a
              scheduler holds, so the cluster-cost metric)
  cpu-hours = realtime * cpus / 3600, summed over tasks (excludes container
              overhead, so it tracks the underlying work change)

Only tasks with status == COMPLETED are counted. Tasks are grouped by the leaf
component of the `process` column, so `RUN:PROFILE:MINIMAP2` is reported as
`MINIMAP2`; distinct full paths that share a leaf are aggregated together.

Column headers follow the reporting convention in .claude/benchmarking.md, so
the table can be pasted into a PR description unmodified.

Usage:
    python bin/compare_traces.py \\
        --baseline trace_dev.tsv --candidate trace_pr.tsv \\
        --pattern MINIMAP2
"""

###########
# IMPORTS #
###########

import argparse
import csv
import gzip
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone.

        Args:
            record: LogRecord object containing timestamp data
            datefmt: Optional date format string (unused)
        Returns:
            Formatted timestamp string in UTC timezone
        """
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

###################
# TRACE UTILITIES #
###################

# Nextflow renders durations as e.g. "1m 37s", "30ms", "2h 5m". Scanning for
# (value, unit) pairs is required; float() fails on every one of these.
DURATION_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
DURATION_PATTERN = re.compile(r"([\d.]+)\s*(ms|s|m|h|d)")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def open_by_suffix(path: Path) -> IO[str]:
    """Open a file for text reading, transparently handling gzip.

    Args:
        path (Path): Path to a plain or gzipped file
    Returns:
        IO[str]: Open text-mode file handle
    """
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return open(path)


def parse_duration(value: str) -> float:
    """Parse a Nextflow duration string into seconds.

    Args:
        value (str): Duration such as "1m 37s", "30ms" or "-"
    Returns:
        float: Duration in seconds; 0.0 for missing or unparseable values
    """
    return sum(
        float(number) * DURATION_UNITS[unit]
        for number, unit in DURATION_PATTERN.findall(value or "")
    )


def parse_timestamp(value: str) -> datetime | None:
    """Parse a Nextflow trace timestamp.

    Args:
        value (str): Timestamp such as "2026-08-17 18:21:17.123"
    Returns:
        datetime | None: Parsed timestamp, or None if missing or unparseable
    """
    try:
        return datetime.strptime(value, TIMESTAMP_FORMAT)
    except (ValueError, TypeError):
        return None


###############
# AGGREGATION #
###############


@dataclass
class ProcessStats:
    """Aggregated timings for one process across all of its tasks."""

    tasks: int = 0
    runtime_s: float = 0.0
    cpu_hours: float = 0.0

    def add(self, runtime_s: float, cpu_hours: float) -> None:
        """Fold one task's timings into the aggregate.

        Args:
            runtime_s (float): Task runtime (complete - start) in seconds
            cpu_hours (float): Task cpu-hours (realtime * cpus / 3600)
        """
        self.tasks += 1
        self.runtime_s += runtime_s
        self.cpu_hours += cpu_hours


def leaf_name(process: str) -> str:
    """Reduce a fully-qualified process path to its leaf name.

    Args:
        process (str): Value of the trace `process` column, e.g.
            "RUN:EXTRACT_VIRAL_READS:MINIMAP2_HUMAN"
    Returns:
        str: Leaf component, e.g. "MINIMAP2_HUMAN"
    """
    return process.rsplit(":", 1)[-1]


def task_metrics(row: dict[str, str]) -> tuple[float, float]:
    """Compute runtime and cpu-hours for one trace row.

    Both metrics follow .claude/benchmarking.md: runtime is the slot wall time
    (complete - start), and cpu-hours are billed rather than actual, so they
    use the allocated `cpus` and not `%cpu`.

    Args:
        row (dict[str, str]): One parsed trace row
    Returns:
        tuple[float, float]: (runtime in seconds, cpu-hours)
    """
    start = parse_timestamp(row.get("start", ""))
    complete = parse_timestamp(row.get("complete", ""))
    runtime_s = (complete - start).total_seconds() if start and complete else 0.0
    try:
        cpus = int(row.get("cpus") or 1)
    except ValueError:
        cpus = 1
    cpu_hours = parse_duration(row.get("realtime", "")) * cpus / 3600
    return runtime_s, cpu_hours


def aggregate_trace(path: Path, pattern: str | None = None) -> dict[str, ProcessStats]:
    """Aggregate a trace file by leaf process name.

    Args:
        path (Path): Path to a Nextflow trace TSV (optionally gzipped)
        pattern (str | None): Optional regex; only processes whose full path
            matches are included. None includes every process.
    Returns:
        dict[str, ProcessStats]: Per-process aggregated timings
    """
    regex = re.compile(pattern) if pattern else None
    stats: dict[str, ProcessStats] = defaultdict(ProcessStats)
    skipped = 0
    with open_by_suffix(path) as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            process = row.get("process", "")
            if row.get("status") != "COMPLETED":
                skipped += 1
                continue
            if regex and not regex.search(process):
                continue
            stats[leaf_name(process)].add(*task_metrics(row))
    logger.info(
        f"{path.name}: {sum(s.tasks for s in stats.values())} matching tasks "
        f"across {len(stats)} processes ({skipped} non-COMPLETED tasks skipped)"
    )
    return dict(stats)


def total_stats(stats: dict[str, ProcessStats]) -> ProcessStats:
    """Sum per-process stats into a single total.

    Args:
        stats (dict[str, ProcessStats]): Per-process aggregated timings
    Returns:
        ProcessStats: Totals across every process
    """
    combined = ProcessStats()
    for entry in stats.values():
        combined.tasks += entry.tasks
        combined.runtime_s += entry.runtime_s
        combined.cpu_hours += entry.cpu_hours
    return combined


#############
# REPORTING #
#############


def format_delta(baseline: float, candidate: float) -> str:
    """Render a percentage change, guarding against a zero baseline.

    Args:
        baseline (float): Baseline value
        candidate (float): Candidate value
    Returns:
        str: Signed percentage such as "-42.1%", or "n/a" when undefined
    """
    if baseline == 0:
        return "n/a" if candidate == 0 else "new"
    return f"{(candidate - baseline) / baseline * 100:+.1f}%"


def warn_on_task_count_mismatch(
    baseline: dict[str, ProcessStats],
    candidate: dict[str, ProcessStats],
) -> None:
    """Log a warning where the two runs executed different numbers of tasks.

    A differing task count usually means the two runs processed different
    cohorts, which makes the totals incomparable. The reporting convention in
    .claude/benchmarking.md has no column for task counts, so this surfaces as
    a log warning rather than in the table.

    Args:
        baseline (dict[str, ProcessStats]): Baseline run timings
        candidate (dict[str, ProcessStats]): Candidate run timings
    """
    for name in sorted(set(baseline) | set(candidate)):
        base = baseline.get(name, ProcessStats()).tasks
        cand = candidate.get(name, ProcessStats()).tasks
        if base != cand:
            logger.warning(
                f"{name}: task count differs between runs ({base} vs {cand}); "
                "the two runs may not be comparable"
            )


def build_table(
    baseline: dict[str, ProcessStats],
    candidate: dict[str, ProcessStats],
    baseline_label: str = "dev",
    candidate_label: str = "PR",
) -> str:
    """Render the per-process comparison as a markdown table.

    Column headers and alignment follow the reporting convention in
    .claude/benchmarking.md, so the output can be pasted into a PR description
    unmodified.

    Args:
        baseline (dict[str, ProcessStats]): Baseline run timings
        candidate (dict[str, ProcessStats]): Candidate run timings
        baseline_label (str): Column label for the baseline run
        candidate_label (str): Column label for the candidate run
    Returns:
        str: Markdown table with a trailing TOTAL row
    """
    header = (
        f"| Scope | {baseline_label} runtime | {candidate_label} runtime | Δ runtime "
        f"| {baseline_label} cpu-h | {candidate_label} cpu-h | Δ cpu-h |\n"
        "|---|---:|---:|---:|---:|---:|---:|"
    )
    rows = []
    # Name breaks ties: cpu-hours alone would leave equal-cost processes in
    # set-iteration order, which varies between runs and adds noise to diffs.
    names = sorted(
        set(baseline) | set(candidate),
        key=lambda n: (
            -max(
                baseline.get(n, ProcessStats()).cpu_hours,
                candidate.get(n, ProcessStats()).cpu_hours,
            ),
            n,
        ),
    )
    for name in [*names, "TOTAL"]:
        if name == "TOTAL":
            base, cand = total_stats(baseline), total_stats(candidate)
            name = "**TOTAL**"
        else:
            base = baseline.get(name, ProcessStats())
            cand = candidate.get(name, ProcessStats())
        rows.append(
            f"| {name} "
            f"| {base.runtime_s / 60:.1f} min | {cand.runtime_s / 60:.1f} min "
            f"| {format_delta(base.runtime_s, cand.runtime_s)} "
            f"| {base.cpu_hours:.2f} | {cand.cpu_hours:.2f} "
            f"| {format_delta(base.cpu_hours, cand.cpu_hours)} |"
        )
    return "\n".join([header, *rows])


#############
# INTERFACE #
#############


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description=DESC, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--baseline", type=Path, required=True, help="Trace TSV for the baseline run"
    )
    parser.add_argument(
        "--candidate", type=Path, required=True, help="Trace TSV for the candidate run"
    )
    parser.add_argument(
        "--pattern",
        default=None,
        help="Regex restricting the comparison to matching process paths "
        "(default: all processes)",
    )
    parser.add_argument(
        "--baseline-label", default="dev", help="Column label for the baseline run"
    )
    parser.add_argument(
        "--candidate-label", default="PR", help="Column label for the candidate run"
    )
    parser.add_argument(
        "--out", type=Path, default=None, help="Write the table here as well as stdout"
    )
    return parser.parse_args()


def main() -> None:
    """Compare two trace files and print a markdown cohort table."""
    start_time = time.time()
    logger.info("Initializing script.")
    args = parse_arguments()
    logger.info(f"Baseline trace: {args.baseline}")
    logger.info(f"Candidate trace: {args.candidate}")
    logger.info(f"Process pattern: {args.pattern or '(all)'}")

    baseline = aggregate_trace(args.baseline, args.pattern)
    candidate = aggregate_trace(args.candidate, args.pattern)
    if not baseline and not candidate:
        raise ValueError(f"No COMPLETED tasks matched pattern {args.pattern!r}")

    warn_on_task_count_mismatch(baseline, candidate)
    table = build_table(baseline, candidate, args.baseline_label, args.candidate_label)
    print(table)
    if args.out:
        args.out.write_text(table + "\n")
        logger.info(f"Wrote table to {args.out}")

    logger.info(f"Total time elapsed: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
