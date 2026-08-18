#!/usr/bin/env python3
"""Pytest suite for compare_traces.py.

Covers the duration/timestamp parsing that Nextflow's trace format makes
error-prone, the two metric definitions from .claude/benchmarking.md, and
end-to-end aggregation against small on-disk trace files.
"""

###########
# IMPORTS #
###########

import gzip
import logging
from datetime import datetime
from pathlib import Path

import pytest
from compare_traces import (
    ProcessStats,
    aggregate_trace,
    build_table,
    format_delta,
    leaf_name,
    open_by_suffix,
    parse_duration,
    parse_timestamp,
    task_metrics,
    total_stats,
    warn_on_task_count_mismatch,
)

############
# FIXTURES #
############

TRACE_HEADER = "task_id\tprocess\tstatus\tcpus\trealtime\tstart\tcomplete"


def trace_row(
    task_id: str,
    process: str,
    status: str = "COMPLETED",
    cpus: str = "16",
    realtime: str = "1m",
    start: str = "2026-08-17 18:00:00.000",
    complete: str = "2026-08-17 18:02:00.000",
) -> str:
    """Build one tab-separated trace row.

    Args:
        task_id (str): Task identifier
        process (str): Fully-qualified process path
        status (str): Task status
        cpus (str): Allocated CPUs
        realtime (str): Nextflow-formatted inner command duration
        start (str): Task start timestamp
        complete (str): Task completion timestamp
    Returns:
        str: Tab-separated row
    """
    return "\t".join([task_id, process, status, cpus, realtime, start, complete])


@pytest.fixture
def trace_file(tmp_path: Path) -> Path:
    """Write a small trace file with two minimap2 processes and one other.

    Args:
        tmp_path (Path): Pytest temporary directory
    Returns:
        Path: Path to the written trace TSV
    """
    path = tmp_path / "trace.tsv"
    path.write_text(
        "\n".join(
            [
                TRACE_HEADER,
                trace_row("1", "RUN:EXTRACT:MINIMAP2_HUMAN"),
                trace_row("2", "RUN:EXTRACT:MINIMAP2_HUMAN", realtime="2m"),
                trace_row("3", "RUN:PROFILE:MINIMAP2", cpus="8"),
                trace_row("4", "RUN:QC:FASTQC", cpus="1"),
                trace_row("5", "RUN:EXTRACT:MINIMAP2_HUMAN", status="FAILED"),
            ]
        )
        + "\n"
    )
    return path


###################
# TRACE UTILITIES #
###################


@pytest.mark.parametrize(
    "value, expected",
    [
        ("30ms", 0.03),
        ("45s", 45.0),
        ("1m 37s", 97.0),
        ("2h 5m", 7500.0),
        ("1d", 86400.0),
        ("1.5s", 1.5),
        ("-", 0.0),
        ("", 0.0),
    ],
)
def test_parse_duration(value: str, expected: float) -> None:
    """Nextflow duration strings parse to seconds, including compound forms."""
    assert parse_duration(value) == pytest.approx(expected)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-08-17 18:21:17.123", datetime(2026, 8, 17, 18, 21, 17, 123000)),
        ("not a timestamp", None),
        ("", None),
    ],
)
def test_parse_timestamp(value: str, expected: datetime | None) -> None:
    """Trace timestamps parse, and malformed values return None rather than raise."""
    assert parse_timestamp(value) == expected


@pytest.mark.parametrize("compressed", [False, True])
def test_open_by_suffix(tmp_path: Path, compressed: bool) -> None:
    """Plain and gzipped files both open transparently in text mode."""
    if compressed:
        path = tmp_path / "f.txt.gz"
        with gzip.open(path, "wt") as handle:
            handle.write("hello")
    else:
        path = tmp_path / "f.txt"
        path.write_text("hello")
    with open_by_suffix(path) as handle:
        assert handle.read() == "hello"


###############
# AGGREGATION #
###############


def test_process_stats_add() -> None:
    """Adding tasks accumulates count, runtime and cpu-hours."""
    stats = ProcessStats()
    stats.add(60.0, 0.5)
    stats.add(30.0, 0.25)
    assert (stats.tasks, stats.runtime_s, stats.cpu_hours) == (2, 90.0, 0.75)


@pytest.mark.parametrize(
    "process, expected",
    [
        ("RUN:EXTRACT:MINIMAP2_HUMAN", "MINIMAP2_HUMAN"),
        ("MINIMAP2", "MINIMAP2"),
    ],
)
def test_leaf_name(process: str, expected: str) -> None:
    """Fully-qualified process paths reduce to their leaf component."""
    assert leaf_name(process) == expected


def test_task_metrics_uses_allocated_cpus_not_percent_cpu() -> None:
    """cpu-hours are billed (realtime * cpus), and runtime is complete - start."""
    row = {
        "cpus": "16",
        "realtime": "30m",
        "%cpu": "150",
        "start": "2026-08-17 18:00:00.000",
        "complete": "2026-08-17 18:35:00.000",
    }
    runtime_s, cpu_hours = task_metrics(row)
    # 35 min of slot wall, but only 30 min of inner command time.
    assert runtime_s == pytest.approx(2100.0)
    assert cpu_hours == pytest.approx(8.0)


@pytest.mark.parametrize(
    "row, expected",
    [
        ({"cpus": "", "realtime": "1h"}, 1.0),
        ({"cpus": "not-a-number", "realtime": "1h"}, 1.0),
    ],
)
def test_task_metrics_defaults_missing_cpus_to_one(
    row: dict[str, str], expected: float
) -> None:
    """A missing or malformed cpus column falls back to a single CPU."""
    assert task_metrics(row)[1] == pytest.approx(expected)


def test_task_metrics_missing_timestamps_yield_zero_runtime() -> None:
    """A task with no start/complete contributes no runtime rather than raising."""
    assert task_metrics({"cpus": "4", "realtime": "1m"})[0] == 0.0


def test_aggregate_trace_groups_and_filters(trace_file: Path) -> None:
    """Tasks group by leaf name; non-COMPLETED rows are excluded."""
    stats = aggregate_trace(trace_file)
    assert set(stats) == {"MINIMAP2_HUMAN", "MINIMAP2", "FASTQC"}
    # The FAILED MINIMAP2_HUMAN row is dropped, leaving two tasks.
    assert stats["MINIMAP2_HUMAN"].tasks == 2
    # 1m*16 + 2m*16 = 48 CPU-minutes = 0.8 cpu-hours.
    assert stats["MINIMAP2_HUMAN"].cpu_hours == pytest.approx(0.8)
    assert stats["MINIMAP2_HUMAN"].runtime_s == pytest.approx(240.0)


def test_aggregate_trace_pattern_matches_full_path(trace_file: Path) -> None:
    """The pattern applies to the full process path, not just the leaf."""
    assert set(aggregate_trace(trace_file, pattern="MINIMAP2")) == {
        "MINIMAP2_HUMAN",
        "MINIMAP2",
    }
    assert set(aggregate_trace(trace_file, pattern="^RUN:PROFILE:")) == {"MINIMAP2"}


def test_aggregate_trace_reads_gzipped_trace(tmp_path: Path, trace_file: Path) -> None:
    """A gzipped trace aggregates identically to its plain counterpart."""
    gz_path = tmp_path / "trace.tsv.gz"
    with gzip.open(gz_path, "wt") as handle:
        handle.write(trace_file.read_text())
    assert aggregate_trace(gz_path) == aggregate_trace(trace_file)


def test_total_stats_sums_every_process(trace_file: Path) -> None:
    """Totals sum tasks, runtime and cpu-hours across all processes."""
    totals = total_stats(aggregate_trace(trace_file))
    assert totals.tasks == 4
    # MINIMAP2_HUMAN 0.8 + MINIMAP2 1m*8 + FASTQC 1m*1 = 0.8 + 0.1333 + 0.0167
    assert totals.cpu_hours == pytest.approx(0.95)


#############
# REPORTING #
#############


@pytest.mark.parametrize(
    "baseline, candidate, expected",
    [
        (10.0, 5.0, "-50.0%"),
        (10.0, 15.0, "+50.0%"),
        (0.0, 0.0, "n/a"),
        (0.0, 5.0, "new"),
    ],
)
def test_format_delta(baseline: float, candidate: float, expected: str) -> None:
    """Percentage deltas are signed, and a zero baseline is handled explicitly."""
    assert format_delta(baseline, candidate) == expected


def test_build_table_header_matches_benchmarking_convention(trace_file: Path) -> None:
    """The header reproduces the table format in .claude/benchmarking.md.

    The convention is what makes these tables comparable across PRs, so the
    emitted header is pinned rather than left to drift.
    """
    stats = aggregate_trace(trace_file)
    lines = build_table(stats, stats).splitlines()
    assert lines[0] == (
        "| Scope | dev runtime | PR runtime | Δ runtime "
        "| dev cpu-h | PR cpu-h | Δ cpu-h |"
    )
    assert lines[1] == "|---|---:|---:|---:|---:|---:|---:|"


def test_build_table_labels_are_overridable(trace_file: Path) -> None:
    """Run labels are configurable for comparisons that are not dev-vs-PR."""
    stats = aggregate_trace(trace_file)
    header = build_table(stats, stats, "CPU", "GPU").splitlines()[0]
    assert "| CPU runtime | GPU runtime |" in header
    assert "| CPU cpu-h | GPU cpu-h |" in header


def test_build_table_orders_by_cost_and_totals(trace_file: Path) -> None:
    """Rows are ordered by cpu-hours descending and end with a TOTAL row."""
    stats = aggregate_trace(trace_file)
    table = build_table(stats, stats)
    lines = table.splitlines()
    # Two header lines, then processes ordered by cpu-hours descending.
    assert [line.split("|")[1].strip() for line in lines[2:]] == [
        "MINIMAP2_HUMAN",
        "MINIMAP2",
        "FASTQC",
        "**TOTAL**",
    ]
    # Comparing a run against itself must show no change on any row.
    assert table.count("+0.0%") == 2 * len(lines[2:])


def test_build_table_breaks_cost_ties_by_name(tmp_path: Path) -> None:
    """Equal-cost processes sort by name, so table order is reproducible.

    Without a secondary key these would come out in set-iteration order, which
    varies between interpreter runs.
    """
    path = tmp_path / "trace.tsv"
    path.write_text(
        "\n".join(
            [
                TRACE_HEADER,
                trace_row("1", "RUN:X:ZEBRA", cpus="4"),
                trace_row("2", "RUN:X:ALPHA", cpus="4"),
                trace_row("3", "RUN:X:MIKE", cpus="4"),
            ]
        )
        + "\n"
    )
    stats = aggregate_trace(path)
    lines = build_table(stats, stats).splitlines()
    assert [line.split("|")[1].strip() for line in lines[2:]] == [
        "ALPHA",
        "MIKE",
        "ZEBRA",
        "**TOTAL**",
    ]


def test_build_table_handles_process_absent_from_baseline(trace_file: Path) -> None:
    """A process only present in the candidate is reported as new, not dropped."""
    candidate = aggregate_trace(trace_file)
    baseline = {k: v for k, v in candidate.items() if k != "MINIMAP2"}
    table = build_table(baseline, candidate)
    assert "| MINIMAP2 | 0.0 min |" in table
    assert "new" in table


def test_warn_on_task_count_mismatch(
    trace_file: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Differing task counts warn, since the runs may not be comparable."""
    candidate = aggregate_trace(trace_file)
    baseline = {k: v for k, v in candidate.items() if k != "FASTQC"}
    with caplog.at_level(logging.WARNING):
        warn_on_task_count_mismatch(baseline, candidate)
    assert "FASTQC: task count differs between runs (0 vs 1)" in caplog.text


def test_warn_on_task_count_mismatch_silent_when_equal(
    trace_file: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Identical cohorts produce no warning."""
    stats = aggregate_trace(trace_file)
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        warn_on_task_count_mismatch(stats, stats)
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
