"""Tests for parse_bench_trace.py."""

from datetime import datetime
from pathlib import Path

import pytest

from parse_bench_trace import (
    Payload,
    TraceEntry,
    aggregate_trace,
    compare,
    fmt_duration,
    fmt_pct,
    parse_duration,
    parse_int,
    parse_timestamp,
    render_markdown,
)


@pytest.mark.parametrize(
    ("inp", "expected"),
    [
        ("", 0.0),
        ("-", 0.0),
        ("0", 0.0),
        ("30ms", 0.03),
        ("1.5s", 1.5),
        ("1m 37s", 97.0),
        ("1.5h", 5400.0),
        ("2d 3h", 2 * 86400 + 3 * 3600),
        ("1h 2m 3s", 3600 + 120 + 3),
    ],
)
def test_parse_duration(inp: str, expected: float) -> None:
    assert parse_duration(inp) == pytest.approx(expected)


def test_parse_timestamp_iso() -> None:
    ts = parse_timestamp("2026-04-19 14:23:45.678")
    assert ts == datetime(2026, 4, 19, 14, 23, 45, 678000)


def test_parse_timestamp_empty_raises() -> None:
    with pytest.raises(ValueError):
        parse_timestamp("")
    with pytest.raises(ValueError):
        parse_timestamp("-")


@pytest.mark.parametrize(
    ("inp", "default", "expected"),
    [
        ("", 0, 0),
        ("-", 0, 0),
        ("", 1, 1),
        ("4", 0, 4),
        ("16", 1, 16),
    ],
)
def test_parse_int(inp: str, default: int, expected: int) -> None:
    assert parse_int(inp, default=default) == expected


# Minimal trace.tsv with the columns the script actually reads. Mirrors
# the schema emitted by Nextflow trace = { fields = "..." } directives.
_TRACE_HEADER = "task_id\thash\tnative_id\tprocess\ttag\tname\tstatus\texit\tcpus\tsubmit\tstart\tcomplete\tduration\trealtime\n"


def _trace_row(
    process: str,
    cpus: int,
    submit: str,
    start: str,
    complete: str,
    realtime: str,
    status: str = "COMPLETED",
) -> str:
    return (
        f"1\t-\t-\t{process}\t-\t-\t{status}\t0\t{cpus}\t{submit}\t{start}\t{complete}\t-\t{realtime}\n"
    )


def _write_trace(tmp_path: Path, name: str, rows: list[str]) -> Path:
    p = tmp_path / name
    p.write_text(_TRACE_HEADER + "".join(rows))
    return p


def test_aggregate_trace_basic(tmp_path: Path) -> None:
    trace = _write_trace(
        tmp_path,
        "trace.tsv",
        [
            _trace_row("FOO", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:01:05", "1m"),
            _trace_row("FOO", 1, "2026-01-01 00:00:01", "2026-01-01 00:00:06", "2026-01-01 00:02:06", "2m"),
            _trace_row("BAR", 4, "2026-01-01 00:00:02", "2026-01-01 00:00:07", "2026-01-01 00:03:07", "3m"),
        ],
    )
    out = aggregate_trace(trace)
    cohort = out["cohort"]
    procs = {p["process"]: p for p in out["processes"]}

    # Wall = latest_complete - earliest_submit = 00:03:07 - 00:00:00 = 187 s
    assert cohort["wall_s"] == pytest.approx(187.0)
    # cpu-h = realtime × cpus / 3600 = (60 + 120) × 1 / 3600 + 180 × 4 / 3600 = 0.05 + 0.2 = 0.25
    assert cohort["total_cpu_h"] == pytest.approx(0.25)
    assert cohort["n_tasks_completed"] == 3

    assert procs["FOO"]["n"] == 2
    assert procs["FOO"]["sum_cpu_h"] == pytest.approx((60 + 120) / 3600)
    assert procs["FOO"]["max_realtime_s"] == pytest.approx(120.0)
    assert procs["FOO"]["cpus"] == 1
    assert procs["BAR"]["n"] == 1
    assert procs["BAR"]["sum_cpu_h"] == pytest.approx(180 * 4 / 3600)
    assert procs["BAR"]["cpus"] == 4


def test_aggregate_trace_skips_non_completed(tmp_path: Path) -> None:
    trace = _write_trace(
        tmp_path,
        "trace.tsv",
        [
            _trace_row("FOO", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:01:05", "1m"),
            _trace_row(
                "BAR", 4, "2026-01-01 00:00:01", "2026-01-01 00:00:06", "2026-01-01 00:02:06", "1m", status="FAILED"
            ),
        ],
    )
    out = aggregate_trace(trace)
    assert out["cohort"]["n_tasks_completed"] == 1
    assert [p["process"] for p in out["processes"]] == ["FOO"]


def test_aggregate_trace_raises_on_empty(tmp_path: Path) -> None:
    trace = _write_trace(tmp_path, "trace.tsv", [])
    with pytest.raises(ValueError, match="no COMPLETED tasks"):
        aggregate_trace(trace)


def test_aggregate_trace_cpu_hours_uses_cpus_not_pct(tmp_path: Path) -> None:
    """Regression: cpu-hours must use `realtime × cpus`, not `realtime × %cpu / 100`.

    A multi-cpu task running a single-threaded inner command has cpus=N but
    %cpu near 100, and the canonical formula bills the full allocation.
    """
    trace = _write_trace(
        tmp_path,
        "trace.tsv",
        [_trace_row("WIDE", 16, "2026-01-01 00:00:00", "2026-01-01 00:00:00", "2026-01-01 00:01:00", "1m")],
    )
    out = aggregate_trace(trace)
    # 60 seconds × 16 cpus / 3600 = 0.2666...
    assert out["processes"][0]["sum_cpu_h"] == pytest.approx(60 * 16 / 3600)


def test_compare_basic(tmp_path: Path) -> None:
    dev = _write_trace(
        tmp_path,
        "dev.tsv",
        [_trace_row("FOO", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:02:05", "2m")],
    )
    pr = _write_trace(
        tmp_path,
        "pr.tsv",
        [_trace_row("FOO", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:01:05", "1m")],
    )
    cmp = compare(aggregate_trace(dev), aggregate_trace(pr))
    foo = next(p for p in cmp["processes"] if p["process"] == "FOO")
    # PR is 60s vs dev 120s of realtime → cpu-h halves → delta_cpu_h.pct = -50%
    assert foo["delta_cpu_h"]["pct"] == pytest.approx(-50.0)


def test_compare_handles_process_only_in_one_side(tmp_path: Path) -> None:
    dev = _write_trace(
        tmp_path,
        "dev.tsv",
        [_trace_row("ONLY_DEV", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:01:05", "1m")],
    )
    pr = _write_trace(
        tmp_path,
        "pr.tsv",
        [_trace_row("ONLY_PR", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:01:05", "1m")],
    )
    cmp = compare(aggregate_trace(dev), aggregate_trace(pr))
    names = {p["process"] for p in cmp["processes"]}
    assert names == {"ONLY_DEV", "ONLY_PR"}
    only_pr = next(p for p in cmp["processes"] if p["process"] == "ONLY_PR")
    # Dev side has zero, so pct delta is undefined (None).
    assert only_pr["delta_cpu_h"]["pct"] is None


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (5.0, "5.0s"),
        (90.0, "1m 30s"),
        (3660.0, "1h 1m"),
        (0.5, "0.5s"),
    ],
)
def test_fmt_duration(seconds: float, expected: str) -> None:
    assert fmt_duration(seconds) == expected


@pytest.mark.parametrize(
    ("pct", "expected"),
    [
        (None, "n/a"),
        (0.0, "+0.0%"),
        (10.5, "+10.5%"),
        (-3.2, "-3.2%"),
    ],
)
def test_fmt_pct(pct: float | None, expected: str) -> None:
    assert fmt_pct(pct) == expected


def test_render_markdown_compare(tmp_path: Path) -> None:
    dev = _write_trace(
        tmp_path,
        "dev.tsv",
        [_trace_row("FOO", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:02:05", "2m")],
    )
    pr = _write_trace(
        tmp_path,
        "pr.tsv",
        [_trace_row("FOO", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:01:05", "1m")],
    )
    dev_agg = aggregate_trace(dev)
    pr_agg = aggregate_trace(pr)
    payload: Payload = Payload(
        traces=[
            TraceEntry(name="dev", path=str(dev), cohort=dev_agg["cohort"], processes=dev_agg["processes"]),
            TraceEntry(name="pr", path=str(pr), cohort=pr_agg["cohort"], processes=pr_agg["processes"]),
        ],
        compare=compare(dev_agg, pr_agg),
    )
    md = render_markdown(payload)
    assert "## Cohort" in md
    assert "## Per-process" in md
    assert "| FOO |" in md
    assert "dev runtime" in md and "pr runtime" in md and "Δ cpu-h" in md


def test_render_markdown_single(tmp_path: Path) -> None:
    trace = _write_trace(
        tmp_path,
        "trace.tsv",
        [_trace_row("FOO", 1, "2026-01-01 00:00:00", "2026-01-01 00:00:05", "2026-01-01 00:01:05", "1m")],
    )
    agg = aggregate_trace(trace)
    payload: Payload = Payload(
        traces=[TraceEntry(name="single", path=str(trace), cohort=agg["cohort"], processes=agg["processes"])]
    )
    md = render_markdown(payload)
    assert "## single" in md
    assert "Cohort wall" in md
    assert "Σ cpu-hours" in md
    assert "| FOO |" in md
