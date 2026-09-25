#!/usr/bin/env python3
"""Tests for check_fan_in.py."""

###########
# IMPORTS #
###########

from pathlib import Path

import pytest
from check_fan_in import find_unannotated_gathers, is_annotated, scan_sources

#######################################
# TESTS: is_annotated                 #
#######################################


@pytest.mark.parametrize(
    ("lines", "index", "expected"),
    [
        (["x = ch.collect() // complete: head node"], 0, True),
        (["// complete: head node", "x = ch.collect()"], 1, True),
        (["// complete: head node", "// more context", "x = ch.collect()"], 2, True),
        (["// complete: head node", "y = 1", "x = ch.collect()"], 2, False),
        (["// some other comment", "x = ch.collect()"], 1, False),
        (["x = ch.collect() // complete:"], 0, False),
    ],
)
def test_is_annotated(lines: list[str], index: int, expected: bool) -> None:
    assert is_annotated(lines, index) is expected


#######################################
# TESTS: find_unannotated_gathers     #
#######################################


@pytest.mark.parametrize(
    ("line", "flagged"),
    [
        ("x = ch.collect()", True),
        ("x = ch.collect(flat: false)", True),
        ('x = ch.collectFile(name: "a.txt")', True),
        ("x = ch.toList()", True),
        ("x = ch.toSortedList()", True),
        ("x = ch.count()", True),
        ("x = ch.reduce { a, b -> a + b }", True),
        ("    .groupTuple()", True),
        ("x = a.join(b, remainder: true)", True),
        ("x = a.join(b)", False),
        ("x = list.collect { it * 2 }", False),
        ("x = ch.map { it }", False),
        ("// ch.collect() in a comment", False),
    ],
)
def test_find_unannotated_gathers_operators(line: str, flagged: bool) -> None:
    assert bool(find_unannotated_gathers(line)) is flagged


def test_find_unannotated_gathers_reports_line_numbers() -> None:
    content = (
        "a = ch.map { it }\nb = ch.collect()\n// complete: sized\nc = ch.groupTuple()\n"
    )
    assert find_unannotated_gathers(content) == [(2, "b = ch.collect()")]


#######################################
# TESTS: scan_sources                 #
#######################################


def test_scan_sources_reports_only_unannotated(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "subworkflows" / "local" / "foo").mkdir(parents=True)
    (tmp_path / "workflows" / "run.nf").write_text(
        "x = ch.collect() // complete: head node\n"
    )
    bad = tmp_path / "subworkflows" / "local" / "foo" / "main.nf"
    bad.write_text("y = ch.groupTuple()\n")
    assert scan_sources(tmp_path) == {bad: [(1, "y = ch.groupTuple()")]}


#######################################
# TESTS: integration (real repo tree) #
#######################################


def test_real_workflows_annotate_every_gather() -> None:
    """Every gathering operator in the repository's workflows must be annotated."""
    root = Path(__file__).resolve().parent.parent
    assert (root / "workflows").is_dir(), f"expected {root / 'workflows'} to exist"
    assert scan_sources(root) == {}
