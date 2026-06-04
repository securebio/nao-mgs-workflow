#!/usr/bin/env python3
"""Tests for check_process_tags.py."""

###########
# IMPORTS #
###########

from pathlib import Path

import pytest
from check_process_tags import find_tag_violations, scan_modules

#############
# FIXTURES  #
#############


def _proc(tag_literal: str) -> str:
    """A single-process file whose tag uses the given literal, with inputs
    declaring the variables exercised by the format/variable tests."""
    return (
        "process FOO {\n"
        f'    tag "{tag_literal}"\n'
        "    input:\n"
        "        tuple val(sample), val(group), val(taxid), val(stage_label)\n"
        "    script:\n"
        '        "a"\n'
        "}\n"
    )


TAGGED = _proc("id=${sample}")

UNTAGGED = """\
process FOO {
    label "small"
    input:
        tuple val(sample), path(x)
    script:
        "echo hi"
}
"""

# `tag` only appears inside the script body, not the directive section.
TAG_IN_SCRIPT_ONLY = """\
process FOO {
    label "small"
    script:
        "tag the output somehow"
}
"""

# exec: body (sentinel-style) with a literal (no-variable) tag.
EXEC_TAGGED = """\
process FOO {
    executor 'local'
    tag "id=util"
    exec:
        x = 1
}
"""

MULTI_MIXED = """\
process FOO {
    tag "id=a"
    script:
        "a"
}

process BAR {
    label "small"
    script:
        "b"
}
"""

# Regression: dev renamed the input to `accession_chunk` but the tag still
# interpolates the removed `taxid` variable. Well-formed, but unresolvable.
TAG_UNKNOWN_VAR = """\
process FOO {
    tag "id=index,name=${taxid}"
    input:
        path(accession_chunk)
    script:
        "a"
}
"""

TAG_RESOLVED_VAR = """\
process FOO {
    tag "id=index,name=${accession_chunk.baseName}"
    input:
        path(accession_chunk)
    script:
        "a"
}
"""


#######################################
# TESTS: find_tag_violations          #
#######################################


@pytest.mark.parametrize(
    "content,expected_names",
    [
        (TAGGED, []),
        (EXEC_TAGGED, []),
        (TAG_RESOLVED_VAR, []),
        (UNTAGGED, ["FOO"]),
        (TAG_IN_SCRIPT_ONLY, ["FOO"]),
        (MULTI_MIXED, ["BAR"]),
        (TAG_UNKNOWN_VAR, ["FOO"]),
        ("", []),
    ],
)
def test_find_tag_violations_names(content: str, expected_names: list[str]) -> None:
    assert [name for name, _ in find_tag_violations(content)] == expected_names


@pytest.mark.parametrize(
    "literal,valid",
    [
        ("id=${sample}", True),
        ("id=${group}", True),
        ("id=index", True),
        ("id=util", True),
        ("id=index,name=${taxid}", True),  # taxid declared as input in _proc
        ("id=${sample},stage=${stage_label}", True),
        ("id=${params.foo}", True),  # params is an allowed global
        ("${sample}", False),  # missing id= prefix
        ("sample=${x}", False),  # first key is not id
        ("id", False),  # no value
        ("id=", False),  # empty value
        ("id=a b", False),  # whitespace in value
        ("id=a,name", False),  # trailing component has no value
    ],
)
def test_find_tag_violations_format(literal: str, valid: bool) -> None:
    violations = find_tag_violations(_proc(literal))
    assert (len(violations) == 0) == valid, violations


def test_unknown_variable_reason_names_the_variable() -> None:
    [(name, reason)] = find_tag_violations(TAG_UNKNOWN_VAR)
    assert name == "FOO"
    assert "taxid" in reason


def test_missing_tag_reason() -> None:
    [(_, reason)] = find_tag_violations(UNTAGGED)
    assert "missing" in reason


def test_find_tag_violations_preserves_declaration_order() -> None:
    content = UNTAGGED.replace("FOO", "AAA") + UNTAGGED.replace("FOO", "BBB")
    assert [name for name, _ in find_tag_violations(content)] == ["AAA", "BBB"]


def test_dynamic_tag_expression_is_accepted() -> None:
    # A non-literal tag expression cannot be format- or variable-checked.
    body = "process FOO {\n    tag computeTag(sample)\n    script:\n        x\n}\n"
    assert find_tag_violations(body) == []


#######################################
# TESTS: scan_modules                 #
#######################################


def test_scan_modules_reports_only_violations(tmp_path: Path) -> None:
    (tmp_path / "foo").mkdir()
    (tmp_path / "bar").mkdir()
    (tmp_path / "foo" / "main.nf").write_text(TAGGED)
    (tmp_path / "bar" / "main.nf").write_text(UNTAGGED)
    violations = scan_modules(tmp_path)
    assert list(violations) == [tmp_path / "bar" / "main.nf"]
    assert violations[tmp_path / "bar" / "main.nf"][0][0] == "FOO"


def test_scan_modules_clean_tree_is_empty(tmp_path: Path) -> None:
    (tmp_path / "foo").mkdir()
    (tmp_path / "foo" / "main.nf").write_text(TAGGED)
    assert scan_modules(tmp_path) == {}


#######################################
# TESTS: integration (real repo tree) #
#######################################


def test_real_modules_local_all_tagged() -> None:
    """Every process in the repository's modules/local/ must be well-tagged."""
    modules_dir = Path(__file__).resolve().parent.parent / "modules" / "local"
    assert modules_dir.is_dir(), f"expected {modules_dir} to exist"
    violations = scan_modules(modules_dir)
    assert violations == {}, f"Tag violations found: {violations}"
