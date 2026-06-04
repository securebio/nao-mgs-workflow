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

TAGGED = """\
process FOO {
    label "small"
    tag "id=${sample}"
    input:
        path(x)
    script:
        "echo hi"
}
"""

UNTAGGED = """\
process FOO {
    label "small"
    input:
        path(x)
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

# exec: body (sentinel-style) with a tag directive.
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

# Tag directive present but the literal template is the wrong shape.
MALFORMED_NO_ID = """\
process FOO {
    tag "${sample}"
    script:
        "a"
}
"""

MALFORMED_WRONG_KEY = """\
process FOO {
    tag "sample=${x}"
    script:
        "a"
}
"""


def _name(content: str) -> str:
    """Build a single-process file body with the given tag literal."""
    return f'process FOO {{\n    tag "{content}"\n    script:\n        "a"\n}}\n'


#######################################
# TESTS: find_tag_violations          #
#######################################


@pytest.mark.parametrize(
    "content,expected_names",
    [
        (TAGGED, []),
        (EXEC_TAGGED, []),
        (UNTAGGED, ["FOO"]),
        (TAG_IN_SCRIPT_ONLY, ["FOO"]),
        (MULTI_MIXED, ["BAR"]),
        (MALFORMED_NO_ID, ["FOO"]),
        (MALFORMED_WRONG_KEY, ["FOO"]),
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
        ("id=index,name=${taxid}", True),
        ("id=${sample},stage=${stage_label}", True),
        ("${sample}", False),  # missing id= prefix
        ("sample=${x}", False),  # first key is not id
        ("id", False),  # no value
        ("id=", False),  # empty value
        ("id=a b", False),  # whitespace in value
        ("id=a,name", False),  # trailing component has no value
    ],
)
def test_find_tag_violations_format(literal: str, valid: bool) -> None:
    violations = find_tag_violations(_name(literal))
    assert (len(violations) == 0) == valid, violations


def test_find_tag_violations_reports_reason() -> None:
    [(name, reason)] = find_tag_violations(UNTAGGED)
    assert name == "FOO"
    assert "missing" in reason


def test_find_tag_violations_preserves_declaration_order() -> None:
    content = UNTAGGED.replace("FOO", "AAA") + UNTAGGED.replace("FOO", "BBB")
    assert [name for name, _ in find_tag_violations(content)] == ["AAA", "BBB"]


def test_dynamic_tag_expression_is_accepted() -> None:
    # A non-literal tag expression cannot be format-checked statically.
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
