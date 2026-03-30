"""Tests for enumerate_child_taxa.py."""

from pathlib import Path

import pytest
from enumerate_child_taxa import enumerate_children

NODES_DMP_CONTENT = """\
1\t|\t1\t|\tno rank\t|\t\t|\t8\t|\t0\t|\t1\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
2\t|\t1\t|\tsuperkingdom\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
10239\t|\t1\t|\tsuperkingdom\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
12475\t|\t10239\t|\tspecies\t|\t\t|\t0\t|\t1\t|\t11\t|\t1\t|\t0\t|\t1\t|\t0\t|\t0\t|\t\t|
10665\t|\t10239\t|\tspecies\t|\t\t|\t0\t|\t1\t|\t11\t|\t1\t|\t0\t|\t1\t|\t0\t|\t0\t|\t\t|
2847173\t|\t12475\t|\tno rank\t|\t\t|\t0\t|\t1\t|\t11\t|\t1\t|\t0\t|\t1\t|\t0\t|\t0\t|\t\t|
"""

@pytest.fixture()
def nodes_dmp_path(tmp_path: Path) -> Path:
    path = tmp_path / "nodes.dmp"
    path.write_text(NODES_DMP_CONTENT)
    return path

class TestEnumerateChildren:
    @pytest.mark.parametrize(("parent_taxid", "expected"), [
        ("1", ["10239", "2"]),  # Root: children 2 and 10239 (excl self-ref)
        ("10239", ["10665", "12475"]),  # Internal node: two children
        ("12475", ["2847173"]),  # Internal node: one child
        ("2847173", ["2847173"]),  # Leaf: returns self
        ("99999", ["99999"]),  # Nonexistent taxid: treated as leaf
    ], ids=["root", "internal_two_children", "internal_one_child", "leaf", "nonexistent"])
    def test_enumerate_children(self, nodes_dmp_path: Path, parent_taxid: str,
                                expected: list[str]) -> None:
        assert sorted(enumerate_children(str(nodes_dmp_path), parent_taxid)) == expected

    def test_empty_file(self, tmp_path: Path) -> None:
        """An empty nodes.dmp returns the parent taxid itself."""
        empty = tmp_path / "empty_nodes.dmp"
        empty.write_text("")
        assert enumerate_children(str(empty), "10239") == ["10239"]
