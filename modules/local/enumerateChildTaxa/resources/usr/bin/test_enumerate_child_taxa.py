"""Tests for enumerate-child-taxa.py."""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT_DIR = Path(__file__).parent
_SPEC = importlib.util.spec_from_file_location(
    "enumerate_child_taxa", _SCRIPT_DIR / "enumerate-child-taxa.py",
)
ect = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(ect)


# ─── Fixtures ─────────────────────────────────────────────────────────────────


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
    """Create a temporary nodes.dmp file with realistic content."""
    path = tmp_path / "nodes.dmp"
    path.write_text(NODES_DMP_CONTENT)
    return path


# ─── Tests for enumerate_children ─────────────────────────────────────────────


class TestEnumerateChildren:
    """Tests for the enumerate_children function."""

    def test_parent_with_multiple_children(self, nodes_dmp_path: Path) -> None:
        """Taxid 10239 has two direct children: 12475 and 10665."""
        result = ect.enumerate_children(str(nodes_dmp_path), "10239")
        assert sorted(result) == ["10665", "12475"]

    def test_parent_with_one_child(self, nodes_dmp_path: Path) -> None:
        """Taxid 12475 has one child: 2847173."""
        result = ect.enumerate_children(str(nodes_dmp_path), "12475")
        assert result == ["2847173"]

    def test_leaf_taxon_returns_self(self, nodes_dmp_path: Path) -> None:
        """Leaf taxid 2847173 has no children, so it returns itself."""
        result = ect.enumerate_children(str(nodes_dmp_path), "2847173")
        assert result == ["2847173"]

    def test_root_taxon(self, nodes_dmp_path: Path) -> None:
        """Root taxid 1 has children 2 and 10239 (excluding self-reference)."""
        result = ect.enumerate_children(str(nodes_dmp_path), "1")
        assert sorted(result) == ["10239", "2"]

    def test_nonexistent_taxid_returns_self(self, nodes_dmp_path: Path) -> None:
        """A taxid not present in the file returns itself (treated as leaf)."""
        result = ect.enumerate_children(str(nodes_dmp_path), "99999")
        assert result == ["99999"]

    def test_empty_file(self, tmp_path: Path) -> None:
        """An empty nodes.dmp returns the parent taxid itself."""
        empty_path = tmp_path / "empty_nodes.dmp"
        empty_path.write_text("")
        result = ect.enumerate_children(str(empty_path), "10239")
        assert result == ["10239"]

