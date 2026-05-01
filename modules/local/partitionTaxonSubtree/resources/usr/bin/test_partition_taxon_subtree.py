"""Tests for partition_taxon_subtree.py."""

from pathlib import Path

import pytest
from partition_taxon_subtree import (
    build_child_map,
    compute_subtree_sizes,
    partition,
    partition_subtree,
)

# Hand-built taxonomy fragment.
#
#   1 (root)
#   ├── 2  (superkingdom)
#   │   └── 1423 (species)
#   └── 10239 (superkingdom: Viruses)
#       ├── 12475 (species)
#       │   ├── 2847173 (no rank)
#       │   └── 2847174 (no rank)
#       └── 10665 (species)
#           ├── 200001
#           ├── 200002
#           └── 200003
NODES_DMP_CONTENT = "\n".join([
    "1\t|\t1\t|\tno rank\t|\t\t|",
    "2\t|\t1\t|\tsuperkingdom\t|\t\t|",
    "10239\t|\t1\t|\tsuperkingdom\t|\t\t|",
    "1423\t|\t2\t|\tspecies\t|\t\t|",
    "12475\t|\t10239\t|\tspecies\t|\t\t|",
    "10665\t|\t10239\t|\tspecies\t|\t\t|",
    "2847173\t|\t12475\t|\tno rank\t|\t\t|",
    "2847174\t|\t12475\t|\tno rank\t|\t\t|",
    "200001\t|\t10665\t|\tno rank\t|\t\t|",
    "200002\t|\t10665\t|\tno rank\t|\t\t|",
    "200003\t|\t10665\t|\tno rank\t|\t\t|",
    "",
])

@pytest.fixture()
def nodes_dmp_path(tmp_path: Path) -> Path:
    path = tmp_path / "nodes.dmp"
    path.write_text(NODES_DMP_CONTENT)
    return path


class TestBuildChildMap:
    def test_excludes_root_self_reference(self, nodes_dmp_path: Path) -> None:
        children = build_child_map(str(nodes_dmp_path))
        # The "1 -> 1" self-edge must be dropped so root-only trees don't
        # recurse forever.
        assert "1" not in children.get("1", [])

    def test_known_edges(self, nodes_dmp_path: Path) -> None:
        children = build_child_map(str(nodes_dmp_path))
        assert sorted(children["10239"]) == ["10665", "12475"]
        assert sorted(children["12475"]) == ["2847173", "2847174"]
        assert sorted(children["10665"]) == ["200001", "200002", "200003"]


class TestComputeSubtreeSizes:
    def test_subtree_sizes(self, nodes_dmp_path: Path) -> None:
        children = build_child_map(str(nodes_dmp_path))
        sizes = compute_subtree_sizes(children, "10239")
        assert sizes == {
            "10239": 8,    # self + 12475 (3) + 10665 (4)
            "12475": 3,    # self + 2 children
            "10665": 4,    # self + 3 children
            "2847173": 1,
            "2847174": 1,
            "200001": 1,
            "200002": 1,
            "200003": 1,
        }

    def test_leaf_size_is_one(self, nodes_dmp_path: Path) -> None:
        children = build_child_map(str(nodes_dmp_path))
        sizes = compute_subtree_sizes(children, "2847173")
        assert sizes == {"2847173": 1}


class TestPartitionSubtree:
    @pytest.mark.parametrize(("max_size", "expected"), [
        # Root subtree fits: emit the root unchanged.
        (10, ["10239"]),
        (8, ["10239"]),
        # Root is over but each immediate child is under: descend one level.
        (4, ["12475", "10665"]),
        # 10665's subtree (4) overflows but 12475's (3) fits: only 10665 splits.
        (3, ["12475", "200001", "200002", "200003"]),
        # Both children overflow (3 > 2 and 4 > 2): emit all five leaves.
        (2, ["2847173", "2847174", "200001", "200002", "200003"]),
        (1, ["2847173", "2847174", "200001", "200002", "200003"]),
    ], ids=["root_fits_loose", "root_fits_exact", "splits_at_children",
            "partial_descend", "leaves_only_threshold_2", "leaves_only_threshold_1"])
    def test_partition_thresholds(self, nodes_dmp_path: Path, max_size: int,
                                  expected: list[str]) -> None:
        children = build_child_map(str(nodes_dmp_path))
        sizes = compute_subtree_sizes(children, "10239")
        segments = partition_subtree(children, sizes, "10239", max_size)
        assert sorted(segments) == sorted(expected)


class TestPartition:
    def test_internal_node(self, nodes_dmp_path: Path) -> None:
        assert sorted(partition(str(nodes_dmp_path), "10239", 4)) == ["10665", "12475"]

    def test_leaf_returns_self(self, nodes_dmp_path: Path) -> None:
        assert partition(str(nodes_dmp_path), "2847173", 1) == ["2847173"]

    def test_nonexistent_taxid_returns_self(self, nodes_dmp_path: Path) -> None:
        assert partition(str(nodes_dmp_path), "99999", 50000) == ["99999"]

    def test_empty_file_returns_root(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty_nodes.dmp"
        empty.write_text("")
        assert partition(str(empty), "10239", 50000) == ["10239"]
