from collections import Counter
from typing import Any

import pytest
from count_reads_per_clade import (
    accumulate_clade_counts,
    ancestors,
    build_parent_map,
    count_direct_reads_per_taxid,
    is_duplicate,
    read_tsv,
    write_output_tsv,
)


def test_is_duplicate() -> None:
    # Test case where read is not a duplicate (seq_id matches exemplar)
    read_not_duplicate = {"seq_id": "read123", "prim_align_dup_exemplar": "read123"}
    assert not is_duplicate(read_not_duplicate)

    # Test case where read is a duplicate (seq_id differs from exemplar)
    read_is_duplicate = {"seq_id": "read456", "prim_align_dup_exemplar": "read123"}
    assert is_duplicate(read_is_duplicate)

    # Test KeyError when seq_id is missing
    with pytest.raises(KeyError):
        is_duplicate({"prim_align_dup_exemplar": "read123"})

    # Test KeyError when prim_align_dup_exemplar is missing
    with pytest.raises(KeyError):
        is_duplicate({"seq_id": "read123"})

    # Test KeyError when both fields are missing
    with pytest.raises(KeyError):
        is_duplicate({})


def test_count_direct_reads_per_taxid() -> None:
    # Test basic counting with some duplicates
    read_data = [
        {
            "aligner_taxid_lca": "100",
            "seq_id": "read1",
            "prim_align_dup_exemplar": "read1",
            "group": "sample1",
        },  # not duplicate
        {
            "aligner_taxid_lca": "100",
            "seq_id": "read2",
            "prim_align_dup_exemplar": "read1",
            "group": "sample1",
        },  # duplicate
        {
            "aligner_taxid_lca": "200",
            "seq_id": "read3",
            "prim_align_dup_exemplar": "read3",
            "group": "sample1",
        },  # not duplicate
        {
            "aligner_taxid_lca": "100",
            "seq_id": "read4",
            "prim_align_dup_exemplar": "read4",
            "group": "sample1",
        },  # not duplicate
    ]

    total, dedup = count_direct_reads_per_taxid(iter(read_data), "sample1")

    # Total counts: taxid 100 has 3 reads, taxid 200 has 1 read
    assert total[100] == 3
    assert total[200] == 1

    # Deduplicated counts: taxid 100 has 2 non-duplicate reads, taxid 200 has 1
    assert dedup[100] == 2
    assert dedup[200] == 1

    # Test with custom taxid field
    read_data = [
        {
            "custom_taxid": "50",
            "seq_id": "read1",
            "prim_align_dup_exemplar": "read1",
            "group": "test_group",
        }
    ]
    total, dedup = count_direct_reads_per_taxid(
        iter(read_data), "test_group", taxid_field="custom_taxid"
    )
    assert total[50] == 1
    assert dedup[50] == 1

    # Test with empty data
    total, dedup = count_direct_reads_per_taxid(iter([]), "empty_group")
    assert len(total) == 0
    assert len(dedup) == 0

    # Test return types are Counters
    total, dedup = count_direct_reads_per_taxid(iter([]), "empty_group")
    assert isinstance(total, Counter)
    assert isinstance(dedup, Counter)


def test_count_direct_reads_per_taxid_group_validation() -> None:
    """Test that count_direct_reads_per_taxid validates group field correctly."""
    # Test that assertion fails when group doesn't match
    read_data_with_wrong_group = [
        {
            "aligner_taxid_lca": "100",
            "seq_id": "read1",
            "prim_align_dup_exemplar": "read1",
            "group": "wrong_group",
        }
    ]

    with pytest.raises(
        AssertionError, match="Expected group 'expected_group', found 'wrong_group'"
    ):
        count_direct_reads_per_taxid(iter(read_data_with_wrong_group), "expected_group")

    # Test with mixed groups (should fail on first mismatch)
    read_data_mixed = [
        {
            "aligner_taxid_lca": "100",
            "seq_id": "read1",
            "prim_align_dup_exemplar": "read1",
            "group": "correct_group",
        },
        {
            "aligner_taxid_lca": "200",
            "seq_id": "read2",
            "prim_align_dup_exemplar": "read2",
            "group": "wrong_group",
        },
    ]

    with pytest.raises(
        AssertionError, match="Expected group 'correct_group', found 'wrong_group'"
    ):
        count_direct_reads_per_taxid(iter(read_data_mixed), "correct_group")


def test_build_parent_map() -> None:
    # Test basic map building: 1->2, 1->3, 2->4
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "3", "parent_taxid": "1"},
        {"taxid": "4", "parent_taxid": "2"},
    ]
    result = build_parent_map(iter(tax_data))
    expected = {2: 1, 3: 1, 4: 2}
    assert result == expected

    # Test with custom field names
    tax_data = [
        {"child_id": "10", "parent_id": "5"},
        {"child_id": "11", "parent_id": "5"},
    ]
    result = build_parent_map(
        iter(tax_data), child_field="child_id", parent_field="parent_id"
    )
    expected = {10: 5, 11: 5}
    assert result == expected

    # Test with empty data
    result = build_parent_map(iter([]))
    assert result == {}

    # Test string to int conversion (keys and values are integers)
    tax_data = [{"taxid": "100", "parent_taxid": "50"}]
    result = build_parent_map(iter(tax_data))
    assert result == {100: 50}


def test_build_parent_map_ncbi_root() -> None:
    """Test that build_parent_map handles NCBI root (taxid 1, parent_taxid 1)."""
    tax_data = [
        {"taxid": "1", "parent_taxid": "1"},  # NCBI root
        {"taxid": "2", "parent_taxid": "1"},  # Child of root
        {"taxid": "3", "parent_taxid": "1"},  # Another child of root
    ]
    result = build_parent_map(iter(tax_data))
    assert result == {1: 1, 2: 1, 3: 1}


def test_build_parent_map_duplicate_child_error() -> None:
    """Test that build_parent_map raises when a child taxid appears twice."""
    # Child taxid 2 appears with two different parents
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "2", "parent_taxid": "3"},  # same child, different parent
    ]
    with pytest.raises(
        ValueError, match="Child taxid 2 appears multiple times in taxdb"
    ):
        build_parent_map(iter(tax_data))

    # Exact duplicate relationship
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "2", "parent_taxid": "1"},  # exact duplicate
    ]
    with pytest.raises(
        ValueError, match="Child taxid 2 appears multiple times in taxdb"
    ):
        build_parent_map(iter(tax_data))


def test_ancestors() -> None:
    # Lineage walk from a leaf up to an implicit root (taxid 1 has no row of its own)
    parent_map = {2: 1, 3: 1, 4: 2}
    assert list(ancestors(4, parent_map)) == [4, 2, 1]
    assert list(ancestors(3, parent_map)) == [3, 1]
    # An implicit root (appears only as a parent) yields just itself
    assert list(ancestors(1, parent_map)) == [1]

    # Explicit NCBI root (self-parent) terminates the walk
    parent_map = {1: 1, 2: 1, 4: 2}
    assert list(ancestors(4, parent_map)) == [4, 2, 1]
    assert list(ancestors(1, parent_map)) == [1]

    # A self-parent node is treated as a root (yields just itself, no error)
    assert list(ancestors(2, {2: 2})) == [2]


def test_ancestors_cycle_error() -> None:
    """Test that a cyclic lineage is detected while walking ancestors."""
    # Two-node cycle
    with pytest.raises(ValueError, match="Cycle detected in taxdb"):
        list(ancestors(1, {1: 2, 2: 1}))

    # Three-node cycle
    with pytest.raises(ValueError, match="Cycle detected in taxdb"):
        list(ancestors(1, {1: 2, 2: 3, 3: 1}))


def test_accumulate_clade_counts() -> None:
    # Tree: 1->2, 1->3, 2->4 (taxid 1 is an implicit root)
    # Direct counts {1:5, 2:10, 3:7, 4:3} give clade counts:
    # - Node 4: 3, Node 3: 7, Node 2: 13 (10+3), Node 1: 25 (5+10+7+3)
    parent_map = {2: 1, 3: 1, 4: 2}
    direct_total = Counter({1: 5, 2: 10, 3: 7, 4: 3})
    clade_total, clade_dedup, sparse_tree, dropped = accumulate_clade_counts(
        direct_total, Counter(), parent_map
    )
    assert clade_total == Counter({1: 25, 2: 13, 3: 7, 4: 3})
    assert clade_dedup == Counter()  # no dedup counts supplied
    assert dropped == 0
    # Sparse tree should hold only the touched edges
    assert dict(sparse_tree) == {1: {2, 3}, 2: {4}}

    # Total and dedup counters are propagated independently in a single pass
    clade_total, clade_dedup, _, _ = accumulate_clade_counts(
        Counter({4: 3}), Counter({4: 2}), {2: 1, 4: 2}
    )
    assert clade_total == Counter({4: 3, 2: 3, 1: 3})
    assert clade_dedup == Counter({4: 2, 2: 2, 1: 2})

    # Reads on taxids not present in the taxonomy are dropped and tallied
    direct_total = Counter({1: 3, 2: 7, 999: 100})
    clade_total, _, _, dropped = accumulate_clade_counts(
        direct_total, Counter(), {2: 1}
    )
    assert clade_total == Counter({1: 10, 2: 7})  # 999 ignored
    assert dropped == 100

    # Empty input yields empty results
    clade_total, clade_dedup, sparse_tree, dropped = accumulate_clade_counts(
        Counter(), Counter(), {2: 1}
    )
    assert clade_total == Counter()
    assert clade_dedup == Counter()
    assert dict(sparse_tree) == {}
    assert dropped == 0

    # Return types
    clade_total, clade_dedup, _, _ = accumulate_clade_counts(
        Counter({1: 1}), Counter({1: 1}), {2: 1}
    )
    assert isinstance(clade_total, Counter)
    assert isinstance(clade_dedup, Counter)


# Integration tests covering nf-test scenarios


@pytest.mark.parametrize(
    "missing_column",
    ["seq_id", "prim_align_dup_exemplar", "aligner_taxid_lca", "group"],
)
def test_missing_reads_columns(tsv_factory: Any, missing_column: str) -> None:
    """Test that missing required columns in reads file raise KeyError."""
    # Start with all required columns and appropriate test values
    all_columns = ["seq_id", "prim_align_dup_exemplar", "aligner_taxid_lca", "group"]
    test_values = ["read1", "read1", "100", "test"]

    # Remove the missing column and its corresponding value
    columns = []
    values = []
    for col, val in zip(all_columns, test_values, strict=True):
        if col != missing_column:
            columns.append(col)
            values.append(val)

    # Create reads file with missing column
    reads_content = "\t".join(columns) + "\n" + "\t".join(values) + "\n"
    reads_file = tsv_factory.create_plain("reads.tsv", reads_content)

    # Should raise KeyError for missing column
    with pytest.raises(KeyError, match=missing_column):
        count_direct_reads_per_taxid(read_tsv(reads_file), "test")


@pytest.mark.parametrize(
    "missing_column",
    ["taxid", "parent_taxid"],
)
def test_missing_taxonomy_columns(tsv_factory: Any, missing_column: str) -> None:
    """Test that missing required columns in taxonomy file raise KeyError."""
    # Start with all required columns
    all_columns = ["taxid", "parent_taxid"]
    # Remove the missing column
    columns = [col for col in all_columns if col != missing_column]

    # Create taxonomy file with missing column
    tax_content = "\t".join(columns) + "\n100\n"
    tax_file = tsv_factory.create_plain("taxonomy.tsv", tax_content)

    # Should raise KeyError for missing column
    with pytest.raises(KeyError, match=missing_column):
        build_parent_map(read_tsv(tax_file))


def test_group_mismatch_error(tsv_factory: Any) -> None:
    """Test that group mismatch raises AssertionError."""
    # Create reads file with group "test"
    reads_content = "seq_id\tprim_align_dup_exemplar\taligner_taxid_lca\tgroup\nread1\tread1\t100\ttest\n"
    reads_file = tsv_factory.create_plain("reads.tsv", reads_content)

    # Try to process with wrong group
    with pytest.raises(
        AssertionError, match="Expected group 'wrong_group', found 'test'"
    ):
        count_direct_reads_per_taxid(read_tsv(reads_file), "wrong_group")


def test_header_only_reads_file(tsv_factory: Any) -> None:
    """Test that header-only reads file produces header-only output."""
    # Create header-only reads file
    reads_content = "seq_id\tprim_align_dup_exemplar\taligner_taxid_lca\tgroup\n"
    reads_file = tsv_factory.create_plain("reads.tsv", reads_content)

    # Create valid taxonomy file
    tax_content = "taxid\tparent_taxid\n100\t1\n"
    tax_file = tsv_factory.create_plain("taxonomy.tsv", tax_content)

    # Process the files
    direct_total, direct_dedup = count_direct_reads_per_taxid(
        read_tsv(reads_file), "test"
    )
    parent_map = build_parent_map(read_tsv(tax_file))
    clade_total, clade_dedup, sparse_tree, dropped = accumulate_clade_counts(
        direct_total, direct_dedup, parent_map
    )
    assert dropped == 0

    # Write output
    output_file = tsv_factory.get_path("output.tsv.gz")
    write_output_tsv(
        output_file,
        "test",
        parent_map,
        sparse_tree,
        direct_total,
        direct_dedup,
        clade_total,
        clade_dedup,
    )

    # Read and verify output is header-only
    output_content = tsv_factory.read_gzip(output_file).strip()
    lines = output_content.split("\n")

    # Should have exactly 1 line (header only)
    assert len(lines) == 1

    # Verify header
    expected_header = "group\ttaxid\tparent_taxid\treads_direct_total\treads_direct_dedup\treads_clade_total\treads_clade_dedup"
    assert lines[0] == expected_header
