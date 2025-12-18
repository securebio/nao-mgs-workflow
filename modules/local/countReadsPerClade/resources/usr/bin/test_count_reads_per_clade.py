from collections import Counter

import pytest
from count_reads_per_clade import (
    build_tree,
    children,
    count_direct_reads_per_taxid,
    get_clade_counts,
    is_duplicate,
    parents,
    read_tsv,
    roots,
    write_output_tsv,
)


def test_is_duplicate():
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


def test_parents():
    # Test with a simple tree: 1->2, 1->3, 2->4
    # Parents should be: {1, 2} (nodes that have children)
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "3", "parent_taxid": "1"},
        {"taxid": "4", "parent_taxid": "2"},
    ]
    tree = build_tree(iter(tax_data))
    result = parents(tree)
    expected = {1, 2}
    assert result == expected

    # Test with empty tree
    empty_tree = build_tree(iter([]))
    result = parents(empty_tree)
    assert result == set()

    # Test with single parent-child relationship
    tax_data = [{"taxid": "20", "parent_taxid": "10"}]
    single_tree = build_tree(iter(tax_data))
    result = parents(single_tree)
    assert result == {10}

    # Test return type is set
    tax_data = [{"taxid": "2", "parent_taxid": "1"}]
    tree = build_tree(iter(tax_data))
    result = parents(tree)
    assert isinstance(result, set)


def test_children():
    # Test with a simple tree: 1->2, 1->3, 2->4
    # Children should be: {2, 3, 4} (nodes that are children of other nodes)
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "3", "parent_taxid": "1"},
        {"taxid": "4", "parent_taxid": "2"},
    ]
    tree = build_tree(iter(tax_data))
    result = children(tree)
    expected = {2, 3, 4}
    assert result == expected

    # Test with empty tree
    empty_tree = build_tree(iter([]))
    result = children(empty_tree)
    assert result == set()

    # Test with single parent-child relationship
    tax_data = [{"taxid": "20", "parent_taxid": "10"}]
    single_tree = build_tree(iter(tax_data))
    result = children(single_tree)
    assert result == {20}

    # Test return type is set
    tax_data = [{"taxid": "2", "parent_taxid": "1"}]
    tree = build_tree(iter(tax_data))
    result = children(tree)
    assert isinstance(result, set)


def test_roots():
    # Test with a simple tree: 1->2, 1->3, 2->4
    # Root should be: {1} (parent that is never a child)
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "3", "parent_taxid": "1"},
        {"taxid": "4", "parent_taxid": "2"},
    ]
    tree = build_tree(iter(tax_data))
    result = roots(tree)
    expected = {1}
    assert result == expected

    # Test with empty tree
    empty_tree = build_tree(iter([]))
    result = roots(empty_tree)
    assert result == set()

    # Test with multiple roots (forest): 1->2, 3->4
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "4", "parent_taxid": "3"},
    ]
    forest = build_tree(iter(tax_data))
    result = roots(forest)
    expected = {1, 3}
    assert result == expected

    # Test with single parent-child relationship
    tax_data = [{"taxid": "20", "parent_taxid": "10"}]
    single_tree = build_tree(iter(tax_data))
    result = roots(single_tree)
    assert result == {10}

    # Test return type is set
    tax_data = [{"taxid": "2", "parent_taxid": "1"}]
    tree = build_tree(iter(tax_data))
    result = roots(tree)
    assert isinstance(result, set)


def test_build_tree():
    # Test basic tree building
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "3", "parent_taxid": "1"},
        {"taxid": "4", "parent_taxid": "2"},
    ]
    result = build_tree(iter(tax_data))
    expected = {1: {2, 3}, 2: {4}}
    assert result == expected

    # Test with custom field names
    tax_data = [
        {"child_id": "10", "parent_id": "5"},
        {"child_id": "11", "parent_id": "5"},
    ]
    result = build_tree(
        iter(tax_data), child_field="child_id", parent_field="parent_id"
    )
    expected = {5: {10, 11}}
    assert result == expected

    # Test with empty data
    result = build_tree(iter([]))
    assert result == {}

    # Test string to int conversion
    tax_data = [{"taxid": "100", "parent_taxid": "50"}]
    result = build_tree(iter(tax_data))
    # Keys and values should be integers
    assert 50 in result
    assert 100 in result[50]


def test_build_tree_duplicate_child_error():
    """Test that build_tree raises an error when a child has multiple parents."""
    # Child taxid 2 appears with two different parents
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "2", "parent_taxid": "3"},  # same child, different parent
    ]
    with pytest.raises(
        ValueError, match="Child taxid 2 appears multiple times in taxdb"
    ):
        build_tree(iter(tax_data))

    # Test with more complex case
    tax_data = [
        {"taxid": "10", "parent_taxid": "5"},
        {"taxid": "20", "parent_taxid": "5"},
        {"taxid": "10", "parent_taxid": "6"},  # duplicate child, different parent
    ]
    with pytest.raises(
        ValueError, match="Child taxid 10 appears multiple times in taxdb"
    ):
        build_tree(iter(tax_data))

    # Exact duplicate relationship
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "2", "parent_taxid": "1"},  # exact duplicate
    ]
    with pytest.raises(
        ValueError, match="Child taxid 2 appears multiple times in taxdb"
    ):
        build_tree(iter(tax_data))

    # Exact duplicate mixed with valid relationships
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "3", "parent_taxid": "1"},  # valid
        {"taxid": "2", "parent_taxid": "1"},  # duplicate of first
    ]
    with pytest.raises(
        ValueError, match="Child taxid 2 appears multiple times in taxdb"
    ):
        build_tree(iter(tax_data))


def test_build_tree_cycle_error():
    """Test that build_tree raises an error when there are cycles in the tree."""
    # NCBI root (taxid 1 with parent_taxid 1) should NOT raise an error
    tax_data = [
        {"taxid": "1", "parent_taxid": "1"},
    ]
    # This should work without raising an error
    result = build_tree(iter(tax_data))
    # Root should have no children since it doesn't add itself as a child
    assert result == {}
    
    # Self-loop with non-root taxid should still be an error
    tax_data = [
        {"taxid": "2", "parent_taxid": "2"},
    ]
    with pytest.raises(ValueError, match="Cycle detected in taxdb"):
        build_tree(iter(tax_data))

    # Two-node cycle
    tax_data = [
        {"taxid": "1", "parent_taxid": "2"},
        {"taxid": "2", "parent_taxid": "1"},
    ]
    with pytest.raises(ValueError, match="Cycle detected in taxdb"):
        build_tree(iter(tax_data))

    # Three-node cycle
    tax_data = [
        {"taxid": "1", "parent_taxid": "2"},
        {"taxid": "2", "parent_taxid": "3"},
        {"taxid": "3", "parent_taxid": "1"},
    ]
    with pytest.raises(ValueError, match="Cycle detected in taxdb"):
        build_tree(iter(tax_data))

    # Complex case with valid tree plus cycle
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},  # valid
        {"taxid": "3", "parent_taxid": "1"},  # valid
        {"taxid": "4", "parent_taxid": "2"},  # valid
        {"taxid": "5", "parent_taxid": "6"},  # start of cycle
        {"taxid": "6", "parent_taxid": "7"},
        {"taxid": "7", "parent_taxid": "5"},  # completes cycle
    ]
    with pytest.raises(ValueError, match="Cycle detected in taxdb"):
        build_tree(iter(tax_data))


def test_count_direct_reads_per_taxid():
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


def test_count_direct_reads_per_taxid_group_validation():
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
        count_direct_reads_per_taxid(read_data_with_wrong_group, "expected_group")

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
        count_direct_reads_per_taxid(read_data_mixed, "correct_group")


def test_build_tree_ncbi_root():
    """Test that build_tree handles NCBI root (taxid 1, parent_taxid 1) correctly."""
    # Test NCBI root with children
    tax_data = [
        {"taxid": "1", "parent_taxid": "1"},  # NCBI root
        {"taxid": "2", "parent_taxid": "1"},  # Child of root
        {"taxid": "3", "parent_taxid": "1"},  # Another child of root
    ]
    result = build_tree(iter(tax_data))
    expected = {1: {2, 3}}
    assert result == expected
    
    # Test that root is correctly identified
    root_nodes = roots(result)
    assert root_nodes == {1}


def test_get_clade_counts():
    # Test with simple tree: 1->2, 1->3, 2->4
    # If node counts are: {1:5, 2:10, 3:7, 4:3}
    # Then clade counts should be:
    # - Node 4: 3 (only itself)
    # - Node 3: 7 (only itself)
    # - Node 2: 13 (10 + 3 from child 4)
    # - Node 1: 25 (5 + 10 + 3 + 7 from all descendants)
    tax_data = [
        {"taxid": "2", "parent_taxid": "1"},
        {"taxid": "3", "parent_taxid": "1"},
        {"taxid": "4", "parent_taxid": "2"},
    ]
    tree = build_tree(iter(tax_data))
    node_counts = Counter({1: 5, 2: 10, 3: 7, 4: 3})

    result = get_clade_counts(node_counts, tree)
    expected = Counter({1: 25, 2: 13, 3: 7, 4: 3})
    assert result == expected

    # Test with nodes that have zero counts
    tax_data = [{"taxid": "2", "parent_taxid": "1"}]
    tree = build_tree(iter(tax_data))
    node_counts = Counter({1: 5})  # node 2 has 0 count
    result = get_clade_counts(node_counts, tree)
    expected = Counter({1: 5, 2: 0})
    assert result == expected

    # Test with empty tree
    tree = build_tree(iter([]))
    node_counts = Counter({1: 5})  # node 1 not in tree
    result = get_clade_counts(node_counts, tree)
    expected = Counter({})  # empty result since no nodes in tree
    assert result == expected

    # Test with node_counts containing nodes not in tree (should be ignored)
    tax_data = [{"taxid": "2", "parent_taxid": "1"}]
    tree = build_tree(iter(tax_data))
    node_counts = Counter({1: 3, 2: 7, 999: 100})  # 999 not in tree
    result = get_clade_counts(node_counts, tree)
    expected = Counter({1: 10, 2: 7})  # 999 ignored

    # Test return type is Counter
    tax_data = [{"taxid": "2", "parent_taxid": "1"}]
    tree = build_tree(iter(tax_data))
    node_counts = Counter({1: 1})
    result = get_clade_counts(node_counts, tree)
    assert isinstance(result, Counter)


# Integration tests covering nf-test scenarios


@pytest.mark.parametrize(
    "missing_column",
    ["seq_id", "prim_align_dup_exemplar", "aligner_taxid_lca", "group"],
)
def test_missing_reads_columns(tsv_factory, missing_column):
    """Test that missing required columns in reads file raise KeyError."""
    # Start with all required columns and appropriate test values
    all_columns = ["seq_id", "prim_align_dup_exemplar", "aligner_taxid_lca", "group"]
    test_values = ["read1", "read1", "100", "test"]

    # Remove the missing column and its corresponding value
    columns = []
    values = []
    for col, val in zip(all_columns, test_values):
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
def test_missing_taxonomy_columns(tsv_factory, missing_column):
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
        build_tree(read_tsv(tax_file))


def test_group_mismatch_error(tsv_factory):
    """Test that group mismatch raises AssertionError."""
    # Create reads file with group "test"
    reads_content = "seq_id\tprim_align_dup_exemplar\taligner_taxid_lca\tgroup\nread1\tread1\t100\ttest\n"
    reads_file = tsv_factory.create_plain("reads.tsv", reads_content)

    # Try to process with wrong group
    with pytest.raises(AssertionError, match="Expected group 'wrong_group', found 'test'"):
        count_direct_reads_per_taxid(read_tsv(reads_file), "wrong_group")


def test_header_only_reads_file(tsv_factory):
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
    tree = build_tree(read_tsv(tax_file))
    clade_total = get_clade_counts(direct_total, tree)
    clade_dedup = get_clade_counts(direct_dedup, tree)

    # Write output
    output_file = tsv_factory.get_path("output.tsv.gz")
    write_output_tsv(
        output_file, "test", tree, direct_total, direct_dedup, clade_total, clade_dedup
    )

    # Read and verify output is header-only
    output_content = tsv_factory.read_gzip(output_file).strip()
    lines = output_content.split('\n')

    # Should have exactly 1 line (header only)
    assert len(lines) == 1

    # Verify header
    expected_header = "group\ttaxid\tparent_taxid\treads_direct_total\treads_direct_dedup\treads_clade_total\treads_clade_dedup"
    assert lines[0] == expected_header
