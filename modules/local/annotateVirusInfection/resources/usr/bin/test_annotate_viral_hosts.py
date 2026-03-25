#!/usr/bin/env python
"""
Comprehensive test suite for the annotate_viral_hosts module.

This file tests the viral host infection annotation pipeline, which determines
whether viruses infect specific host taxa based on Virus-Host DB data and 
taxonomic relationships.

Test Organization:
- Test classes are grouped by the function they test
- TestMarkAncestorInfectionsSingle: Core propagation logic from children to parents
- TestMarkDirectInfections: Initial infection status assignment
- TestExpandTaxid: Taxonomic tree expansion
- TestBuildVirusTree: Virus taxonomy structure creation
- TestMarkDescendantInfections: Propagation from parents to descendants
- TestAddDescendants: Helper for expanding taxid sets
- TestExcludeInfections: Handling special exclusion cases
- TestMarkAncestorInfections: Wrapper orchestrating ancestor marking
- TestGetHostTaxids: Host taxonomy expansion
- TestGetVirusHostMapping: Virus-Host DB parsing
- TestAnnotateVirusDbSingle: Single host annotation function

Note: Tests for check_infection and annotate_virus_db have been omitted as they would
require extensive mocking of complex dependencies without providing significant value
beyond the existing tests of their component functions. Integration testing for these
functions exists in tests/modules/local/annotateVirusInfection/main.nf.test.

Key concepts for mark_ancestor_infections_single:
- Parent nodes can only have initial states of UNRESOLVED (-1) or INCONSISTENT (0)
  (MATCH nodes are pre-marked as "checked" and never processed by this function)
- Child nodes can have any of the six states: UNRESOLVED, MATCH, INCONSISTENT,
  CONSISTENT, UNCLEAR, or MAYBE_INCONSISTENT
- The "Untested before/after" comments help track test coverage progression
- Helper functions at the top generate test case combinations
- Comments track which state combinations remain untested after each test

State meanings:
- MATCH (1): Confirmed to infect the host
- INCONSISTENT (0): Confirmed NOT to infect the host
- CONSISTENT (3): Likely infects (has MATCH descendants, no INCONSISTENT ones)
- UNCLEAR (2): Uncertain (has both MATCH and INCONSISTENT descendants)
- UNRESOLVED (-1): No data available (temporary state)
- MAYBE_INCONSISTENT (-2): Has INCONSISTENT descendants (temporary state)
"""

# =======================================================================
# Preamble
# =======================================================================

from pathlib import Path

import pytest
import pandas as pd
import itertools
from typing import Callable, TypeAlias, Protocol

from annotate_viral_hosts import (
    mark_ancestor_infections_single,
    mark_direct_infections,
    expand_taxid,
    build_virus_tree,
    mark_descendant_infections,
    add_descendants,
    exclude_infections,
    mark_ancestor_infections,
    get_host_taxids,
    get_virus_host_mapping,
    annotate_virus_db_single,
    INCONSISTENT,
    MATCH,
    UNCLEAR,
    CONSISTENT,
    UNRESOLVED,
    MAYBE_INCONSISTENT,
)

# =======================================================================
# Type definitions and constants
# =======================================================================

State: TypeAlias = int
StateTuple: TypeAlias = tuple[State, ...]


class DataFrameFactory(Protocol):
    """Protocol for the dataframe factory fixture. Mainly used for type hinting."""

    def __call__(
        self,
        parent_status: int,
        child_statuses: dict[str, int],
        checked_states: dict[str, bool] | None = None,
    ) -> pd.DataFrame: ...


ALL_STATES: list[State] = [
    INCONSISTENT,
    MATCH,
    UNCLEAR,
    CONSISTENT,
    UNRESOLVED,
    MAYBE_INCONSISTENT,
]


# =======================================================================
# Test case generation helpers
# =======================================================================
def generate_combinations_with_state(
    state: State, num_children: int = 3
) -> list[StateTuple]:
    """Generate all combinations of states that include at least one instance of the given state."""
    all_combinations = itertools.product(ALL_STATES, repeat=num_children)
    return [
        case
        for case in all_combinations
        if state in case and len(set(case)) > 1  # Exclude uniform combinations
    ]


def generate_conflict_combinations(num_children: int = 3) -> list[StateTuple]:
    """Generate combinations of states with both inconsistent/maybe_inconsistent and match/consistent states."""
    inconsistent_group = {INCONSISTENT, MAYBE_INCONSISTENT}
    matched_group = {MATCH, CONSISTENT}
    all_combinations = itertools.product(ALL_STATES, repeat=num_children)
    return [
        combo
        for combo in all_combinations
        if any(item in inconsistent_group for item in combo)
        and any(item in matched_group for item in combo)
    ]


def generate_mixed_state_cases(
    child_states: list[State],
    parent_states: list[State],
    expected_logic: Callable[[tuple, int], int],
) -> list[StateTuple]:
    """Generate test cases for mixed child states with expected parent outcomes."""
    all_child_combos = itertools.product(child_states, repeat=3)
    # Filter out uniform combinations (all children have same state)
    mixed_combos = [combo for combo in all_child_combos if len(set(combo)) > 1]
    test_cases = []
    for children in mixed_combos:
        for initial_parent in parent_states:
            expected_parent = expected_logic(children, initial_parent)
            test_cases.append((*children, initial_parent, expected_parent))
    return test_cases


def inconsistent_propagation_logic(_: StateTuple, initial_parent: State) -> State:
    # We ignore children states because for this test case, all children are 
    # UNRESOLVED, MAYBE_INCONSISTENT, or INCONSISTENT. In this scenario,
    # only the parent's initial state determines the outcome.
    if initial_parent == INCONSISTENT:
        return INCONSISTENT
    return MAYBE_INCONSISTENT


# Generate propagation test cases
inconsistent_propagation_cases = generate_mixed_state_cases(
    child_states=[UNRESOLVED, INCONSISTENT, MAYBE_INCONSISTENT],
    parent_states=[UNRESOLVED, INCONSISTENT],
    expected_logic=inconsistent_propagation_logic,
)

consistent_propagation_cases = generate_mixed_state_cases(
    child_states=[UNRESOLVED, CONSISTENT, MATCH],
    parent_states=[UNRESOLVED, INCONSISTENT],
    expected_logic=lambda children, parent: CONSISTENT,
)


def generate_test_id(params: tuple) -> str:
    """Function to format test case IDs."""
    if len(params) == 5:
        c1, c2, c3, initial_p, expected_p = params
        return f"children({c1},{c2},{c3})-init({initial_p})-expect({expected_p})"
    else:
        return str(params)


# =======================================================================
# Fixtures
# =======================================================================


@pytest.fixture
def virus_taxonomy_tree() -> dict[str, set[str]]:
    """Create a test taxonomy tree with parent-child relationships."""

    return {
        "parent1": {"child1", "child2", "child3"},
        "child1": set(),
        "child2": set(),
        "child3": set(),
    }


@pytest.fixture
def dataframe_factory() -> DataFrameFactory:
    """Factory fixture to create test DataFrames with specified states."""

    def create_dataframe(
        parent_status: int,
        child_statuses: dict[str, int],
        checked_states: dict[str, bool] | None = None,
    ) -> pd.DataFrame:
        if checked_states is None:
            checked_states = {}
        rows = [
            {
                "taxid": "parent1",
                "status": parent_status,
                "checked": checked_states.get("parent1", False),
            }
        ]
        for child_id, status in child_statuses.items():
            rows.append(
                {
                    "taxid": child_id,
                    "status": status,
                    "checked": checked_states.get(child_id, True),
                }
            )

        df = pd.DataFrame(rows)
        df.set_index("taxid", inplace=True)
        return df

    return create_dataframe


@pytest.fixture
def host_taxonomy_nodes() -> pd.DataFrame:
    """Create a test NCBI nodes DataFrame for host taxonomy.
    
    Structure:
    9606 (human)
    ├── 741158
    └── 63221
    
    7742 (vertebrate)
    ├── 1476529
    ├── 7776
    └── 2662825
    """
    nodes_data = [
        {"taxid": "9606", "parent_taxid": "0"},  # Human root
        {"taxid": "741158", "parent_taxid": "9606"},
        {"taxid": "63221", "parent_taxid": "9606"},
        {"taxid": "7742", "parent_taxid": "0"},  # Vertebrate root
        {"taxid": "1476529", "parent_taxid": "7742"},
        {"taxid": "7776", "parent_taxid": "7742"},
        {"taxid": "2662825", "parent_taxid": "7742"},
    ]
    return pd.DataFrame(nodes_data)


@pytest.fixture  
def sample_virus_db() -> pd.DataFrame:
    """Create a sample virus database for testing."""
    return pd.DataFrame({
        "taxid": ["v1", "v2", "v3"],
        "parent_taxid": ["0", "0", "0"],
    })


@pytest.fixture
def sample_virus_tree() -> dict[str, set[str]]:
    """Create a sample virus tree for testing."""
    return {
        "0": {"v1", "v2", "v3"},
        "v1": set(),
        "v2": set(), 
        "v3": set(),
    }


# =======================================================================
# Test Classes
# =======================================================================

# =======================================================================
# Tests for mark_ancestor_infections_single
# =======================================================================

class TestUniformChildStates:
    # Untested before: All combinations
    @pytest.mark.parametrize(
        "child_state,expected_parent",
        [
            (UNRESOLVED, UNRESOLVED),
            (MATCH, MATCH),
            (INCONSISTENT, INCONSISTENT),
            (CONSISTENT, CONSISTENT),
            (UNCLEAR, UNCLEAR),
            (MAYBE_INCONSISTENT, MAYBE_INCONSISTENT),
        ],
    )
    def test_uniform_child_states_propagate_to_parent(
        self,
        dataframe_factory: DataFrameFactory,
        virus_taxonomy_tree: dict[str, set[str]],
        child_state: State,
        expected_parent: State,
    ) -> None:
        """If all children have the same state, the parent also has that state."""
        # Arrange
        child_statuses: dict[str, State] = {
            "child1": child_state,
            "child2": child_state,
            "child3": child_state,
        }
        df = dataframe_factory(
            parent_status=UNRESOLVED,
            child_statuses=child_statuses,
        )
        # Act
        result_df = mark_ancestor_infections_single("parent1", df, virus_taxonomy_tree)
        # Assert
        assert result_df.loc["parent1", "checked"]
        assert result_df.loc["parent1", "status"] == expected_parent

    # Untested after: All combinations where children have different states


class TestUnclearStatePropagation:
    # Untested before: All mixed child state combinations
    @pytest.mark.parametrize(
        "child1,child2,child3", generate_combinations_with_state(UNCLEAR)
    )
    def test_any_unclear_child_makes_parent_unclear(
        self,
        dataframe_factory: DataFrameFactory,
        virus_taxonomy_tree: dict[str, set[str]],
        child1: State,
        child2: State,
        child3: State,
    ) -> None:
        """Any child with UNCLEAR state should make parent state UNCLEAR."""
        # Arrange
        child_statuses: dict[str, State] = {
            "child1": child1,
            "child2": child2,
            "child3": child3,
        }
        df = dataframe_factory(parent_status=UNRESOLVED, child_statuses=child_statuses)
        # Act
        result_df = mark_ancestor_infections_single("parent1", df, virus_taxonomy_tree)
        # Assert
        assert result_df.loc["parent1", "checked"]
        assert result_df.loc["parent1", "status"] == UNCLEAR

    # Untested after: Mixed states without any UNCLEAR children

    @pytest.mark.parametrize("child1,child2,child3", generate_conflict_combinations())
    def test_conflicting_states_make_parent_unclear(
        self,
        dataframe_factory: DataFrameFactory,
        virus_taxonomy_tree: dict[str, set[str]],
        child1: State,
        child2: State,
        child3: State,
    ) -> None:
        """If there are children with both inconsistent/maybe_inconsistent and match/consistent states, make the parent UNCLEAR."""
        # Arrange
        child_statuses: dict[str, State] = {
            "child1": child1,
            "child2": child2,
            "child3": child3,
        }
        df = dataframe_factory(parent_status=UNRESOLVED, child_statuses=child_statuses)
        # Act
        result_df = mark_ancestor_infections_single("parent1", df, virus_taxonomy_tree)
        # Assert
        assert result_df.loc["parent1", "checked"]
        assert result_df.loc["parent1", "status"] == UNCLEAR

    # Untested after: Mixed states without UNCLEAR children AND without conflicting positive/negative evidence


class TestMixedStatePropagation:
    """Test propagation rules for mixed child states."""

    # Untested before: Mixed states without UNCLEAR children AND without conflicting positive/negative evidence

    @pytest.mark.parametrize(
        "child1,child2,child3,initial_parent,expected_parent",
        inconsistent_propagation_cases,
        ids=[generate_test_id(case) for case in inconsistent_propagation_cases],
    )
    def test_inconsistent_state_propagation(
        self,
        dataframe_factory: DataFrameFactory,
        virus_taxonomy_tree: dict[str, set[str]],
        child1: State,
        child2: State,
        child3: State,
        initial_parent: State,
        expected_parent: State,
    ) -> None:
        """Test propagation when children have inconsistent/maybe_inconsistent/unresolved states."""
        # Arrange
        child_statuses: dict[str, State] = {
            "child1": child1,
            "child2": child2,
            "child3": child3,
        }
        df = dataframe_factory(
            parent_status=initial_parent, child_statuses=child_statuses
        )
        # Act
        result_df = mark_ancestor_infections_single("parent1", df, virus_taxonomy_tree)
        # Assert
        assert result_df.loc["parent1", "checked"]
        assert result_df.loc["parent1", "status"] == expected_parent, (
            f"Expected parent status {expected_parent}, "
            f"but got {result_df.loc['parent1', 'status']}"
        )

    # Untested after: Mixed states with only positive evidence (MATCH/CONSISTENT/UNRESOLVED)

    @pytest.mark.parametrize(
        "child1,child2,child3,initial_parent,expected_parent",
        consistent_propagation_cases,
        ids=[generate_test_id(case) for case in consistent_propagation_cases],
    )
    def test_consistent_state_propagation(
        self,
        dataframe_factory: DataFrameFactory,
        virus_taxonomy_tree: dict[str, set[str]],
        child1: State,
        child2: State,
        child3: State,
        initial_parent: State,
        expected_parent: State,
    ) -> None:
        """Test propagation when children have match/consistent/unresolved states."""
        # Arrange
        child_statuses: dict[str, State] = {
            "child1": child1,
            "child2": child2,
            "child3": child3,
        }
        df = dataframe_factory(
            parent_status=initial_parent, child_statuses=child_statuses
        )
        # Act
        result_df = mark_ancestor_infections_single("parent1", df, virus_taxonomy_tree)
        # Assert
        assert result_df.loc["parent1", "checked"]
        assert result_df.loc["parent1", "status"] == expected_parent

    # Untested after: All major logic branches tested for mark_ancestor_infections_single


# =======================================================================
# Tests for mark_direct_infections
# =======================================================================

class TestMarkDirectInfections:
    """Test the mark_direct_infections function."""

    def test_virus_has_matching_hosts(self) -> None:
        """Test that virus with matching hosts returns MATCH."""
        # Arrange
        virus_taxids = pd.Series(["virus1", "virus2", "virus3"])
        host_taxids = {"host1", "host2", "host3"}
        virus_host_mapping = {
            "virus1": {"host1", "host4"},  # Has matching host (host1)
            "virus2": {"host2", "host3"},  # Has matching hosts (host2, host3)
            "virus3": {"host4", "host5"},  # No matching hosts
        }
        
        # Act
        result = mark_direct_infections(virus_taxids, host_taxids, virus_host_mapping)
        
        # Assert
        assert result["virus1"] == MATCH
        assert result["virus2"] == MATCH
        assert result["virus3"] == INCONSISTENT

    def test_virus_not_in_mapping(self) -> None:
        """Test that virus not in mapping returns UNRESOLVED."""
        # Arrange
        virus_taxids = pd.Series(["virus1", "virus2", "virus3"])
        host_taxids = {"host1", "host2"}
        virus_host_mapping = {
            "virus1": {"host1"},  # Has matching host
        }
        # virus2 and virus3 are not in mapping
        
        # Act
        result = mark_direct_infections(virus_taxids, host_taxids, virus_host_mapping)
        
        # Assert
        assert result["virus1"] == MATCH
        assert result["virus2"] == UNRESOLVED
        assert result["virus3"] == UNRESOLVED

    def test_empty_inputs(self) -> None:
        """Test that empty virus series is handled gracefully."""
        # Arrange
        virus_taxids = pd.Series([], dtype=str)
        host_taxids = {"host1", "host2"}
        virus_host_mapping = {"virus1": {"host1"}}
        
        # Act
        result = mark_direct_infections(virus_taxids, host_taxids, virus_host_mapping)
        
        # Assert
        assert len(result) == 0
        assert isinstance(result, pd.Series)

    def test_empty_host_taxids(self) -> None:
        """Test behavior with empty host taxids set."""
        # Arrange
        virus_taxids = pd.Series(["virus1", "virus2"])
        host_taxids: set[str] = set()  # Empty set
        virus_host_mapping = {
            "virus1": {"host1", "host2"},
            "virus2": {"host3"},
        }
        
        # Act
        result = mark_direct_infections(virus_taxids, host_taxids, virus_host_mapping)
        
        # Assert
        # All viruses should be INCONSISTENT since no hosts match empty set
        assert result["virus1"] == INCONSISTENT
        assert result["virus2"] == INCONSISTENT


# =======================================================================
# Tests for expand_taxid
# =======================================================================

class TestExpandTaxid:
    """Test the expand_taxid function."""

    def test_linear_taxonomy(self) -> None:
        """Test expansion of a linear chain of descendants."""
        # Arrange - Create a linear taxonomy: parent -> child -> grandchild
        nodes_data = [
            {"taxid": "1", "parent_taxid": "0"},  # Root
            {"taxid": "2", "parent_taxid": "1"},  # Child of 1
            {"taxid": "3", "parent_taxid": "2"},  # Grandchild of 1
            {"taxid": "4", "parent_taxid": "3"},  # Great-grandchild of 1
        ]
        nodes = pd.DataFrame(nodes_data)
        
        # Act
        result = expand_taxid("1", nodes)
        
        # Assert
        expected = {"1", "2", "3", "4"}
        assert result == expected

    def test_branching_taxonomy(self) -> None:
        """Test expansion with multiple descendant branches."""
        # Arrange - Create a branching taxonomy
        nodes_data = [
            {"taxid": "1", "parent_taxid": "0"},  # Root
            {"taxid": "2", "parent_taxid": "1"},  # First child
            {"taxid": "3", "parent_taxid": "1"},  # Second child
            {"taxid": "4", "parent_taxid": "2"},  # Grandchild via first child
            {"taxid": "5", "parent_taxid": "3"},  # Grandchild via second child
            {"taxid": "6", "parent_taxid": "3"},  # Another grandchild via second child
        ]
        nodes = pd.DataFrame(nodes_data)
        
        # Act
        result = expand_taxid("1", nodes)
        
        # Assert
        expected = {"1", "2", "3", "4", "5", "6"}
        assert result == expected

    def test_leaf_node(self) -> None:
        """Test that leaf node returns only itself."""
        # Arrange
        nodes_data = [
            {"taxid": "1", "parent_taxid": "0"},
            {"taxid": "2", "parent_taxid": "1"},
            {"taxid": "3", "parent_taxid": "1"},
            {"taxid": "4", "parent_taxid": "2"},  # Leaf node
        ]
        nodes = pd.DataFrame(nodes_data)
        
        # Act
        result = expand_taxid("4", nodes)  # Query leaf node
        
        # Assert
        assert result == {"4"}

    def test_taxid_not_in_nodes(self) -> None:
        """Test that taxid not in nodes returns only itself."""
        # Arrange
        nodes_data = [
            {"taxid": "1", "parent_taxid": "0"},
            {"taxid": "2", "parent_taxid": "1"},
        ]
        nodes = pd.DataFrame(nodes_data)
        
        # Act
        result = expand_taxid("999", nodes)  # Non-existent taxid
        
        # Assert
        assert result == {"999"}

    def test_complex_tree(self) -> None:
        """Test expansion in a more complex tree structure."""
        # Arrange - Create a complex tree
        nodes_data = [
            {"taxid": "1", "parent_taxid": "0"},
            # First branch
            {"taxid": "10", "parent_taxid": "1"},
            {"taxid": "11", "parent_taxid": "10"},
            {"taxid": "12", "parent_taxid": "10"},
            # Second branch
            {"taxid": "20", "parent_taxid": "1"},
            {"taxid": "21", "parent_taxid": "20"},
            {"taxid": "22", "parent_taxid": "20"},
            # Deep branch from 21
            {"taxid": "210", "parent_taxid": "21"},
            {"taxid": "211", "parent_taxid": "210"},
        ]
        nodes = pd.DataFrame(nodes_data)
        
        # Act - Expand from different starting points
        result_root = expand_taxid("1", nodes)
        result_branch = expand_taxid("20", nodes)
        
        # Assert
        expected_root = {"1", "10", "11", "12", "20", "21", "22", "210", "211"}
        expected_branch = {"20", "21", "22", "210", "211"}
        assert result_root == expected_root
        assert result_branch == expected_branch


# =======================================================================
# Tests for build_virus_tree
# =======================================================================

class TestBuildVirusTree:
    """Test the build_virus_tree function."""

    def test_valid_input(self) -> None:
        """Test creating correct parent→children mapping from valid input."""
        # Arrange
        viral_taxa_data = [
            {"taxid": "1", "parent_taxid": "0"},  # Root has parent 0
            {"taxid": "2", "parent_taxid": "1"},  # Child of 1
            {"taxid": "3", "parent_taxid": "1"},  # Another child of 1
            {"taxid": "4", "parent_taxid": "2"},  # Child of 2
            {"taxid": "5", "parent_taxid": "2"},  # Another child of 2
        ]
        viral_taxa_df = pd.DataFrame(viral_taxa_data)
        
        # Act
        result = build_virus_tree(viral_taxa_df)
        
        # Assert
        expected = {
            "0": {"1"},        # Parent 0 has child 1
            "1": {"2", "3"},  # Parent 1 has children 2 and 3
            "2": {"4", "5"},  # Parent 2 has children 4 and 5
        }
        # Check all expected keys are present
        for parent, children in expected.items():
            assert parent in result
            assert result[parent] == children
        
        # Verify that nodes without children return empty sets
        assert result["3"] == set()
        assert result["4"] == set()
        assert result["5"] == set()

    def test_empty_dataframe(self) -> None:
        """Test that empty DataFrame returns empty defaultdict."""
        # Arrange
        viral_taxa_df = pd.DataFrame(columns=["taxid", "parent_taxid"])
        
        # Act
        result = build_virus_tree(viral_taxa_df)
        
        # Assert
        # Should be an empty defaultdict
        assert len(result) == 0
        # But still should behave like defaultdict
        assert result["any_key"] == set()

    def test_complex_hierarchy(self) -> None:
        """Test a more complex viral taxonomy hierarchy."""
        # Arrange - Create a complex tree structure
        viral_taxa_data = [
            # Top level
            {"taxid": "100", "parent_taxid": "root"},
            {"taxid": "200", "parent_taxid": "root"},
            # Second level
            {"taxid": "110", "parent_taxid": "100"},
            {"taxid": "120", "parent_taxid": "100"},
            {"taxid": "210", "parent_taxid": "200"},
            # Third level
            {"taxid": "111", "parent_taxid": "110"},
            {"taxid": "112", "parent_taxid": "110"},
            {"taxid": "211", "parent_taxid": "210"},
            {"taxid": "212", "parent_taxid": "210"},
            {"taxid": "213", "parent_taxid": "210"},
        ]
        viral_taxa_df = pd.DataFrame(viral_taxa_data)
        
        # Act
        result = build_virus_tree(viral_taxa_df)
        
        # Assert
        assert result["root"] == {"100", "200"}
        assert result["100"] == {"110", "120"}
        assert result["200"] == {"210"}
        assert result["110"] == {"111", "112"}
        assert result["210"] == {"211", "212", "213"}
        # Leaf nodes
        assert result["120"] == set()
        assert result["111"] == set()
        assert result["112"] == set()
        assert result["211"] == set()
        assert result["212"] == set()
        assert result["213"] == set()


# =======================================================================
# Tests for mark_descendant_infections
# =======================================================================

class TestMarkDescendantInfections:
    """Test mark_descendant_infections function.
    
    This function propagates infection statuses from parent nodes to their descendants.
    The propagation follows a specific priority order:
    1. MATCH propagation (overrides everything)
    2. INCONSISTENT propagation (overrides everything except MATCH)
    3. CONSISTENT propagation (doesn't override MATCH)
    4. UNCLEAR propagation (only affects UNRESOLVED and MAYBE_INCONSISTENT)
    
    After this function runs:
    - No nodes should have UNRESOLVED status
    - No nodes should have MAYBE_INCONSISTENT status
    
    Valid Parent-Child State Combinations
    Based on the logic in the code, these are the valid parent-child combinations that can exist when mark_descendant_infections is called:
    
    Parent: MATCH
    - Children can be: Any state
    - Note: MATCH parents are marked as "checked" early and skip mark_ancestor_infections_single
    
    Parent: INCONSISTENT
    - If all children are INCONSISTENT: Parent inherits INCONSISTENT (uniform rule)
    - Otherwise: Children can be INCONSISTENT, MAYBE_INCONSISTENT, or UNRESOLVED
    - Cannot have: MATCH or CONSISTENT children (would make parent UNCLEAR instead)
    
    Parent: UNCLEAR
    - Must have one of:
      - At least one UNCLEAR child, OR
      - Mix of (INCONSISTENT/MAYBE_INCONSISTENT) and (MATCH/CONSISTENT) children
    - Cannot have: All children with same state (would trigger uniform rule)
    
    Parent: CONSISTENT
    - Children must be some mix of: MATCH, CONSISTENT, and/or UNRESOLVED only
    - Cannot have: INCONSISTENT, MAYBE_INCONSISTENT, or UNCLEAR children
    - Cannot have: All children with same state (would trigger uniform rule)
    
    Parent: UNRESOLVED
    - If parent is UNRESOLVED after mark_ancestor_infections, children must all be UNRESOLVED
    - This follows from the logic: parent only stays UNRESOLVED if all children are UNRESOLVED
    
    Parent: MAYBE_INCONSISTENT
    - Must have: At least one INCONSISTENT or MAYBE_INCONSISTENT child
    - Parent's original state was UNRESOLVED (before mark_ancestor_infections_single)
    - Cannot have: Only MATCH/CONSISTENT children (would make parent CONSISTENT)
    """
    @pytest.fixture
    def simple_tree(self) -> dict[str, set[str]]:
        """Create a simple tree structure for testing.
        
        Structure:
        root
        ├── v1
        │   ├── v11
        │   └── v12
        └── v2
            └── v21
        """
        return {
            "root": {"v1", "v2"},
            "v1": {"v11", "v12"},
            "v2": {"v21"},
            "v11": set(),
            "v12": set(),
            "v21": set(),
        }
    
    @pytest.fixture
    def comprehensive_propagation_tree(self) -> dict[str, set[str]]:
        """Create a comprehensive tree for testing all propagation scenarios.
        
        Structure:
        root (UNCLEAR)
        ├── child1 (MATCH)              # Tests MATCH propagation
        │   ├── child11 (UNRESOLVED)
        │   ├── child12 (INCONSISTENT)
        │   ├── child13 (MATCH)
        │   ├── child14 (CONSISTENT)    # Has children to test override
        │   │   ├── child141 (MATCH)
        │   │   └── child142 (UNRESOLVED)
        │   ├── child15 (UNCLEAR)       # Has children to test override
        │   │   ├── child151 (MATCH)
        │   │   └── child152 (INCONSISTENT)
        │   └── child16 (MAYBE_INCONSISTENT) # Has children to test override
        │       ├── child161 (UNRESOLVED)
        │       └── child162 (INCONSISTENT)
        ├── child2 (CONSISTENT)         # Tests CONSISTENT propagation  
        │   ├── child21 (MATCH)
        │   └── child22 (UNRESOLVED)
        ├── child3 (MAYBE_INCONSISTENT) # Tests UNCLEAR propagation
        │   ├── child31 (UNRESOLVED)
        │   └── child32 (INCONSISTENT)
        └── child4 (INCONSISTENT)       # Tests INCONSISTENT propagation
            ├── child41 (UNRESOLVED)
            ├── child42 (INCONSISTENT)
            └── child43 (MAYBE_INCONSISTENT) # Test INCONSISTENT overrides MAYBE_INCONSISTENT
                ├── child431 (INCONSISTENT)
                └── child432 (UNRESOLVED)
        """
        return {
            "root": {"child1", "child2", "child3", "child4"},
            "child1": {"child11", "child12", "child13", "child14", "child15", "child16"},
            "child2": {"child21", "child22"},
            "child3": {"child31", "child32"},
            "child4": {"child41", "child42", "child43"},
            "child11": set(),
            "child12": set(),
            "child13": set(),
            "child14": {"child141", "child142"},
            "child15": {"child151", "child152"},
            "child16": {"child161", "child162"},
            "child21": set(),
            "child22": set(),
            "child31": set(),
            "child32": set(),
            "child41": set(),
            "child42": set(),
            "child43": {"child431", "child432"},
            "child141": set(),
            "child142": set(),
            "child151": set(),
            "child152": set(),
            "child161": set(),
            "child162": set(),
            "child431": set(),
            "child432": set(),
        }
    

    def test_all_propagation_scenarios(self, comprehensive_propagation_tree: dict[str, set[str]]) -> None:
        """Test all propagation scenarios in a single comprehensive test.
        
        Initial state setup per the example:
        root (UNCLEAR/2)
        ├── child1 (MATCH/1)
        │   ├── child11 (UNRESOLVED/-1)
        │   ├── child12 (INCONSISTENT/0)
        │   ├── child13 (MATCH/1)
        │   ├── child14 (CONSISTENT/3)
        │   │   ├── child141 (MATCH/1)
        │   │   └── child142 (UNRESOLVED/-1)
        │   ├── child15 (UNCLEAR/2)
        │   │   ├── child151 (MATCH/1)
        │   │   └── child152 (INCONSISTENT/0)
        │   └── child16 (MAYBE_INCONSISTENT/-2)
        │       ├── child161 (UNRESOLVED/-1)
        │       └── child162 (INCONSISTENT/0)
        ├── child2 (CONSISTENT/3)
        │   ├── child21 (MATCH/1)
        │   └── child22 (UNRESOLVED/-1)
        ├── child3 (MAYBE_INCONSISTENT/-2)
        │   ├── child31 (UNRESOLVED/-1)
        │   └── child32 (INCONSISTENT/0)
        └── child4 (INCONSISTENT/0)
            ├── child41 (UNRESOLVED/-1)
            ├── child42 (INCONSISTENT/0)
            └── child43 (MAYBE_INCONSISTENT/-2)
                ├── child431 (INCONSISTENT/0)
                └── child432 (UNRESOLVED/-1)
        
        Expected after propagation:
        - All descendants of child1 become MATCH (MATCH overrides all)
        - Descendants of child2: child21 stays MATCH, child22 becomes CONSISTENT
        - child3 becomes UNCLEAR (from MAYBE_INCONSISTENT), child31 becomes UNCLEAR, child32 stays INCONSISTENT
        - Descendants of child4 become INCONSISTENT
        """
        # Arrange
        statuses = pd.Series({
            "root": UNCLEAR,
            "child1": MATCH,
            "child11": UNRESOLVED,
            "child12": INCONSISTENT,
            "child13": MATCH,
            "child14": CONSISTENT,
            "child141": MATCH,
            "child142": UNRESOLVED,
            "child15": UNCLEAR,
            "child151": MATCH,
            "child152": INCONSISTENT,
            "child16": MAYBE_INCONSISTENT,
            "child161": UNRESOLVED,
            "child162": INCONSISTENT,
            "child2": CONSISTENT,
            "child21": MATCH,
            "child22": UNRESOLVED,
            "child3": MAYBE_INCONSISTENT,
            "child31": UNRESOLVED,
            "child32": INCONSISTENT,
            "child4": INCONSISTENT,
            "child41": UNRESOLVED,
            "child42": INCONSISTENT,
            "child43": MAYBE_INCONSISTENT,
            "child431": INCONSISTENT,
            "child432": UNRESOLVED,
        })
        
        # Act
        result = mark_descendant_infections(comprehensive_propagation_tree, statuses.copy())
        
        # Assert
        # Root remains UNCLEAR
        assert result["root"] == UNCLEAR
        
        # Test MATCH propagation (child1 and descendants)
        assert result["child1"] == MATCH
        assert result["child11"] == MATCH  # MATCH overrides UNRESOLVED
        assert result["child12"] == MATCH  # MATCH overrides INCONSISTENT
        assert result["child13"] == MATCH  # Already MATCH, preserved
        assert result["child14"] == MATCH  # MATCH overrides CONSISTENT
        assert result["child141"] == MATCH  # Inherits MATCH from grandparent
        assert result["child142"] == MATCH  # Inherits MATCH from grandparent
        assert result["child15"] == MATCH  # MATCH overrides UNCLEAR
        assert result["child151"] == MATCH  # Inherits MATCH from grandparent
        assert result["child152"] == MATCH  # MATCH overrides INCONSISTENT
        assert result["child16"] == MATCH  # MATCH overrides MAYBE_INCONSISTENT
        assert result["child161"] == MATCH  # Inherits MATCH from grandparent
        assert result["child162"] == MATCH  # MATCH overrides INCONSISTENT
        
        # Test CONSISTENT propagation (child2 and descendants)
        assert result["child2"] == CONSISTENT
        assert result["child21"] == MATCH      # MATCH is preserved (not overridden by CONSISTENT)
        assert result["child22"] == CONSISTENT  # Inherits CONSISTENT from parent
        
        # Test UNCLEAR propagation to MAYBE_INCONSISTENT (child3 and descendants)
        assert result["child3"] == UNCLEAR  # Changed from MAYBE_INCONSISTENT by UNCLEAR root
        assert result["child31"] == UNCLEAR  # Changed from UNRESOLVED by UNCLEAR parent
        assert result["child32"] == INCONSISTENT  # INCONSISTENT is preserved (not overridden by UNCLEAR)
        
        # Test INCONSISTENT propagation (child4 and descendants)
        assert result["child4"] == INCONSISTENT
        assert result["child41"] == INCONSISTENT  # Inherits from INCONSISTENT parent
        assert result["child42"] == INCONSISTENT  # Already INCONSISTENT, preserved
        assert result["child43"] == INCONSISTENT  # INCONSISTENT overrides MAYBE_INCONSISTENT
        assert result["child431"] == INCONSISTENT  # Already INCONSISTENT, preserved
        assert result["child432"] == INCONSISTENT  # INCONSISTENT overrides UNRESOLVED
        
        # Verify no UNRESOLVED or MAYBE_INCONSISTENT states remain
        assert UNRESOLVED not in result.values
        assert MAYBE_INCONSISTENT not in result.values

    def test_assertion_unresolved_remaining(self, simple_tree: dict[str, set[str]]) -> None:
        """Test that assertion fires if UNRESOLVED remains (shouldn't happen in practice)."""
        # Arrange - Create a tree with an isolated UNRESOLVED node
        isolated_tree: dict[str, set[str]] = {"isolated": set()}
        statuses = pd.Series({"isolated": UNRESOLVED})
        
        # Act & Assert
        with pytest.raises(AssertionError, match="Some taxids are still unresolved"):
            mark_descendant_infections(isolated_tree, statuses)

    def test_assertion_maybe_inconsistent_remaining(self, simple_tree: dict[str, set[str]]) -> None:
        """Test that assertion fires if MAYBE_INCONSISTENT remains (shouldn't happen in practice)."""
        # Arrange - Create a tree with an isolated MAYBE_INCONSISTENT node
        isolated_tree: dict[str, set[str]] = {"isolated": set()}
        statuses = pd.Series({"isolated": MAYBE_INCONSISTENT})
        
        # Act & Assert
        with pytest.raises(AssertionError, match="Some taxids are still maybe inconsistent"):
            mark_descendant_infections(isolated_tree, statuses)

    def test_assertion_consistent_with_inconsistent_descendant(self) -> None:
        """Test that assertion fires if CONSISTENT has INCONSISTENT descendants."""
        # Arrange - This shouldn't happen after mark_ancestor_infections
        tree = {
            "root": {"child"},
            "child": set()
        }
        statuses = pd.Series({
            "root": CONSISTENT,
            "child": INCONSISTENT  # Invalid: CONSISTENT parent can't have INCONSISTENT child
        })
        
        # Act & Assert
        with pytest.raises(AssertionError, match="Some taxids marked as CONSISTENT have descendants marked as INCONSISTENT or UNCLEAR"):
            mark_descendant_infections(tree, statuses)

    def test_assertion_consistent_with_unclear_descendant(self) -> None:
        """Test that assertion fires if CONSISTENT has UNCLEAR descendants."""
        # Arrange
        tree = {
            "root": {"child"},
            "child": set()
        }
        statuses = pd.Series({
            "root": CONSISTENT,
            "child": UNCLEAR  # Invalid: CONSISTENT parent can't have UNCLEAR child
        })
        
        # Act & Assert
        with pytest.raises(AssertionError, match="Some taxids marked as CONSISTENT have descendants marked as INCONSISTENT or UNCLEAR"):
            mark_descendant_infections(tree, statuses)

# =======================================================================
# Tests for add_descendants
# =======================================================================

class TestAddDescendants:
    """Test the add_descendants helper function."""

    def test_basic_expansion(self) -> None:
        """Test that function adds all descendants correctly."""
        # Arrange
        virus_tree = {
            "1": {"2", "3"},
            "2": {"4", "5"},
            "3": {"6"},
            "4": set(),
            "5": set(),
            "6": {"7", "8"},
            "7": set(),
            "8": set(),
        }
        taxids_start = {"1"}
        
        # Act
        result = add_descendants(virus_tree, taxids_start)
        
        # Assert
        expected = {"1", "2", "3", "4", "5", "6", "7", "8"}
        assert result == expected

    def test_no_descendants(self) -> None:
        """Test that function returns input set unchanged when no descendants."""
        # Arrange
        virus_tree: dict[str, set[str]] = {
            "1": set(),
            "2": set(),
            "3": set(),
        }
        taxids_start = {"1", "2"}
        
        # Act
        result = add_descendants(virus_tree, taxids_start)
        
        # Assert
        assert result == taxids_start

    def test_empty_input(self) -> None:
        """Test that function returns empty set for empty input."""
        # Arrange
        virus_tree = {
            "1": {"2", "3"},
            "2": set(),
            "3": set(),
        }
        taxids_start: set[str] = set()
        
        # Act
        result = add_descendants(virus_tree, taxids_start)
        
        # Assert
        assert result == set()

    def test_multiple_starting_taxids(self) -> None:
        """Test expansion from multiple starting points."""
        # Arrange
        virus_tree = {
            "1": {"2", "3"},
            "2": {"4"},
            "3": {"5"},
            "4": set(),
            "5": set(),
            "6": {"7", "8"},
            "7": set(),
            "8": set(),
        }
        taxids_start = {"1", "6"}
        
        # Act
        result = add_descendants(virus_tree, taxids_start)
        
        # Assert
        expected = {"1", "2", "3", "4", "5", "6", "7", "8"}
        assert result == expected


# =======================================================================
# Tests for exclude_infections
# =======================================================================

class TestExcludeInfections:
    """Test the exclude_infections function."""

    def test_valid_exclusions(self) -> None:
        """Test that taxids and descendants are marked INCONSISTENT."""
        # Arrange
        virus_tree = {
            "1": {"2", "3"},
            "2": {"4", "5"},
            "3": set(),
            "4": set(),
            "5": set(),
        }
        statuses = pd.Series({
            "1": MATCH,
            "2": MATCH,
            "3": UNCLEAR,
            "4": CONSISTENT,
            "5": UNRESOLVED,
        })
        exclude_taxids = ["2"]  # Should exclude 2, 4, and 5
        
        # Act
        result = exclude_infections(virus_tree, statuses.copy(), exclude_taxids)
        
        # Assert
        assert result["1"] == MATCH  # Not excluded
        assert result["2"] == INCONSISTENT  # Excluded
        assert result["3"] == UNCLEAR  # Not excluded
        assert result["4"] == INCONSISTENT  # Descendant of excluded
        assert result["5"] == INCONSISTENT  # Descendant of excluded

    def test_empty_exclude_list(self) -> None:
        """Test that empty exclude list results in no changes."""
        # Arrange
        virus_tree = {
            "1": {"2", "3"},
            "2": set(),
            "3": set(),
        }
        statuses = pd.Series({
            "1": MATCH,
            "2": CONSISTENT,
            "3": UNCLEAR,
        })
        exclude_taxids: list[str] = []

        # Act
        result = exclude_infections(virus_tree, statuses.copy(), exclude_taxids)
        
        # Assert
        pd.testing.assert_series_equal(result, statuses)

    def test_exclude_with_deep_hierarchy(self) -> None:
        """Test exclusion propagates through deep hierarchy."""
        # Arrange
        virus_tree = {
            "1": {"2"},
            "2": {"3"},
            "3": {"4"},
            "4": {"5"},
            "5": set(),
        }
        statuses = pd.Series({
            "1": MATCH,
            "2": MATCH,
            "3": MATCH,
            "4": MATCH,
            "5": MATCH,
        })
        exclude_taxids = ["2"]
        
        # Act
        result = exclude_infections(virus_tree, statuses.copy(), exclude_taxids)
        
        # Assert
        assert result["1"] == MATCH  # Not excluded
        assert result["2"] == INCONSISTENT  # Excluded
        assert result["3"] == INCONSISTENT  # Descendant
        assert result["4"] == INCONSISTENT  # Descendant
        assert result["5"] == INCONSISTENT  # Descendant


# =======================================================================
# Tests for mark_ancestor_infections
# =======================================================================

class TestMarkAncestorInfections:
    """Test the mark_ancestor_infections wrapper function."""

    def test_all_nodes_processed(self) -> None:
        """Test that all nodes are marked as checked after processing."""
        # Arrange
        virus_tree = {
            "1": {"2", "3"},
            "2": {"4", "5"},
            "3": set(),
            "4": set(),
            "5": set(),
        }
        statuses = pd.Series({
            "1": UNRESOLVED,
            "2": UNRESOLVED,
            "3": MATCH,
            "4": INCONSISTENT,
            "5": MATCH,
        })
        
        # Act
        result = mark_ancestor_infections(virus_tree, statuses)
        
        # Assert
        # All nodes should have been processed
        # Node 2 should be UNCLEAR (has both MATCH and INCONSISTENT children)
        # Node 1 should be UNCLEAR (has UNCLEAR child)
        assert result["1"] == UNCLEAR
        assert result["2"] == UNCLEAR
        assert result["3"] == MATCH
        assert result["4"] == INCONSISTENT
        assert result["5"] == MATCH

    def test_initial_state_preserved(self) -> None:
        """Test that MATCH and childless nodes are preserved."""
        # Arrange
        virus_tree = {
            "1": {"2", "3"},
            "2": set(),
            "3": set(),
            "4": set(),  # Isolated node
        }
        statuses = pd.Series({
            "1": MATCH,  # Should be preserved
            "2": INCONSISTENT, # Childless, should be preserved
            "3": UNRESOLVED, # Childless, should be preserved
            "4": UNCLEAR,  # Childless, should be preserved
        })
        
        # Act
        result = mark_ancestor_infections(virus_tree, statuses)
        
        # Assert
        assert result["1"] == MATCH  # Preserved
        assert result["2"] == INCONSISTENT  # Preserved (childless)
        assert result["3"] == UNRESOLVED  # Preserved (childless)
        assert result["4"] == UNCLEAR  # Preserved (childless)


# =======================================================================
# Tests for get_host_taxids
# =======================================================================

class TestGetHostTaxids:
    """Test the get_host_taxids function."""

    def test_multiple_hosts(self, host_taxonomy_nodes: pd.DataFrame) -> None:
        """Test that each host is expanded correctly."""
        # Arrange
        hosts = {
            "human": "9606",
            "vertebrate": "7742",
        }
        
        # Act
        result = get_host_taxids(hosts, host_taxonomy_nodes)
        
        # Assert
        assert result["human"] == {"9606", "741158", "63221"}
        assert result["vertebrate"] == {"7742", "1476529", "7776", "2662825"}

    def test_empty_input(self) -> None:
        """Test that empty input returns empty dictionary."""
        # Arrange
        hosts: dict[str, str] = {}
        nodes = pd.DataFrame()
        
        # Act
        result = get_host_taxids(hosts, nodes)
        
        # Assert
        assert result == {}


# =======================================================================
# Tests for get_virus_host_mapping
# =======================================================================

class TestGetVirusHostMapping:
    """Test the get_virus_host_mapping function."""

    def test_valid_tsv(self, tmp_path: Path) -> None:
        """Test reading valid TSV returns correct mapping."""
        # Arrange
        tsv_content = (
            "virus tax id\thost tax id\n"
            "1\t100\n"
            "1\t101\n"
            "2\t200\n"
            "3\t300"
        )
        tsv_file = tmp_path / "virus_host.tsv"
        tsv_file.write_text(tsv_content)
        
        # Act
        result = get_virus_host_mapping(str(tsv_file))
        
        # Assert
        assert result["1"] == {"100", "101"}
        assert result["2"] == {"200"}
        assert result["3"] == {"300"}

    def test_duplicate_entries(self, tmp_path: Path) -> None:
        """Test that duplicate entries are handled correctly."""
        # Arrange
        tsv_content = (
            "virus tax id\thost tax id\n"
            "1\t100\n"
            "1\t100\n"
            "1\t101\n"
            "2\t200"
        )
        tsv_file = tmp_path / "virus_host.tsv"
        tsv_file.write_text(tsv_content)
        
        # Act
        result = get_virus_host_mapping(str(tsv_file))
        
        # Assert
        assert result["1"] == {"100", "101"}  # Duplicates removed
        assert result["2"] == {"200"}

    def test_empty_file(self, tmp_path: Path) -> None:
        """Test that empty file returns empty dictionary."""
        # Arrange
        tsv_content = "virus tax id\thost tax id"  # Header only
        tsv_file = tmp_path / "virus_host.tsv"
        tsv_file.write_text(tsv_content)
        
        # Act
        result = get_virus_host_mapping(str(tsv_file))
        
        # Assert
        assert result == {}


# =======================================================================
# Tests for annotate_virus_db_single
# =======================================================================

class TestAnnotateVirusDbSingle:
    """Test the annotate_virus_db_single function."""

    def test_column_naming(self, sample_virus_db: pd.DataFrame, sample_virus_tree: dict[str, set[str]], monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that infection status column is named correctly."""
        # Mock check_infection since it's a complex function with many dependencies
        def mock_check_infection(virus_taxids: pd.Series, host_taxids: set[str], virus_tree: dict[str, set[str]], virus_host_mapping: dict[str, set[str]], hard_exclude: list[str]) -> pd.Series:  # type: ignore[type-arg]
            return pd.Series([MATCH, INCONSISTENT, UNCLEAR], index=virus_taxids)
        
        monkeypatch.setattr("annotate_viral_hosts.check_infection", mock_check_infection)
        
        # Arrange
        host_name = "human"
        host_taxids = {"9606"}
        virus_host_mapping: dict[str, set[str]] = {}
        hard_exclude: list[str] = []
        
        # Act
        result = annotate_virus_db_single(
            sample_virus_db, host_name, host_taxids, sample_virus_tree, virus_host_mapping, hard_exclude
        )
        
        # Assert
        assert "infection_status_human" in result.columns
        assert list(result["infection_status_human"]) == [MATCH, INCONSISTENT, UNCLEAR]


# =======================================================================
# Main execution
# =======================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
