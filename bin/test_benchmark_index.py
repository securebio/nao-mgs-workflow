#!/usr/bin/env python3
"""Pytest suite for the pure-function helpers in benchmark_index.py.

I/O staging (S3/local) is exercised by running the script end-to-end against
real index releases; this file covers the deterministic diff logic only.
"""

###########
# IMPORTS #
###########

import pandas as pd
import pytest
from benchmark_index import (
    annotate_changes_with_coverage,
    build_parent_map,
    classify_coverage,
    compare_size_listings,
    diff_genome_metadata,
    diff_params,
    diff_taxonomy,
    includes_for_other_hosts,
    infection_status_changes,
    infection_status_columns,
    infection_status_transitions,
)

###########
# FIXTURE #
###########


@pytest.fixture
def old_genome_meta() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "genome_id": "G1",
                "taxid": "10",
                "species_taxid": "100",
                "organism_name": "A",
            },
            {
                "genome_id": "G2",
                "taxid": "10",
                "species_taxid": "100",
                "organism_name": "A",
            },
            {
                "genome_id": "G3",
                "taxid": "20",
                "species_taxid": "200",
                "organism_name": "B",
            },
        ]
    )


@pytest.fixture
def new_genome_meta() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "genome_id": "G2",
                "taxid": "10",
                "species_taxid": "100",
                "organism_name": "A",
            },
            {
                "genome_id": "G4",
                "taxid": "20",
                "species_taxid": "200",
                "organism_name": "B",
            },
            {
                "genome_id": "G5",
                "taxid": "30",
                "species_taxid": "300",
                "organism_name": "C",
            },
        ]
    )


###########
# TESTS   #
###########


class TestCompareSizeListings:
    def test_grown_shrunk_unchanged_and_pct(self) -> None:
        result = compare_size_listings(
            {"alpha": 100, "beta": 200, "gamma": 50},
            {"alpha": 150, "beta": 200, "delta": 80},
        )
        result = result.set_index("name")
        assert result.loc["alpha", "delta_bytes"] == 50
        assert result.loc["alpha", "pct_change"] == 50.0
        assert result.loc["beta", "delta_bytes"] == 0
        # gamma vanished; delta is new
        assert result.loc["gamma", "delta_bytes"] == -50
        assert result.loc["delta", "old_bytes"] == 0
        assert pd.isna(result.loc["delta", "pct_change"])  # no old size

    def test_sorted_by_absolute_delta(self) -> None:
        result = compare_size_listings({"a": 100, "b": 100}, {"a": 200, "b": 105})
        assert list(result["name"]) == ["a", "b"]  # +100 sorted before +5


class TestDiffGenomeMetadata:
    def test_added_and_removed_genome_ids(
        self, old_genome_meta: pd.DataFrame, new_genome_meta: pd.DataFrame
    ) -> None:
        added, removed, _ = diff_genome_metadata(old_genome_meta, new_genome_meta)
        assert set(added["genome_id"]) == {"G4", "G5"}
        assert set(removed["genome_id"]) == {"G1", "G3"}

    def test_per_species_delta(
        self, old_genome_meta: pd.DataFrame, new_genome_meta: pd.DataFrame
    ) -> None:
        _, _, species = diff_genome_metadata(old_genome_meta, new_genome_meta)
        species = species.set_index("species_taxid")
        # species 100 had 2 in old, 1 in new -> delta -1
        assert species.loc["100", "delta"] == -1
        # species 200 had 1 in old, 1 in new (different genome_id) -> delta 0
        assert species.loc["200", "delta"] == 0
        # species 300 is new -> delta +1
        assert species.loc["300", "delta"] == 1

    def test_rejects_missing_columns(self) -> None:
        bad = pd.DataFrame([{"genome_id": "X"}])
        good = pd.DataFrame(
            [
                {
                    "genome_id": "Y",
                    "taxid": "1",
                    "species_taxid": "1",
                    "organism_name": "Z",
                }
            ]
        )
        with pytest.raises(ValueError, match="missing required columns"):
            diff_genome_metadata(bad, good)


class TestDiffTaxonomy:
    def test_returns_added_and_removed(self) -> None:
        old = pd.DataFrame(
            [
                {"taxid": "1", "name": "Alpha", "rank": "species"},
                {"taxid": "2", "name": "Beta", "rank": "species"},
            ]
        )
        new = pd.DataFrame(
            [
                {"taxid": "2", "name": "Beta", "rank": "species"},
                {"taxid": "3", "name": "Gamma", "rank": "species"},
            ]
        )
        added, removed = diff_taxonomy(old, new)
        assert list(added["taxid"]) == ["3"]
        assert list(removed["taxid"]) == ["1"]


class TestInfectionStatus:
    @pytest.fixture
    def db_pair(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        old = pd.DataFrame(
            [
                {
                    "taxid": "1",
                    "name": "A",
                    "rank": "species",
                    "infection_status_human": "1",
                },
                {
                    "taxid": "2",
                    "name": "B",
                    "rank": "species",
                    "infection_status_human": "1",
                },
                {
                    "taxid": "3",
                    "name": "C",
                    "rank": "species",
                    "infection_status_human": "0",
                },
                {
                    "taxid": "4",
                    "name": "D",
                    "rank": "species",
                    "infection_status_human": "2",
                },
            ]
        )
        new = pd.DataFrame(
            [
                # taxid 1: unchanged
                {
                    "taxid": "1",
                    "name": "A",
                    "rank": "species",
                    "infection_status_human": "1",
                },
                # taxid 2: demoted 1 -> 0
                {
                    "taxid": "2",
                    "name": "B",
                    "rank": "species",
                    "infection_status_human": "0",
                },
                # taxid 3: promoted 0 -> 1
                {
                    "taxid": "3",
                    "name": "C",
                    "rank": "species",
                    "infection_status_human": "1",
                },
                # taxid 4: 2 -> 3
                {
                    "taxid": "4",
                    "name": "D",
                    "rank": "species",
                    "infection_status_human": "3",
                },
            ]
        )
        return old, new

    def test_columns_helper(self, db_pair: tuple[pd.DataFrame, pd.DataFrame]) -> None:
        old, _ = db_pair
        assert infection_status_columns(old) == ["infection_status_human"]

    def test_transitions_counts(
        self, db_pair: tuple[pd.DataFrame, pd.DataFrame]
    ) -> None:
        old, new = db_pair
        trans = infection_status_transitions(old, new, "infection_status_human")
        rows = {(r["old"], r["new"]): r["count"] for _, r in trans.iterrows()}
        assert rows == {("1", "0"): 1, ("0", "1"): 1, ("2", "3"): 1}

    def test_changes_list_pins_demoted_and_promoted(
        self, db_pair: tuple[pd.DataFrame, pd.DataFrame]
    ) -> None:
        old, new = db_pair
        changes = infection_status_changes(
            old, new, "infection_status_human"
        ).set_index("taxid")
        # Unchanged taxid 1 must not appear
        assert "1" not in changes.index
        assert changes.loc["2", "old_status"] == "1"
        assert changes.loc["2", "new_status"] == "0"
        assert changes.loc["3", "old_status"] == "0"
        assert changes.loc["3", "new_status"] == "1"


class TestDiffParams:
    def test_includes_changed_lines(self) -> None:
        old = {"kraken_db": "old.tar.gz", "shared": "x"}
        new = {"kraken_db": "new.tar.gz", "shared": "x", "added": True}
        diff = diff_params(old, new)
        assert "old.tar.gz" in diff
        assert "new.tar.gz" in diff
        assert "added" in diff


class TestCoverageClassification:
    """A small DB: 1 (root) -> 2 (family Smacoviridae) -> 3 (genus) -> 4 (species);
    1 -> 10 (species WNV-like, in overrides for "human")."""

    @pytest.fixture
    def parent_map(self) -> dict[str, str]:
        return {"1": "1", "2": "1", "3": "2", "4": "3", "10": "1"}

    @pytest.fixture
    def excluded(self) -> set[str]:
        return {"2"}  # Smacoviridae-equivalent

    @pytest.fixture
    def included(self) -> dict[str, set[str]]:
        return {"human": {"10"}}

    def test_classifies_excluded_ancestor(
        self,
        parent_map: dict[str, str],
        excluded: set[str],
        included: dict[str, set[str]],
    ) -> None:
        # taxid 4 is a descendant of excluded family 2
        assert classify_coverage("4", parent_map, excluded, included, "human") == (
            "excluded",
            "2",
        )

    def test_classifies_included_self(
        self,
        parent_map: dict[str, str],
        excluded: set[str],
        included: dict[str, set[str]],
    ) -> None:
        assert classify_coverage("10", parent_map, excluded, included, "human") == (
            "included",
            "10",
        )

    def test_uncovered_returns_empty(
        self,
        parent_map: dict[str, str],
        excluded: set[str],
        included: dict[str, set[str]],
    ) -> None:
        # taxid 1 (root) is neither excluded nor in any host's includes
        assert classify_coverage("1", parent_map, excluded, included, "human") == (
            "",
            "",
        )

    def test_excluded_wins_over_included_when_walking_up(
        self,
        parent_map: dict[str, str],
        included: dict[str, set[str]],
    ) -> None:
        # If a closer ancestor is excluded, that's reported first (we walk from
        # the leaf upward). This makes the coverage column show the *nearest*
        # explanation rather than skipping past it.
        excluded = {"3"}  # genus
        # walk: 4 -> not in set; 3 -> excluded; never reaches 10
        assert classify_coverage(
            "4", {"4": "3", "3": "2", "2": "1", "10": "1"}, excluded, included, "human"
        ) == ("excluded", "3")

    def test_other_host_not_matched(
        self,
        parent_map: dict[str, str],
        excluded: set[str],
        included: dict[str, set[str]],
    ) -> None:
        # taxid 10 is included for "human" but not "vertebrate"
        assert classify_coverage(
            "10", parent_map, excluded, included, "vertebrate"
        ) == (
            "",
            "",
        )

    def test_build_parent_map_from_db(self) -> None:
        db = pd.DataFrame(
            [
                {"taxid": "1", "parent_taxid": "0"},
                {"taxid": "2", "parent_taxid": "1"},
            ]
        )
        assert build_parent_map(db) == {"1": "0", "2": "1"}

    def test_annotate_changes_adds_coverage_columns(
        self,
        parent_map: dict[str, str],
        excluded: set[str],
        included: dict[str, set[str]],
    ) -> None:
        changes = pd.DataFrame(
            [
                {
                    "taxid": "4",
                    "name": "species under excluded family",
                    "rank": "species",
                    "old_status": "1",
                    "new_status": "0",
                },
                {
                    "taxid": "10",
                    "name": "covered by include",
                    "rank": "species",
                    "old_status": "0",
                    "new_status": "1",
                },
                {
                    "taxid": "999",
                    "name": "uncovered",
                    "rank": "species",
                    "old_status": "0",
                    "new_status": "1",
                },
            ]
        )
        out = annotate_changes_with_coverage(
            changes, "human", parent_map, excluded, included
        )
        assert list(out["covered_by"]) == ["excluded", "included", ""]
        assert list(out["covered_rule_taxid"]) == ["2", "10", ""]

    def test_annotate_empty_changes_still_has_columns(self) -> None:
        empty = pd.DataFrame(
            columns=["taxid", "name", "rank", "old_status", "new_status"]
        )
        out = annotate_changes_with_coverage(empty, "human", {}, set(), {})
        assert "covered_by" in out.columns
        assert "covered_rule_taxid" in out.columns
        assert "included_for_other_hosts" in out.columns
        assert out.empty

    def test_includes_for_other_hosts_flags_policy_gap(self) -> None:
        # taxid 5 is included for human + vertebrate but not primate.
        # When we ask about primate, includes_for_other_hosts should return
        # ['human', 'vertebrate']; when we ask about human, it returns [].
        parent_map = {"5": "1", "1": "0"}
        included = {"human": {"5"}, "vertebrate": {"5"}, "primate": set()}
        assert includes_for_other_hosts("5", parent_map, included, "primate") == [
            "human",
            "vertebrate",
        ]
        assert includes_for_other_hosts("5", parent_map, included, "human") == [
            "vertebrate"
        ]

    def test_includes_for_other_hosts_walks_lineage(self) -> None:
        # Ancestor 1 is included for human; descendant 5 should report that.
        parent_map = {"5": "3", "3": "1", "1": "0"}
        included = {"human": {"1"}}
        assert includes_for_other_hosts("5", parent_map, included, "primate") == [
            "human"
        ]

    def test_annotate_adds_other_hosts_column(self) -> None:
        # Banzi-virus-style case: taxid 5 is overridden for human + vertebrate.
        # In a primate demotion (covered_by == ""), we want
        # included_for_other_hosts == "human,vertebrate".
        parent_map = {"5": "1", "1": "0"}
        included = {"human": {"5"}, "vertebrate": {"5"}, "primate": set()}
        changes = pd.DataFrame(
            [
                {
                    "taxid": "5",
                    "name": "Banzi-like",
                    "rank": "species",
                    "old_status": "1",
                    "new_status": "0",
                }
            ]
        )
        out = annotate_changes_with_coverage(
            changes, "primate", parent_map, set(), included
        )
        assert out["covered_by"].iloc[0] == ""
        assert out["included_for_other_hosts"].iloc[0] == "human,vertebrate"

    def test_other_hosts_column_blank_when_covered_by_include(self) -> None:
        # If a taxid IS included for the host we're asking about, the
        # included_for_other_hosts column should be blank to avoid noise.
        parent_map = {"5": "1", "1": "0"}
        included = {"human": {"5"}, "vertebrate": {"5"}}
        changes = pd.DataFrame(
            [
                {
                    "taxid": "5",
                    "name": "x",
                    "rank": "species",
                    "old_status": "0",
                    "new_status": "1",
                }
            ]
        )
        out = annotate_changes_with_coverage(
            changes, "human", parent_map, set(), included
        )
        assert out["covered_by"].iloc[0] == "included"
        assert out["included_for_other_hosts"].iloc[0] == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
