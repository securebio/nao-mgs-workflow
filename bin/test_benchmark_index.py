#!/usr/bin/env python3
"""Pytest suite for the pure-function helpers in benchmark_index.py.

I/O staging (S3/local) is exercised by running the script end-to-end against
real index releases; this file covers the deterministic diff logic only.
"""

###########
# IMPORTS #
###########

from pathlib import Path

import pandas as pd
import pytest
from benchmark_index import (
    _ancestor_in,
    annotate_changes_with_coverage,
    annotate_cross_host_actionables,
    annotate_lost_genomes,
    build_parent_map,
    categorize_gained_genomes_raw,
    categorize_lost_genomes_raw,
    check_ref_staleness,
    classify_coverage,
    compare_size_listings,
    detect_bidirectional_flips,
    diff_genome_metadata,
    diff_params,
    diff_reassignments,
    diff_taxonomy,
    fasta_content_stats,
    includes_for_other_hosts,
    infection_status_changes,
    infection_status_columns,
    infection_status_transitions,
    parse_kraken_url_date,
    parse_silva_url_release,
    summarise_params_changes,
    surveilled_species,
    tsv_row_count,
)

###########
# FIXTURE #
###########


@pytest.fixture
def old_genome_meta() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "assembly_accession": "GCA_1",
                "genome_id": "G1",
                "taxid": "10",
                "species_taxid": "100",
                "organism_name": "A",
            },
            {
                "assembly_accession": "GCA_2",
                "genome_id": "G2",
                "taxid": "10",
                "species_taxid": "100",
                "organism_name": "A",
            },
            {
                "assembly_accession": "GCA_3",
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
                "assembly_accession": "GCA_2",
                "genome_id": "G2",
                "taxid": "10",
                "species_taxid": "100",
                "organism_name": "A",
            },
            {
                "assembly_accession": "GCA_4",
                "genome_id": "G4",
                "taxid": "20",
                "species_taxid": "200",
                "organism_name": "B",
            },
            {
                "assembly_accession": "GCA_5",
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


class TestDiffReassignments:
    def test_flags_species_change_in_intersection(self) -> None:
        old = pd.DataFrame(
            [
                {"genome_id": "G1", "species_taxid": "100", "organism_name": "A"},
                {"genome_id": "G2", "species_taxid": "100", "organism_name": "A"},
                {"genome_id": "G3", "species_taxid": "200", "organism_name": "B"},
            ]
        )
        new = pd.DataFrame(
            [
                # G1 reassigned 100 -> 300; G2 unchanged; G3 removed (not in new);
                # G9 added (not in old). Only G1 should count.
                {"genome_id": "G1", "species_taxid": "300", "organism_name": "A2"},
                {"genome_id": "G2", "species_taxid": "100", "organism_name": "A"},
                {"genome_id": "G9", "species_taxid": "400", "organism_name": "Z"},
            ]
        )
        flows = diff_reassignments(old, new)
        assert len(flows) == 1
        row = flows.iloc[0]
        assert row["old_species_taxid"] == "100"
        assert row["new_species_taxid"] == "300"
        assert int(row["n_genomes"]) == 1

    def test_empty_when_no_reassignment(self) -> None:
        meta = pd.DataFrame(
            [{"genome_id": "G1", "species_taxid": "100", "organism_name": "A"}]
        )
        flows = diff_reassignments(meta, meta)
        assert flows.empty
        assert "n_genomes" in flows.columns


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

    @pytest.mark.parametrize(
        "taxid,host,expected",
        [
            # taxid 4 is a descendant of excluded family 2
            ("4", "human", ("excluded", "2")),
            # taxid 10 is directly in the human-includes set
            ("10", "human", ("included", "10")),
            # taxid 1 (root) is neither excluded nor in any host's includes
            ("1", "human", ("", "")),
        ],
    )
    def test_classifies_lineage(
        self,
        parent_map: dict[str, str],
        excluded: set[str],
        included: dict[str, set[str]],
        taxid: str,
        host: str,
        expected: tuple[str, str],
    ) -> None:
        assert (
            classify_coverage(taxid, parent_map, excluded, included, host) == expected
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


class TestAnnotateLostGenomes:
    """The genome_id-level redistribution check classifies "lost" species by
    where their underlying genome_ids actually went. A species_taxid whose
    genomes were reassigned to a different species_taxid in the new metadata
    is `likely_rename = yes` (the genomes are still in the index); one whose
    genomes are absent from new metadata entirely is a true loss."""

    def test_redistribution_detected_via_genome_ids(self) -> None:
        # Species 100 had 4 genomes in old; all 4 are present in new under
        # species_taxid 999 — this is the Jingmen-tick-virus pattern. Should
        # be marked likely_rename=yes with redistribution annotations.
        lost = pd.DataFrame(
            [
                {
                    "species_taxid": "100",
                    "organism_name": "Old name",
                    "old_count": 4,
                    "new_count": 0,
                    "delta": -4,
                },
            ]
        )
        old_meta = pd.DataFrame(
            [
                {
                    "genome_id": f"G{i}",
                    "taxid": "100",
                    "species_taxid": "100",
                    "organism_name": "Old name",
                }
                for i in range(4)
            ]
        )
        new_meta = pd.DataFrame(
            [
                {
                    "genome_id": f"G{i}",
                    "taxid": "100",
                    "species_taxid": "999",
                    "organism_name": "New species",
                }
                for i in range(4)
            ]
        )
        new_db = pd.DataFrame([{"taxid": "100", "name": "Old name"}])
        out = annotate_lost_genomes(lost, old_meta, new_meta, new_db)
        row = out.iloc[0]
        assert row["likely_rename"] == "yes"
        assert row["redistributed_to_species_taxid"] == "999"
        assert row["redistributed_to_name"] == "New species"
        assert row["redistributed_genome_count"] == 4
        assert row["truly_lost_count"] == 0

    def test_true_loss_when_genome_ids_absent(self) -> None:
        lost = pd.DataFrame(
            [
                {
                    "species_taxid": "100",
                    "organism_name": "Gone",
                    "old_count": 3,
                    "new_count": 0,
                    "delta": -3,
                },
            ]
        )
        old_meta = pd.DataFrame(
            [
                {
                    "genome_id": f"G{i}",
                    "taxid": "100",
                    "species_taxid": "100",
                    "organism_name": "Gone",
                }
                for i in range(3)
            ]
        )
        new_meta = pd.DataFrame(
            [
                {
                    "genome_id": "X1",
                    "taxid": "200",
                    "species_taxid": "200",
                    "organism_name": "Different",
                }
            ]
        )
        new_db = pd.DataFrame([{"taxid": "100", "name": "Gone"}])
        out = annotate_lost_genomes(lost, old_meta, new_meta, new_db)
        row = out.iloc[0]
        assert row["likely_rename"] == "no"
        assert row["redistributed_genome_count"] == 0
        assert row["truly_lost_count"] == 3

    def test_partial_redistribution_uses_half_threshold(self) -> None:
        # 3 of 4 redistributed (>=50%) -> likely_rename=yes; 1 of 4 -> no.
        lost = pd.DataFrame(
            [
                {
                    "species_taxid": "100",
                    "organism_name": "Three of four",
                    "old_count": 4,
                    "new_count": 0,
                    "delta": -4,
                },
                {
                    "species_taxid": "200",
                    "organism_name": "One of four",
                    "old_count": 4,
                    "new_count": 0,
                    "delta": -4,
                },
            ]
        )
        old_meta = pd.DataFrame(
            [
                {
                    "genome_id": f"A{i}",
                    "taxid": "100",
                    "species_taxid": "100",
                    "organism_name": "Three of four",
                }
                for i in range(4)
            ]
            + [
                {
                    "genome_id": f"B{i}",
                    "taxid": "200",
                    "species_taxid": "200",
                    "organism_name": "One of four",
                }
                for i in range(4)
            ]
        )
        new_meta = pd.DataFrame(
            [
                {
                    "genome_id": f"A{i}",
                    "taxid": "100",
                    "species_taxid": "999",
                    "organism_name": "Dest",
                }
                for i in range(3)
            ]  # A3 absent
            + [
                {
                    "genome_id": "B0",
                    "taxid": "200",
                    "species_taxid": "888",
                    "organism_name": "Other",
                }
            ]  # B1-B3 absent
        )
        new_db = pd.DataFrame()
        out = annotate_lost_genomes(lost, old_meta, new_meta, new_db).set_index(
            "species_taxid"
        )
        assert out.loc["100", "likely_rename"] == "yes"
        assert out.loc["200", "likely_rename"] == "no"

    def test_hard_exclude_coverage_via_ancestry(self) -> None:
        # species 100 -> parent 50 -> parent 10 (excluded). Should be marked covered.
        lost = pd.DataFrame(
            [
                {
                    "species_taxid": "100",
                    "organism_name": "Excluded family member",
                    "old_count": 5,
                    "new_count": 0,
                    "delta": -5,
                },
                {
                    "species_taxid": "200",
                    "organism_name": "Unrelated",
                    "old_count": 2,
                    "new_count": 0,
                    "delta": -2,
                },
            ]
        )
        old_meta = pd.DataFrame()  # no redistribution; doesn't matter for coverage
        new_meta = pd.DataFrame()
        new_db = pd.DataFrame()
        parent_map = {"100": "50", "50": "10", "200": "20"}
        out = annotate_lost_genomes(
            lost,
            old_meta,
            new_meta,
            new_db,
            parent_map=parent_map,
            excluded_taxids={"10"},
        ).set_index("species_taxid")
        assert out.loc["100", "covered_by_hard_exclude"] == "10"
        assert out.loc["200", "covered_by_hard_exclude"] == ""

    def test_empty_input(self) -> None:
        empty = pd.DataFrame(
            columns=[
                "species_taxid",
                "organism_name",
                "old_count",
                "new_count",
                "delta",
            ]
        )
        out = annotate_lost_genomes(
            empty, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        )
        assert out.empty
        for col in (
            "redistributed_to_species_taxid",
            "redistributed_genome_count",
            "truly_lost_count",
            "likely_rename",
            "covered_by_hard_exclude",
        ):
            assert col in out.columns


class TestSurveilledSpecies:
    def test_positive_for_any_screened_host(self) -> None:
        db = pd.DataFrame(
            [
                {
                    "taxid": "1",
                    "infection_status_vertebrate": "1",
                    "infection_status_human": "0",
                },
                {
                    "taxid": "2",
                    "infection_status_vertebrate": "0",
                    "infection_status_human": "1",
                },
                {
                    "taxid": "3",
                    "infection_status_vertebrate": "0",
                    "infection_status_human": "0",
                },
            ]
        )
        assert surveilled_species(db, ["vertebrate", "human"]) == {"1", "2"}

    def test_unscreened_host_ignored(self) -> None:
        db = pd.DataFrame(
            [
                {
                    "taxid": "1",
                    "infection_status_vertebrate": "0",
                    "infection_status_bird": "1",
                }
            ]
        )
        # bird is not in the screen -> taxid 1 is not surveilled
        assert surveilled_species(db, ["vertebrate"]) == set()

    def test_missing_columns_returns_empty(self) -> None:
        assert (
            surveilled_species(pd.DataFrame([{"taxid": "1"}]), ["vertebrate"]) == set()
        )


class TestCategorizeLostGenomesRaw:
    """Exact lost-genome categorization keyed on the target index's pre-filter
    raw metadata (build-time taxid + assembly_status)."""

    @pytest.fixture
    def new_db(self) -> pd.DataFrame:
        # taxid 700 sits under excluded ancestor 70; 800 is an unsurveilled
        # species (810 is a child leaf of it); 900 is a surveilled species.
        return pd.DataFrame(
            [
                {
                    "taxid": "1",
                    "taxid_species": "",
                    "parent_taxid": "1",
                    "infection_status_vertebrate": "0",
                },
                {
                    "taxid": "70",
                    "taxid_species": "",
                    "parent_taxid": "1",
                    "infection_status_vertebrate": "0",
                },
                {
                    "taxid": "700",
                    "taxid_species": "700",
                    "parent_taxid": "70",
                    "infection_status_vertebrate": "0",
                },
                {
                    "taxid": "800",
                    "taxid_species": "800",
                    "parent_taxid": "1",
                    "infection_status_vertebrate": "0",
                },
                {
                    "taxid": "810",
                    "taxid_species": "800",
                    "parent_taxid": "800",
                    "infection_status_vertebrate": "0",
                },
                {
                    "taxid": "900",
                    "taxid_species": "900",
                    "parent_taxid": "1",
                    "infection_status_vertebrate": "1",
                },
            ]
        )

    @pytest.fixture
    def raw_meta(self) -> pd.DataFrame:
        cols = [
            "assembly_accession",
            "taxid",
            "organism_name",
            "source_database",
            "assembly_status",
        ]
        rows = [
            ("GCA_NC", "900", "X", "SOURCE_DATABASE_GENBANK", "suppressed"),
            ("GCA_HE", "700", "X", "SOURCE_DATABASE_GENBANK", "current"),
            ("GCA_RE", "810", "X", "SOURCE_DATABASE_GENBANK", "current"),
            ("GCA_DE", "800", "X", "SOURCE_DATABASE_GENBANK", "current"),
            ("GCA_OT", "900", "X", "SOURCE_DATABASE_GENBANK", "current"),
        ]  # GCA_ABS deliberately absent
        return pd.DataFrame(rows, columns=cols)

    @pytest.fixture
    def removed(self) -> pd.DataFrame:
        cols = [
            "assembly_accession",
            "genome_id",
            "taxid",
            "species_taxid",
            "organism_name",
        ]
        rows = [
            ("GCA_ABS", "gA", "100", "100", "Absent"),
            ("GCA_NC", "gN", "900", "900", "NonCurrent"),
            ("GCA_HE", "gH", "700", "700", "HardExcl"),
            ("GCA_RE", "gR", "200", "200", "Reassigned"),
            ("GCA_DE", "gD", "800", "800", "Demoted"),
            ("GCA_OT", "gO", "900", "900", "Other"),
        ]
        return pd.DataFrame(rows, columns=cols)

    def test_assigns_each_category(
        self, removed: pd.DataFrame, raw_meta: pd.DataFrame, new_db: pd.DataFrame
    ) -> None:
        out = categorize_lost_genomes_raw(
            removed, raw_meta, new_db, build_parent_map(new_db), {"70"}, ["vertebrate"]
        ).set_index("genome_id")
        assert out.loc["gA", "reason"] == "absent_from_ncbi"
        assert out.loc["gN", "reason"] == "non_current_genome_version"
        assert out.loc["gH", "reason"] == "hard_excluded"
        assert out.loc["gH", "reason_taxid"] == "70"
        assert out.loc["gR", "reason"] == "reassigned_to_excluded"
        # reason_taxid is the new *leaf* taxon, not its species rollup (800)
        assert out.loc["gR", "reason_taxid"] == "810"
        assert out.loc["gD", "reason"] == "infection_status_demotion"
        assert out.loc["gO", "reason"] == "other"

    def test_keyed_on_leaf_not_species_rollup(self, new_db: pd.DataFrame) -> None:
        # A genome whose leaf taxon is unchanged but whose species rollup moved
        # (taxonomy restructure) is a DEMOTION (same assigned taxon, status
        # flipped), not a reassignment. Species-keying would wrongly call it
        # reassigned. Leaf 810 is stable old->new; only its species rollup (800)
        # differs from the old species_taxid (200), and 810 is unsurveilled.
        removed = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_X",
                    "genome_id": "gX",
                    "taxid": "810",
                    "species_taxid": "200",
                    "organism_name": "restructured",
                }
            ]
        )
        raw = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_X",
                    "taxid": "810",
                    "organism_name": "x",
                    "source_database": "SOURCE_DATABASE_GENBANK",
                    "assembly_status": "current",
                }
            ]
        )
        out = categorize_lost_genomes_raw(
            removed, raw, new_db, build_parent_map(new_db), set(), ["vertebrate"]
        )
        assert out.iloc[0]["reason"] == "infection_status_demotion"

    def test_surveilled_via_species_rollup(self) -> None:
        # The surveillance predicate mirrors the inclusion filter: leaf positive
        # OR its species rollup positive. Leaf 950 is itself infection-negative
        # but rolls up to surveilled species 900, so it counts as surveilled ->
        # a lost genome here is "other" (present/surveilled yet absent), NOT a
        # demotion. Without the rollup it would be mislabelled a demotion.
        new_db = pd.DataFrame(
            [
                {
                    "taxid": "900",
                    "taxid_species": "900",
                    "parent_taxid": "1",
                    "infection_status_vertebrate": "1",
                },
                {
                    "taxid": "950",
                    "taxid_species": "900",
                    "parent_taxid": "900",
                    "infection_status_vertebrate": "0",
                },
            ]
        )
        removed = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_Y",
                    "genome_id": "gY",
                    "taxid": "950",
                    "species_taxid": "950",
                    "organism_name": "strain",
                }
            ]
        )
        raw = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_Y",
                    "taxid": "950",
                    "organism_name": "x",
                    "source_database": "SOURCE_DATABASE_GENBANK",
                    "assembly_status": "current",
                }
            ]
        )
        out = categorize_lost_genomes_raw(
            removed, raw, new_db, build_parent_map(new_db), set(), ["vertebrate"]
        )
        assert out.iloc[0]["reason"] == "other"

    def test_non_current_outranks_taxonomy_reasons(
        self, removed: pd.DataFrame, raw_meta: pd.DataFrame, new_db: pd.DataFrame
    ) -> None:
        # A hard-excludable assembly that is also non-current should report the
        # assembly-lifecycle reason (non-current wins over hard-exclude).
        raw_meta = raw_meta.copy()
        raw_meta.loc[raw_meta["assembly_accession"] == "GCA_HE", "assembly_status"] = (
            "suppressed"
        )
        out = categorize_lost_genomes_raw(
            removed, raw_meta, new_db, build_parent_map(new_db), {"70"}, ["vertebrate"]
        ).set_index("genome_id")
        assert out.loc["gH", "reason"] == "non_current_genome_version"

    def test_empty_input(self, raw_meta: pd.DataFrame, new_db: pd.DataFrame) -> None:
        empty = pd.DataFrame(
            columns=[
                "assembly_accession",
                "genome_id",
                "taxid",
                "species_taxid",
                "organism_name",
            ]
        )
        out = categorize_lost_genomes_raw(
            empty, raw_meta, new_db, {}, set(), ["vertebrate"]
        )
        assert out.empty
        assert "reason" in out.columns
        assert "reason_taxid" in out.columns


class TestContentMetrics:
    def test_fasta_stats_counts_records_bp_and_n(self, tmp_path: Path) -> None:
        fa = tmp_path / "sample.fa"
        fa.write_text(">r1\nACGT\nNNNN\n>r2\nACgtN\n")
        stats = fasta_content_stats(fa)
        assert stats == {"records": 2, "total_bp": 13, "n_bp": 5}

    def test_fasta_stats_handles_gzip(self, tmp_path: Path) -> None:
        import gzip

        fa = tmp_path / "sample.fa.gz"
        with gzip.open(fa, "wt") as f:
            f.write(">a\nACGTACGT\n>b\nNN\n")
        stats = fasta_content_stats(fa)
        assert stats == {"records": 2, "total_bp": 10, "n_bp": 2}

    def test_tsv_row_count_excludes_header(self, tmp_path: Path) -> None:
        t = tmp_path / "x.tsv"
        t.write_text("a\tb\n1\t2\n3\t4\n5\t6\n")
        assert tsv_row_count(t) == 3


class TestRefStaleness:
    @pytest.mark.parametrize(
        "url,expected",
        [
            (
                "https://genome-idx.s3.amazonaws.com/kraken/k2_standard_20251015.tar.gz",
                "20251015",
            ),
            ("https://example.com/foo.tar.gz", ""),
        ],
    )
    def test_parse_kraken_url_date(self, url: str, expected: str) -> None:
        assert parse_kraken_url_date(url) == expected

    @pytest.mark.parametrize(
        "url,expected",
        [
            # dotted form
            (
                "https://www.arb-silva.de/fileadmin/silva_databases/release_138.2/Exports/x.gz",
                "138.2",
            ),
            # underscore form (parses to same release identifier)
            ("https://ftp.arb-silva.de/release_138_2/Exports/x.gz", "138.2"),
            # no release token in URL
            ("https://example.com/foo", ""),
        ],
    )
    def test_parse_silva_url_release(self, url: str, expected: str) -> None:
        assert parse_silva_url_release(url) == expected

    def test_check_ref_staleness_passive_only(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # With no Kraken/SILVA URLs in params, only the passive refs should
        # appear (status=unknown), and no network calls are made.
        params = {
            "human_url": "https://example.com/genome.fa.gz",
            "taxonomy_url": "https://ftp.ncbi.nlm.nih.gov/.../new_taxdump.zip",
        }
        rows = check_ref_staleness(params)
        assert {r["ref"] for r in rows} == {"human_url", "taxonomy_url"}
        assert all(r["status"] == "unknown" for r in rows)

    @pytest.mark.parametrize(
        "current_url,latest_return,expected_status",
        [
            # current_date matches latest_date → current
            (
                "https://genome-idx.s3.amazonaws.com/kraken/k2_standard_20260226.tar.gz",
                ("20260226", "k2_standard_20260226.tar.gz"),
                "current",
            ),
            # current_date older than latest_date → stale
            (
                "https://genome-idx.s3.amazonaws.com/kraken/k2_standard_20250714.tar.gz",
                ("20260226", "k2_standard_20260226.tar.gz"),
                "stale",
            ),
            # fetcher returned None (network blip / parse failure) → error
            (
                "https://genome-idx.s3.amazonaws.com/kraken/k2_standard_20260226.tar.gz",
                None,
                "error",
            ),
        ],
    )
    def test_check_ref_staleness_kraken_branches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        current_url: str,
        latest_return: tuple[str, str] | None,
        expected_status: str,
    ) -> None:
        monkeypatch.setattr(
            "benchmark_index.latest_kraken_release", lambda: latest_return
        )
        rows = check_ref_staleness({"kraken_db": current_url})
        kraken_row = next(r for r in rows if r["ref"] == "kraken_db")
        assert kraken_row["status"] == expected_status

    @pytest.mark.parametrize(
        "current_url,latest_return,expected_status",
        [
            # current matches latest → current
            (
                "https://www.arb-silva.de/.../release_138.2/Exports/x.gz",
                "138.2",
                "current",
            ),
            # current older than latest → stale
            (
                "https://www.arb-silva.de/.../release_138.1/Exports/x.gz",
                "138.2",
                "stale",
            ),
            # fetcher returned None → error
            (
                "https://www.arb-silva.de/.../release_138.2/Exports/x.gz",
                None,
                "error",
            ),
        ],
    )
    def test_check_ref_staleness_silva_branches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        current_url: str,
        latest_return: str | None,
        expected_status: str,
    ) -> None:
        monkeypatch.setattr(
            "benchmark_index.latest_silva_release", lambda: latest_return
        )
        rows = check_ref_staleness({"ssu_url": current_url})
        silva_row = next(r for r in rows if r["ref"] == "ssu_url")
        assert silva_row["status"] == expected_status

    def test_check_ref_staleness_silva_call_hoisted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # When both ssu_url and lsu_url are present, latest_silva_release()
        # is called exactly once (the C3 hoist).
        calls = {"n": 0}

        def fake() -> str:
            calls["n"] += 1
            return "138.2"

        monkeypatch.setattr("benchmark_index.latest_silva_release", fake)
        check_ref_staleness(
            {
                "ssu_url": "https://www.arb-silva.de/.../release_138.2/Exports/ssu.gz",
                "lsu_url": "https://www.arb-silva.de/.../release_138.2/Exports/lsu.gz",
            }
        )
        assert calls["n"] == 1


class TestDetectBidirectionalFlips:
    """A species_taxid that's actionable in BOTH directions across different
    hosts is the upstream-VHDB-taxonomy-churn fingerprint that summary.md
    needs to surface as its own callout (the Turlock virus case)."""

    def _changes_row(
        self,
        taxid: str,
        old_s: str,
        new_s: str,
        name: str = "x",
        covered: str = "",
    ) -> dict[str, str]:
        return {
            "taxid": taxid,
            "name": name,
            "rank": "species",
            "old_status": old_s,
            "new_status": new_s,
            "covered_by": covered,
        }

    def test_detects_bidirectional_taxid(self) -> None:
        per_host = {
            "human": pd.DataFrame([self._changes_row("100", "0", "1", "T")]),
            "primate": pd.DataFrame([self._changes_row("100", "0", "1", "T")]),
            "bird": pd.DataFrame([self._changes_row("100", "1", "0", "T")]),
        }
        out = detect_bidirectional_flips(per_host)
        assert len(out) == 1
        row = out.iloc[0]
        assert row["taxid"] == "100"
        assert row["hosts_up"] == "human,primate"
        assert row["hosts_down"] == "bird"

    def test_single_direction_does_not_qualify(self) -> None:
        per_host = {
            "human": pd.DataFrame([self._changes_row("100", "0", "1")]),
            "primate": pd.DataFrame([self._changes_row("100", "0", "1")]),
        }
        assert detect_bidirectional_flips(per_host).empty

    def test_covered_rows_are_excluded(self) -> None:
        # If both flips are covered by rules, they shouldn't surface.
        per_host = {
            "human": pd.DataFrame(
                [self._changes_row("100", "0", "1", covered="excluded")]
            ),
            "bird": pd.DataFrame(
                [self._changes_row("100", "1", "0", covered="excluded")]
            ),
        }
        assert detect_bidirectional_flips(per_host).empty


class TestAnnotateCrossHostActionables:
    """`cross_host_actionable_on` mirrors `included_for_other_hosts` but for
    actionable transitions — surfacing that a single taxid is actionable on
    multiple hosts so the report doesn't write the same item up four times.
    `driven_by_genome_loss` flags demotions whose cause is the §3.1 genome
    loss rather than VHDB drift."""

    def _row(
        self,
        taxid: str,
        old_s: str,
        new_s: str,
        name: str = "x",
        covered: str = "",
    ) -> dict[str, str]:
        return {
            "taxid": taxid,
            "name": name,
            "rank": "species",
            "old_status": old_s,
            "new_status": new_s,
            "covered_by": covered,
        }

    def test_cross_host_lists_other_hosts_with_same_direction(self) -> None:
        per_host = {
            "human": pd.DataFrame([self._row("100", "0", "1")]),
            "primate": pd.DataFrame([self._row("100", "0", "1")]),
            "mammal": pd.DataFrame([self._row("100", "0", "1")]),
            "bird": pd.DataFrame([self._row("200", "1", "0")]),
        }
        out = annotate_cross_host_actionables(per_host, set())
        assert out["human"]["cross_host_actionable_on"].iloc[0] == "mammal,primate"
        assert out["primate"]["cross_host_actionable_on"].iloc[0] == "human,mammal"
        assert out["bird"]["cross_host_actionable_on"].iloc[0] == ""  # only host

    def test_covered_rows_get_empty_cross_host(self) -> None:
        per_host = {
            "human": pd.DataFrame([self._row("100", "0", "1", covered="excluded")]),
            "mammal": pd.DataFrame([self._row("100", "0", "1", covered="excluded")]),
        }
        out = annotate_cross_host_actionables(per_host, set())
        assert out["human"]["cross_host_actionable_on"].iloc[0] == ""

    def test_driven_by_genome_loss_only_on_demotions(self) -> None:
        per_host = {
            "mammal": pd.DataFrame(
                [
                    self._row("100", "1", "0"),  # taxid in lost set
                    self._row("200", "1", "0"),  # taxid not in lost set
                    self._row("300", "0", "1"),  # promotion, not demotion
                ]
            ),
        }
        out = annotate_cross_host_actionables(
            per_host, species_lost_taxids={"100", "300"}
        )
        gloss = out["mammal"].set_index("taxid")["driven_by_genome_loss"]
        assert gloss["100"] == "yes"
        assert gloss["200"] == ""
        assert gloss["300"] == ""  # promotions don't get the flag


class TestSummariseParamsChanges:
    def test_added_removed_changed(self) -> None:
        out = summarise_params_changes(
            {"a": 1, "b": "old", "kept": 42},
            {"a": 1, "b": "new", "c": "fresh", "kept": 42},
        ).set_index("key")
        assert "kept" not in out.index  # unchanged keys omitted
        assert out.loc["b", "kind"] == "changed"
        assert out.loc["b", "old"] == "old"
        assert out.loc["b", "new"] == "new"
        assert out.loc["c", "kind"] == "added"
        assert out.loc["c", "old"] == ""

    def test_truncates_long_values(self) -> None:
        long_val = "x" * 500
        out = summarise_params_changes({"k": "short"}, {"k": long_val})
        assert out.iloc[0]["new"].endswith("…")
        assert len(out.iloc[0]["new"]) <= 121


class TestAncestorIn:
    """`_ancestor_in` is the load-bearing lineage walk for `hard_excluded`
    and `hard_included` classification in the two categorizers below."""

    @pytest.mark.parametrize(
        "taxid,target,expected",
        [
            # Self-match: target hit at the starting taxid.
            ("4", {"4"}, "4"),
            # Ancestor match: target hit while walking up.
            ("4", {"2"}, "2"),
            # No match anywhere in lineage.
            ("4", {"99"}, ""),
            # Empty target set always misses.
            ("4", set(), ""),
        ],
    )
    def test_lineage_walk(self, taxid: str, target: set[str], expected: str) -> None:
        # 4 -> 3 -> 2 -> 1 (root, self-loop).
        parent_map = {"4": "3", "3": "2", "2": "1", "1": "1"}
        assert _ancestor_in(taxid, parent_map, target) == expected

    def test_terminates_on_self_loop_at_root(self) -> None:
        # Root taxid's parent is itself ({"1": "1"}); the walk must terminate
        # rather than spinning forever.
        assert _ancestor_in("1", {"1": "1"}, {"99"}) == ""

    def test_terminates_on_missing_parent(self) -> None:
        # A taxid whose parent isn't in the map is treated as root.
        assert _ancestor_in("4", {}, {"99"}) == ""


class TestCategorizeGainedGenomesRaw:
    """Leaf-keyed gained-gid categorizer: genome-lifecycle reasons (release date
    / source) before taxonomy / policy reasons."""

    OLD_BUILD = "2025-08-25"

    @pytest.fixture
    def old_db(self) -> pd.DataFrame:
        # 400 present but unsurveilled (promotion); 800 present and surveilled
        # (other); 300/100 present; 700 absent (new taxon).
        return pd.DataFrame(
            [
                {
                    "taxid": "100",
                    "taxid_species": "100",
                    "infection_status_vertebrate": "1",
                },
                {
                    "taxid": "300",
                    "taxid_species": "300",
                    "infection_status_vertebrate": "1",
                },
                {
                    "taxid": "400",
                    "taxid_species": "400",
                    "infection_status_vertebrate": "0",
                },
                {
                    "taxid": "800",
                    "taxid_species": "800",
                    "infection_status_vertebrate": "1",
                },
            ]
        )

    @pytest.fixture
    def raw_meta(self) -> pd.DataFrame:
        cols = [
            "assembly_accession",
            "taxid",
            "organism_name",
            "source_database",
            "assembly_status",
            "release_date",
        ]
        rows = [
            ("GCA_NEW", "600", "x", "SOURCE_DATABASE_GENBANK", "current", "2026-01-01"),
            ("GCA_RS", "300", "x", "SOURCE_DATABASE_REFSEQ", "current", "2010-01-01"),
            ("GCA_HI", "100", "x", "SOURCE_DATABASE_GENBANK", "current", "2010-01-01"),
            ("GCA_NT", "700", "x", "SOURCE_DATABASE_GENBANK", "current", "2010-01-01"),
            ("GCA_PR", "400", "x", "SOURCE_DATABASE_GENBANK", "current", "2010-01-01"),
            ("GCA_OT", "800", "x", "SOURCE_DATABASE_GENBANK", "current", "2010-01-01"),
        ]
        return pd.DataFrame(rows, columns=cols)

    @pytest.fixture
    def added(self) -> pd.DataFrame:
        cols = [
            "assembly_accession",
            "genome_id",
            "taxid",
            "species_taxid",
            "organism_name",
        ]
        rows = [
            ("GCA_NEW", "gNEW", "600", "600", "new deposit"),
            ("GCA_RS", "gRS", "300", "300", "refseq pull-in"),
            ("GCA_HI", "gHI", "100", "100", "overridden"),
            ("GCA_NT", "gNT", "700", "700", "new taxon"),
            ("GCA_PR", "gPR", "400", "400", "promoted"),
            ("GCA_OT", "gOT", "800", "800", "pre-existing surveilled"),
        ]
        return pd.DataFrame(rows, columns=cols)

    # leaf 100 -> parent 50 (in overrides); everything else roots at 1
    PARENT_MAP = {
        "100": "50",
        "50": "1",
        "300": "1",
        "400": "1",
        "700": "1",
        "800": "1",
    }

    def test_assigns_each_category(
        self, added: pd.DataFrame, raw_meta: pd.DataFrame, old_db: pd.DataFrame
    ) -> None:
        out = categorize_gained_genomes_raw(
            added,
            raw_meta,
            old_db,
            self.PARENT_MAP,
            {"host": {"50"}},
            ["vertebrate"],
            self.OLD_BUILD,
        ).set_index("genome_id")
        assert out.loc["gNEW", "reason"] == "newly_deposited"
        assert out.loc["gHI", "reason"] == "hard_included"
        assert out.loc["gHI", "reason_taxid"] == "50"  # matched override ancestor
        assert out.loc["gNT", "reason"] == "new_taxon_in_taxonomy"
        assert out.loc["gPR", "reason"] == "infection_status_promotion"
        # Both the pre-existing RefSeq pull-in (gRS) and the pre-existing GenBank
        # genome of an already-eligible taxon (gOT) are the same general bucket;
        # source is recorded for the §3.3 driver annotation, not the category.
        assert out.loc["gRS", "reason"] == "pre_existing_reincluded"
        assert out.loc["gOT", "reason"] == "pre_existing_reincluded"
        assert out.loc["gRS", "source_database"] == "SOURCE_DATABASE_REFSEQ"

    def test_missing_release_date_is_no_release_date(
        self, old_db: pd.DataFrame
    ) -> None:
        # Without a release date the new-vs-pre-existing call can't be made.
        added = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_U",
                    "genome_id": "g",
                    "taxid": "800",
                    "species_taxid": "800",
                    "organism_name": "x",
                }
            ]
        )
        raw = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_U",
                    "taxid": "800",
                    "organism_name": "x",
                    "source_database": "SOURCE_DATABASE_GENBANK",
                    "assembly_status": "current",
                    "release_date": "",
                }
            ]
        )
        out = categorize_gained_genomes_raw(
            added, raw, old_db, {}, {}, ["vertebrate"], self.OLD_BUILD
        )
        assert out.iloc[0]["reason"] == "no_release_date"

    def test_newly_deposited_outranks_pre_existing(self, old_db: pd.DataFrame) -> None:
        # A RefSeq assembly released *after* the old build is a genuine new
        # deposit, not a pre-existing re-inclusion.
        added = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_A",
                    "genome_id": "g",
                    "taxid": "300",
                    "species_taxid": "300",
                    "organism_name": "x",
                }
            ]
        )
        raw = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_A",
                    "taxid": "300",
                    "organism_name": "x",
                    "source_database": "SOURCE_DATABASE_REFSEQ",
                    "assembly_status": "current",
                    "release_date": "2026-02-02",
                }
            ]
        )
        out = categorize_gained_genomes_raw(
            added, raw, old_db, {}, {}, ["vertebrate"], self.OLD_BUILD
        )
        assert out.iloc[0]["reason"] == "newly_deposited"

    def test_old_surveilled_via_species_rollup(self) -> None:
        # The old-surveillance check mirrors the filter: leaf positive OR its old
        # species rollup positive. Leaf 950 was infection-negative in the old DB
        # but rolls up to surveilled species 900, so it was already surveilled
        # then -> a pre-existing genome of it is NOT a promotion. Without the
        # rollup it would be mislabelled an infection_status_promotion.
        old_db = pd.DataFrame(
            [
                {
                    "taxid": "900",
                    "taxid_species": "900",
                    "infection_status_vertebrate": "1",
                },
                {
                    "taxid": "950",
                    "taxid_species": "900",
                    "infection_status_vertebrate": "0",
                },
            ]
        )
        added = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_Z",
                    "genome_id": "gZ",
                    "taxid": "950",
                    "species_taxid": "950",
                    "organism_name": "strain",
                }
            ]
        )
        raw = pd.DataFrame(
            [
                {
                    "assembly_accession": "GCA_Z",
                    "taxid": "950",
                    "organism_name": "x",
                    "source_database": "SOURCE_DATABASE_GENBANK",
                    "assembly_status": "current",
                    "release_date": "2010-01-01",
                }
            ]
        )
        out = categorize_gained_genomes_raw(
            added, raw, old_db, {}, {}, ["vertebrate"], self.OLD_BUILD
        )
        assert out.iloc[0]["reason"] == "pre_existing_reincluded"

    def test_empty_input_has_columns(
        self, raw_meta: pd.DataFrame, old_db: pd.DataFrame
    ) -> None:
        empty = pd.DataFrame(
            columns=[
                "assembly_accession",
                "genome_id",
                "taxid",
                "species_taxid",
                "organism_name",
            ]
        )
        out = categorize_gained_genomes_raw(
            empty, raw_meta, old_db, {}, {}, ["vertebrate"], self.OLD_BUILD
        )
        assert "reason" in out.columns
        assert out.empty


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
