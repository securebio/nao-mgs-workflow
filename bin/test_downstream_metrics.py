"""Unit tests for downstream_metrics.py (pure calculation functions).

Tests use small synthetic manifests/DataFrames only -- never real delivery data.
Test order mirrors the order of functions in downstream_metrics.py.
"""

import downstream_metrics as dm
import pandas as pd
import pytest


def _entry(
    n_rows: int | None = None,
    columns: list[str] | None = None,
) -> dm.FileEntry:
    return dm.FileEntry(n_rows=n_rows, columns=columns)


def _manifest(spec: dict[str, tuple[str, dict[str, dm.FileEntry]]]) -> dm.SideManifest:
    """Build a SideManifest from {group: (platform, {file_type: FileEntry})}."""
    return {
        g: dm.GroupManifest(platform=plat, files=files)
        for g, (plat, files) in spec.items()
    }


class TestCompareFileInventory:
    def test_present_and_expected_missing_files(self) -> None:
        reference = _manifest(
            {
                "G1": (
                    "illumina",
                    {"validation_hits": _entry(n_rows=100, columns=["a"])},
                )
            }
        )
        candidate = _manifest(
            {
                "G1": (
                    "illumina",
                    {"validation_hits": _entry(n_rows=150, columns=["a"])},
                )
            }
        )
        df = dm.compare_file_inventory(
            reference,
            candidate,
            {"illumina": {"validation_hits", "bracken"}},
        ).set_index("file_type")
        assert df.loc["validation_hits"].in_reference
        assert df.loc["validation_hits"].in_candidate
        assert not df.loc["bracken"].in_reference
        assert not df.loc["bracken"].in_candidate
        assert df.loc["validation_hits"].row_delta == 50
        assert df.loc["validation_hits"].row_pct_change == 50.0

    @pytest.mark.parametrize(
        ("reference_rows", "candidate_rows", "expected_delta"),
        [(0, 3, 3), (None, 3, None)],
    )
    def test_row_pct_is_null_without_nonzero_reference(
        self,
        reference_rows: int | None,
        candidate_rows: int,
        expected_delta: int | None,
    ) -> None:
        reference = _manifest(
            {"G1": ("illumina", {"bracken": _entry(n_rows=reference_rows)})}
        )
        candidate = _manifest(
            {"G1": ("illumina", {"bracken": _entry(n_rows=candidate_rows)})}
        )
        row = dm.compare_file_inventory(reference, candidate).iloc[0]
        if expected_delta is None:
            assert pd.isna(row.row_delta)
        else:
            assert row.row_delta == expected_delta
        assert pd.isna(row.row_pct_change)

    def test_presence_mismatch_one_side(self) -> None:
        reference = _manifest({"G1": ("ont", {"kraken": _entry(columns=["t"])})})
        candidate = _manifest(
            {
                "G1": ("ont", {"kraken": _entry(columns=["t"])}),
                "G2": ("ont", {"kraken": _entry(columns=["t"])}),
            }
        )
        df = dm.compare_file_inventory(reference, candidate)
        g2 = df[df.group == "G2"].iloc[0]
        assert not g2.in_reference
        assert g2.in_candidate

    def test_platform_mismatch_unions_expected_types(self) -> None:
        # Illumina on reference, ONT on candidate (degraded): report the mismatch and still
        # surface Illumina-only expected types missing on both sides.
        reference = _manifest(
            {"G": ("illumina", {"clade_counts": _entry(columns=["t"])})}
        )
        candidate = _manifest({"G": ("ont", {"kraken": _entry(columns=["t"])})})
        expected = {
            "illumina": {"clade_counts", "kraken", "bracken"},
            "ont": {"kraken"},
        }
        df = dm.compare_file_inventory(reference, candidate, expected)
        assert "mismatch" in df.iloc[0].platform
        # bracken is illumina-expected and absent on both sides -> still a row.
        assert (df.file_type == "bracken").any()


class TestCompareColumnsToSchema:
    def test_conformant_columns_report_clean(self) -> None:
        cols = ["seq_id", "group"]
        man = _manifest({"G1": ("illumina", {"validation_hits": _entry(columns=cols)})})
        df = dm.compare_columns_to_schema(man, man, {"validation_hits": cols})
        row = df.iloc[0]
        assert row.missing_vs_schema_reference == ""
        assert row.extra_vs_schema_reference == ""
        assert row.groups_consistent_reference

    def test_empty_file_reports_empty_marker_not_full_schema(self) -> None:
        man = _manifest({"G1": ("illumina", {"bracken": _entry(columns=[])})})
        df = dm.compare_columns_to_schema(
            man, man, {"bracken": ["taxid", "name", "fraction_total_reads"]}
        )
        row = df.iloc[0]
        assert row.missing_vs_schema_reference == "(empty file)"
        assert row.missing_vs_schema_candidate == "(empty file)"

    def test_column_added_in_candidate_is_flagged(self) -> None:
        reference = _manifest(
            {"G1": ("illumina", {"kraken": _entry(columns=["taxid"])})}
        )
        candidate = _manifest(
            {"G1": ("illumina", {"kraken": _entry(columns=["taxid", "new_col"])})}
        )
        df = dm.compare_columns_to_schema(reference, candidate, {"kraken": ["taxid"]})
        row = df.iloc[0]
        # A column added on candidate but absent from the schema surfaces as extra.
        assert row.extra_vs_schema_candidate == "new_col"
        assert row.extra_vs_schema_reference == ""

    def test_file_type_without_schema_still_reported(self) -> None:
        man = _manifest({"G1": ("illumina", {"mystery": _entry(columns=["x"])})})
        df = dm.compare_columns_to_schema(man, man, {})
        row = df[df.file_type == "mystery"].iloc[0]
        # Without a schema we can't judge missing/extra; has_schema=False is the
        # signal that an output lacks a schema entirely.
        assert not row.has_schema
        assert row.extra_vs_schema_reference == ""

    def test_inconsistent_groups_are_aggregated(self) -> None:
        reference = _manifest(
            {
                "G1": ("illumina", {"kraken": _entry(columns=["taxid", "name"])}),
                "G2": ("illumina", {"kraken": _entry(columns=["taxid"])}),
            }
        )
        df = dm.compare_columns_to_schema(
            reference, reference, {"kraken": ["taxid", "name"]}
        )
        row = df[df.file_type == "kraken"].iloc[0]
        assert not row.groups_consistent_reference
        assert row.missing_vs_schema_reference == "name"


def _qc_row(group: str, sample: str, stage: str, platform: str, **vals: object) -> dict:
    base: dict[str, object] = {
        "group": group,
        "sample": sample,
        "stage": stage,
        "platform": platform,
    }
    base.update(vals)
    return base


class TestCompareQcNumeric:
    def test_delta_and_pct_per_key(self) -> None:
        reference = pd.DataFrame(
            [
                _qc_row(
                    "G1",
                    "S1",
                    "cleaned",
                    "illumina",
                    n_reads_single=1000,
                    percent_gc=50.0,
                )
            ]
        )
        candidate = pd.DataFrame(
            [
                _qc_row(
                    "G1",
                    "S1",
                    "cleaned",
                    "illumina",
                    n_reads_single=900,
                    percent_gc=51.0,
                )
            ]
        )
        out = dm.compare_qc_numeric(
            reference, candidate, metrics=("n_reads_single", "percent_gc")
        )
        reads = out[out.metric == "n_reads_single"].iloc[0]
        assert reads.delta == -100
        assert reads["pct_change"] == -10.0
        gc = out[out.metric == "percent_gc"].iloc[0]
        assert gc.delta == 1.0

    def test_na_metric_yields_na_delta(self) -> None:
        # ONT: n_read_pairs is NA on both sides.
        reference = pd.DataFrame(
            [_qc_row("G1", "S1", "raw", "ont", n_read_pairs=None, n_reads_single=500)]
        )
        candidate = pd.DataFrame(
            [_qc_row("G1", "S1", "raw", "ont", n_read_pairs=None, n_reads_single=480)]
        )
        out = dm.compare_qc_numeric(reference, candidate)
        pairs = out[out.metric == "n_read_pairs"].iloc[0]
        assert pd.isna(pairs.delta)
        single = out[out.metric == "n_reads_single"].iloc[0]
        assert single.delta == -20

    def test_zero_reference_gives_na_pct(self) -> None:
        reference = pd.DataFrame(
            [_qc_row("G1", "S1", "raw", "illumina", n_reads_single=0)]
        )
        candidate = pd.DataFrame(
            [_qc_row("G1", "S1", "raw", "illumina", n_reads_single=5)]
        )
        out = dm.compare_qc_numeric(reference, candidate, metrics=("n_reads_single",))
        assert pd.isna(out.iloc[0]["pct_change"])
        assert out.iloc[0].delta == 5


class TestCompareQcFlags:
    FLAGS = ["per_base_sequence_quality", "per_tile_sequence_quality"]

    def test_only_changed_flags_returned(self) -> None:
        reference = pd.DataFrame(
            [
                {
                    "group": "G1",
                    "sample": "S1",
                    "stage": "raw",
                    "per_base_sequence_quality": "pass",
                    "per_tile_sequence_quality": "warn",
                }
            ]
        )
        candidate = pd.DataFrame(
            [
                {
                    "group": "G1",
                    "sample": "S1",
                    "stage": "raw",
                    "per_base_sequence_quality": "fail",
                    "per_tile_sequence_quality": "warn",
                }
            ]
        )
        out = dm.compare_qc_flags(reference, candidate, self.FLAGS)
        assert list(out.check) == ["per_base_sequence_quality"]
        assert (
            out.iloc[0].reference_flag == "pass"
            and out.iloc[0].candidate_flag == "fail"
        )

    def test_na_vs_na_not_flagged(self) -> None:
        # Both sides NA must NOT register as a change (the float-NaN bug).
        reference = pd.DataFrame(
            [
                {
                    "group": "G1",
                    "sample": "S1",
                    "stage": "raw",
                    "per_tile_sequence_quality": None,
                }
            ]
        )
        candidate = pd.DataFrame(
            [
                {
                    "group": "G1",
                    "sample": "S1",
                    "stage": "raw",
                    "per_tile_sequence_quality": None,
                }
            ]
        )
        out = dm.compare_qc_flags(reference, candidate, ["per_tile_sequence_quality"])
        assert out.empty


def _kr(group: str, ribosomal: bool, rank: str, taxid: int, name: str, n: int) -> dict:
    return {
        "group": group,
        "ribosomal": ribosomal,
        "rank": rank,
        "taxid": taxid,
        "name": name,
        "n_reads_clade": n,
    }


class TestKrakenRelativeAbundance:
    def test_filters_rank_and_aggregates_samples_to_fraction(self) -> None:
        # Two species at rank S (plus a genus row that must be ignored).
        df = pd.DataFrame(
            [
                _kr("G1", False, "S", 1, "sp1", 30),
                _kr("G1", False, "S", 2, "sp2", 10),
                _kr("G1", False, "G", 9, "genus", 999),
            ]
        )
        out = dm.kraken_relative_abundance(df, "S")
        assert set(out.taxid) == {1, 2}
        rel = dict(zip(out.taxid, out.rel, strict=True))
        assert rel[1] == 0.75 and rel[2] == 0.25

    def test_zero_total_set_dropped(self) -> None:
        df = pd.DataFrame([_kr("G1", False, "S", 1, "sp1", 0)])
        out = dm.kraken_relative_abundance(df, "S")
        assert out.empty


class TestKrakenBrayCurtis:
    def test_identical_profiles_give_zero(self) -> None:
        df = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 50), _kr("G1", False, "S", 2, "b", 50)]
        )
        out = dm.kraken_bray_curtis(df, df, ranks=("S",))
        assert out.iloc[0].bray_curtis == 0.0

    def test_disjoint_profiles_give_one(self) -> None:
        reference = pd.DataFrame([_kr("G1", False, "S", 1, "a", 100)])
        candidate = pd.DataFrame([_kr("G1", False, "S", 2, "b", 100)])
        out = dm.kraken_bray_curtis(reference, candidate, ranks=("S",))
        assert out.iloc[0].bray_curtis == 1.0

    def test_known_intermediate_value(self) -> None:
        # reference 80/20, candidate 50/50 -> TVD = 0.5*(|.8-.5|+|.2-.5|) = 0.3
        reference = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 80), _kr("G1", False, "S", 2, "b", 20)]
        )
        candidate = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 50), _kr("G1", False, "S", 2, "b", 50)]
        )
        out = dm.kraken_bray_curtis(reference, candidate, ranks=("S",))
        assert abs(out.iloc[0].bray_curtis - 0.3) < 1e-9
        assert out.iloc[0].n_taxa_union == 2

    def test_one_sided_set_gives_one_not_half(self) -> None:
        # A (group, ribosomal) set present on only one side: Bray-Curtis must be
        # 1.0 (disjoint), not 0.5 from a naive 0.5*L1 on an all-zero other side.
        reference = pd.DataFrame([_kr("G1", False, "S", 1, "a", 100)])
        candidate = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 100), _kr("G1", True, "S", 2, "b", 50)]
        )
        out = dm.kraken_bray_curtis(reference, candidate, ranks=("S",))
        rib = out[out.ribosomal].iloc[0]
        assert rib.bray_curtis == 1.0
        nonrib = out[~out.ribosomal].iloc[0]
        assert nonrib.bray_curtis == 0.0


class TestKrakenTopMovers:
    def test_orders_by_abs_change_and_signs_delta(self) -> None:
        reference = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 90), _kr("G1", False, "S", 2, "b", 10)]
        )
        candidate = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 10), _kr("G1", False, "S", 2, "b", 90)]
        )
        out = dm.kraken_top_movers(reference, candidate, "S", n=1)
        top = out.iloc[0]
        # Both move 80pp; tie broken arbitrarily but delta sign must be correct.
        assert abs(abs(top.delta_pp) - 80.0) < 1e-9
        if top.taxid == 1:
            assert top.delta_pp < 0
        else:
            assert top.delta_pp > 0

    def test_mover_rank_marks_largest_including_zero_to_present(self) -> None:
        # A taxon appearing 0->present (a large positive Δ) must rank above a small
        # negative mover, so mover_rank==1 surfaces the true dominant change rather
        # than whatever a reader spots first.
        reference = pd.DataFrame(
            [
                _kr("G1", False, "S", 1, "small_drop", 55),
                _kr("G1", False, "S", 2, "x", 45),
            ]
        )
        candidate = pd.DataFrame(
            [
                _kr("G1", False, "S", 1, "small_drop", 50),
                _kr("G1", False, "S", 2, "x", 45),
                _kr("G1", False, "S", 3, "newcomer", 60),
            ]
        )
        out = dm.kraken_top_movers(reference, candidate, "S", n=10)
        rank1 = out[out.mover_rank == 1].iloc[0]
        assert rank1["name"] == "newcomer"
        assert rank1.pct_reference == 0.0 and rank1.pct_candidate > 0
        # abs_delta_pp matches |delta_pp| and ranks descend.
        assert rank1.abs_delta_pp == pytest.approx(abs(rank1.delta_pp))
        assert list(out.mover_rank) == sorted(out.mover_rank)


def _synthetic_tree() -> dm.TaxonomyTree:
    """A small taxonomy mimicking the viral root layout.

    1 (root, no rank)
    +- 10239 Viruses (acellular root)
       +- 100 realmA (realm)
       |  +- 200 familyX (family)
       |  |  +- 300 genusG (genus)
       |  |     +- 401 speciesA (species)
       |  |     +- 402 speciesB (species)
       |  +- 210 familyY (family)
       |     +- 410 speciesC (species)
       +- 110 realmB (realm)
          +- 500 speciesD (species)
    +- 2 cellular (no rank) -> 600 bacSpecies (species)
    """
    parent = {
        1: 1,
        10239: 1,
        100: 10239,
        200: 100,
        300: 200,
        401: 300,
        402: 300,
        210: 100,
        410: 210,
        110: 10239,
        500: 110,
        2: 1,
        600: 2,
    }
    rank = {
        1: "no rank",
        10239: "acellular root",
        100: "realm",
        200: "family",
        300: "genus",
        401: "species",
        402: "species",
        210: "family",
        410: "species",
        110: "realm",
        500: "species",
        2: "no rank",
        600: "species",
    }
    return dm.TaxonomyTree(parent, rank)


class TestTaxonomyTree:
    def setup_method(self) -> None:
        self.tax = _synthetic_tree()

    @pytest.mark.parametrize(
        ("a", "b", "expected"),
        [
            (401, 401, "identical"),
            (401, 402, "same-genus"),  # siblings under genus 300
            (401, 410, "same-realm"),  # different family, share realm 100
            (401, 500, dm.SHARED_HIGHER),  # different realm, share Viruses root
            (401, 10239, dm.SHARED_HIGHER),  # rolled up to Viruses
            (401, 600, dm.CROSS_ROOT),  # virus vs bacterial species
            (401, 999, dm.UNRESOLVED_TAXID),  # 999 absent from the taxonomy
        ],
    )
    def test_divergence_bucket(self, a: int, b: int, expected: str) -> None:
        assert self.tax.divergence_bucket(a, b) == expected


class TestJoinReadAssignments:
    def _vh(self, rows: list[list]) -> pd.DataFrame:
        return pd.DataFrame(rows, columns=["group", "seq_id", "aligner_taxid_lca"])

    def test_lost_gained_same_reassigned(self) -> None:
        reference = self._vh([["G", "r1", 401], ["G", "r2", 401], ["G", "r3", 401]])
        candidate = self._vh([["G", "r1", 401], ["G", "r2", 402], ["G", "r4", 500]])
        out = dm.join_read_assignments(reference, candidate)
        status = dict(zip(out.seq_id, out.status, strict=True))
        assert status["r1"] == "same"
        assert status["r2"] == "reassigned"
        assert status["r3"] == "lost"
        assert status["r4"] == "gained"


class TestSummariseAndBuckets:
    def _joined(self) -> pd.DataFrame:
        reference = pd.DataFrame(
            [["G", "r1", 401], ["G", "r2", 401], ["G", "r3", 600]],
            columns=["group", "seq_id", "aligner_taxid_lca"],
        )
        candidate = pd.DataFrame(
            [["G", "r1", 402], ["G", "r2", 401], ["G", "r4", 401]],
            columns=["group", "seq_id", "aligner_taxid_lca"],
        )
        return dm.join_read_assignments(reference, candidate)

    def test_vertebrate_scope_filters(self) -> None:
        joined = self._joined()
        # 401/402 vertebrate; 600 (bacterial) not.
        summ = dm.summarize_read_status(joined, vert={401, 402})
        vert = summ[summ.scope == "vertebrate"].iloc[0]
        # r3 (600->lost) is excluded from vertebrate scope.
        assert vert.n_lost == 0
        all_ = summ[summ.scope == "all"].iloc[0]
        assert all_.n_lost == 1

    def test_dominant_gained_and_lost_taxon(self) -> None:
        # Two reads gained on 402 and one on 401; one read lost on 401. The
        # dominant gained taxon is 402 (2 of 3 gained), the dominant lost is 401.
        reference = pd.DataFrame(
            [["G", "r1", 401], ["G", "r2", 401]],
            columns=["group", "seq_id", "aligner_taxid_lca"],
        )
        candidate = pd.DataFrame(
            [["G", "r1", 401], ["G", "g1", 402], ["G", "g2", 402], ["G", "g3", 401]],
            columns=["group", "seq_id", "aligner_taxid_lca"],
        )
        joined = dm.join_read_assignments(reference, candidate)
        summ = dm.summarize_read_status(
            joined, vert={401, 402}, name_map={402: "speciesB", 401: "speciesA"}
        )
        row = summ[summ.scope == "vertebrate"].iloc[0]
        assert row.dominant_gained_taxid == 402
        assert row.dominant_gained_name == "speciesB"
        assert row.dominant_gained_reads == 2
        assert row.dominant_gained_frac == pytest.approx(2 / 3)
        assert row.dominant_lost_taxid == 401
        assert row.dominant_lost_name == "speciesA"

    def test_no_gained_leaves_dominant_empty(self) -> None:
        # A group with no gained reads must leave the gained-driver fields empty
        # (None), not raise or fabricate a taxon.
        reference = pd.DataFrame(
            [["G", "r1", 401]], columns=["group", "seq_id", "aligner_taxid_lca"]
        )
        candidate = pd.DataFrame(
            [["G", "r1", 401]], columns=["group", "seq_id", "aligner_taxid_lca"]
        )
        joined = dm.join_read_assignments(reference, candidate)
        row = dm.summarize_read_status(joined, vert={401}).iloc[0]
        assert pd.isna(row.dominant_gained_taxid)
        assert pd.isna(row.dominant_lost_taxid)

    def test_zero_vertebrate_group_still_gets_a_row(self) -> None:
        # A group with no vertebrate reads must still get an explicit zero row
        # (so "none observed" differs from "not computed").
        joined = self._joined()
        summ = dm.summarize_read_status(joined, vert=set())  # nothing vertebrate
        vert = summ[summ.scope == "vertebrate"]
        assert len(vert) == 1
        assert vert.iloc[0].n_reference == 0 and vert.iloc[0].n_candidate == 0

    def test_bucket_summary_empty_emits_canonical_zero_rows(self) -> None:
        empty = pd.DataFrame(
            columns=[
                "group",
                "scope",
                "seq_id",
                "taxid_reference",
                "taxid_candidate",
                "bucket",
            ]
        )
        out = dm.bucket_summary(empty)
        assert set(out.scope) == {"all", "vertebrate"}
        assert "unresolved-taxid" in set(out.bucket)
        assert (out.n_reads == 0).all()

    def test_bucket_summary_populated_counts_and_canonical_buckets(self) -> None:
        joined = self._joined()
        tax = _synthetic_tree()
        pairs = dm.reassignment_pair_counts(joined, tax, vert={401, 402})
        buckets = dm.bucket_summary(pairs)
        all_buckets = buckets[buckets.scope == "all"]
        # All canonical buckets are emitted (0 when none) so zero rows are visible.
        assert "unresolved-taxid" in set(all_buckets.bucket)
        assert "cross-root" in set(all_buckets.bucket)
        nonzero = all_buckets[all_buckets.n_reads > 0]
        # r1: 401->402 is the only reassigned read, a same-genus move.
        assert list(nonzero.bucket) == ["same-genus"]
        assert nonzero.iloc[0].n_reads == 1
        # Vertebrate scope is always emitted with all canonical buckets; here r1
        # (401->402) is vertebrate, so it carries the same-genus read too.
        vert_buckets = buckets[buckets.scope == "vertebrate"]
        assert "cross-root" in set(vert_buckets.bucket)
        vert_nonzero = vert_buckets[vert_buckets.n_reads > 0]
        assert list(vert_nonzero.bucket) == ["same-genus"]


class TestCladeRankShares:
    # rank_map from the (complete) taxonomy; name_map from annotations. 10239 is
    # the Viruses root whose count is the total-viral denominator.
    RANK = {dm.VIRUSES_TAXID: "acellular root", 200: "family", 210: "family"}
    NAME = {200: "familyX", 210: "familyY"}

    def _clade(self, rows: list[list]) -> pd.DataFrame:
        rows = [row if len(row) == 4 else [*row, row[2]] for row in rows]
        return pd.DataFrame(
            rows, columns=["group", "taxid", "reads_clade_total", "reads_clade_dedup"]
        )

    def test_family_shares_over_total_viral(self) -> None:
        # Total shares change, while deduplicated shares show that the underlying
        # family breadth is stable.
        reference = self._clade(
            [
                ["G", dm.VIRUSES_TAXID, 200, 100],
                ["G", 200, 80, 20],
                ["G", 210, 40, 40],
            ]
        )
        candidate = self._clade(
            [
                ["G", dm.VIRUSES_TAXID, 200, 100],
                ["G", 200, 60, 20],
                ["G", 210, 40, 40],
            ]
        )
        out = dm.clade_rank_shares(
            reference,
            candidate,
            self.RANK,
            self.NAME,
            rank_levels=("family",),
        )
        fx = out[out.taxid == 200].set_index("count_type")
        assert fx.loc["reads_clade_total"].delta_pp == pytest.approx(-10.0)
        assert fx.loc["reads_clade_dedup"].delta_pp == pytest.approx(0.0)

    def test_unchanged_family_not_inflated_when_other_collapses(self) -> None:
        # The old within-rank bug: familyX's raw count is UNCHANGED, but familyY
        # collapses. With a within-rank denominator familyX's share would jump up
        # (a spurious positive delta). With the total-viral denominator it does
        # not move, and delta_reads is exactly 0.
        reference = self._clade(
            [
                ["G", dm.VIRUSES_TAXID, 200],
                ["G", 200, 50],
                ["G", 210, 100],
            ]
        )
        candidate = self._clade(
            [["G", dm.VIRUSES_TAXID, 200], ["G", 200, 50], ["G", 210, 10]]
        )
        out = dm.clade_rank_shares(
            reference,
            candidate,
            self.RANK,
            self.NAME,
            rank_levels=("family",),
        )
        fx = out[out.taxid == 200].iloc[0]
        assert fx.delta_reads == 0
        assert abs(fx.delta_pp) < 1e-9  # NOT a positive gainer
        fy = out[out.taxid == 210].iloc[0]
        assert fy.delta_reads == -90
        assert fy.delta_pp < 0  # the family that actually fell

    def test_family_dropped_in_candidate(self) -> None:
        reference = self._clade(
            [["G", dm.VIRUSES_TAXID, 100], ["G", 200, 50], ["G", 210, 50]]
        )
        candidate = self._clade([["G", dm.VIRUSES_TAXID, 100], ["G", 200, 50]])
        out = dm.clade_rank_shares(
            reference,
            candidate,
            self.RANK,
            self.NAME,
            rank_levels=("family",),
        )
        fy = out[out.taxid == 210].iloc[0]
        assert fy.share_candidate == 0.0
        assert fy.delta_pp == -50.0
        assert fy.delta_reads == -50
        # A family present on the reference but gone in the candidate reaches zero;
        # one still present does not.
        assert bool(fy.reaches_zero)
        assert not bool(out[out.taxid == 200].iloc[0].reaches_zero)

    def test_reaches_zero_requires_candidate_denominator(self) -> None:
        # Group G has no candidate clade rows at all (only group H does), so its
        # zero candidate count is "no candidate data", not a real drop -> must NOT
        # be flagged reaches_zero (else a one-sided clade file fabricates findings).
        reference = self._clade([["G", dm.VIRUSES_TAXID, 100], ["G", 200, 50]])
        candidate = self._clade([["H", dm.VIRUSES_TAXID, 100], ["H", 200, 50]])
        out = dm.clade_rank_shares(
            reference, candidate, self.RANK, self.NAME, rank_levels=("family",)
        )
        g_row = out[(out.taxid == 200) & (out.group == "G")].iloc[0]
        assert g_row.reads_candidate == 0
        assert not bool(g_row.reaches_zero)

    def test_missing_viruses_root_gives_nan_share(self) -> None:
        # No Viruses-root row -> denominator missing -> share NaN (surfaced, not 0).
        reference = self._clade([["G", 200, 50]])
        candidate = self._clade([["G", 200, 50]])
        out = dm.clade_rank_shares(
            reference,
            candidate,
            self.RANK,
            self.NAME,
            rank_levels=("family",),
        )
        fx = out[out.taxid == 200].iloc[0]
        assert pd.isna(fx.share_reference) and pd.isna(fx.share_candidate)
        # Raw counts are still reported.
        assert fx.reads_reference == 50 and fx.delta_reads == 0

    def test_name_falls_back_to_taxid(self) -> None:
        reference = self._clade([["G", dm.VIRUSES_TAXID, 100], ["G", 999, 50]])
        candidate = self._clade([["G", dm.VIRUSES_TAXID, 100], ["G", 999, 50]])
        out = dm.clade_rank_shares(
            reference,
            candidate,
            {**self.RANK, 999: "family"},
            self.NAME,  # 999 absent from name_map -> falls back to "999"
            rank_levels=("family",),
        )
        f999 = out[out.taxid == 999].iloc[0]
        assert f999["name"] == "999"
        assert abs(f999.share_reference - 0.5) < 1e-9


class TestValidationAgreement:
    def test_counts_and_rates(self) -> None:
        vh = pd.DataFrame(
            {
                "group": ["G", "G", "G", "G"],
                "validation_distance_aligner": [0, 2, None, 0],
            }
        )
        out = dm.validation_agreement(vh).iloc[0]
        assert out.n_reads == 4
        assert out.n_validated == 3
        assert abs(out.frac_validated - 0.75) < 1e-9
        # 2 of 3 validated reads agree (distance 0).
        assert abs(out.agreement_rate - 2 / 3) < 1e-9


class TestValidationAgreementByTaxon:
    def test_per_taxon_rates_and_distance(self) -> None:
        vh = pd.DataFrame(
            {
                "group": ["G", "G", "G", "G", "G"],
                "aligner_taxid_lca": [10, 10, 10, 20, 20],
                "validation_distance_aligner": [0, 2, None, 0, 0],
            }
        )
        out = dm.validation_agreement_by_taxon(vh).set_index("taxid")
        # taxon 10: 3 reads, 2 validated, 1 agrees (distance 0), mean distance 1
        # over all validated, but the disagreement-only mean is 2.
        assert out.loc[10].n_reads == 3
        assert out.loc[10].n_validated == 2
        assert abs(out.loc[10].agreement_rate - 0.5) < 1e-9
        assert abs(out.loc[10].mean_distance_disagree - 2.0) < 1e-9
        # taxon 20: both validated and agree, so no disagreement distance.
        assert abs(out.loc[20].agreement_rate - 1.0) < 1e-9
        assert pd.isna(out.loc[20].mean_distance_disagree)

    def test_disagreement_distance_not_diluted_by_agreements(self) -> None:
        # 9 agreements + 1 distance-10 disagreement: report the disagreement
        # distance itself, not a mean diluted by the agreements.
        vh = pd.DataFrame(
            {
                "group": ["G"] * 10,
                "aligner_taxid_lca": [10] * 10,
                "validation_distance_aligner": [0] * 9 + [10],
            }
        )
        row = dm.validation_agreement_by_taxon(vh).iloc[0]
        assert abs(row.agreement_rate - 0.9) < 1e-9
        assert abs(row.mean_distance_disagree - 10.0) < 1e-9

    def test_empty_input_returns_header_only(self) -> None:
        out = dm.validation_agreement_by_taxon(pd.DataFrame())
        assert out.empty
        assert list(out.columns) == [
            "group",
            "taxid",
            "n_reads",
            "n_validated",
            "agreement_rate",
            "mean_distance_disagree",
        ]


class TestVertebrateStatusFlips:
    def _ann(self, rows: list[list]) -> pd.DataFrame:
        return pd.DataFrame(
            rows,
            columns=["taxid", "taxid_species", "name", "infection_status_vertebrate"],
        )

    def test_gained_and_lost(self) -> None:
        old = self._ann([[1, 1, "a", "1"], [2, 2, "b", "0"], [3, 3, "c", "1"]])
        new = self._ann([[1, 1, "a", "1"], [2, 2, "b", "1"], [3, 3, "c", "0"]])
        out = dm.vertebrate_status_flips(old, new)
        changes = dict(zip(out.taxid, out.change, strict=True))
        assert changes[2] == "gained_vertebrate"
        assert changes[3] == "lost_vertebrate"
        assert 1 not in changes

    def test_added_and_removed_taxa_not_called_flips(self) -> None:
        # Taxon 4 is host-infecting but exists only in the candidate (new) DB; it
        # is an ADDED taxon, not a gain flip. Taxon 5 is host-infecting only in the
        # reference (old) DB; it was REMOVED, not a loss flip.
        old = self._ann([[1, 1, "a", "1"], [5, 5, "e", "1"]])
        new = self._ann([[1, 1, "a", "1"], [4, 4, "d", "1"]])
        out = dm.vertebrate_status_flips(old, new)
        changes = dict(zip(out.taxid, out.change, strict=True))
        assert changes[4] == "added_vertebrate"
        assert changes[5] == "removed_vertebrate"
        # A genuine flip (present both, status crossed) stays distinct.
        assert "gained_vertebrate" not in changes.values()
        assert "lost_vertebrate" not in changes.values()


class TestBuildFlags:
    def test_fixed_threshold_on_bray_curtis(self) -> None:
        bc = pd.DataFrame(
            {
                "group": ["A", "B"],
                "rank": ["S", "S"],
                "ribosomal": [False, False],
                "bray_curtis": [0.02, 0.40],
            }
        )
        flags = dm.build_flags({"kraken_bray_curtis": bc})
        assert len(flags) == 1
        assert "B" in flags.iloc[0].key
        assert flags.iloc[0].flag_type == "fixed"

    def test_below_threshold_not_flagged(self) -> None:
        # No cohort test: a group merely out of line with its siblings but under
        # the fixed threshold must NOT be flagged.
        status = pd.DataFrame(
            {
                "group": [f"G{i}" for i in range(6)],
                "scope": ["vertebrate"] * 6,
                "pct_lost": [0.1, 0.2, 0.1, 0.15, 0.1, 0.12],
                "pct_reassigned": [1.0, 1.1, 0.9, 1.0, 1.2, 9.0],
            }
        )
        flags = dm.build_flags({"viral_read_status": status})
        # 9.0 is below the viral_pct_reassigned default of 10; nothing flagged.
        assert flags.empty

    def test_fastqc_flag_worsening_flagged_improvement_not(self) -> None:
        # Only worsening FASTQC transitions (pass<warn<fail) are flagged; an
        # improvement changes the table but must not be flagged.
        qc_flags = pd.DataFrame(
            {
                "group": ["A", "B", "C"],
                "sample": ["A", "B", "C"],
                "stage": ["cleaned", "cleaned", "cleaned"],
                "check": ["per_base_sequence_quality"] * 3,
                "reference_flag": ["pass", "warn", "fail"],
                "candidate_flag": ["fail", "pass", "fail"],
            }
        )
        flags = dm.build_flags({"qc_flag_changes": qc_flags})
        # A: pass->fail flagged; B: warn->pass improvement not flagged; C: fail->fail
        # is not in a real change table but here is a no-op rank move, not flagged.
        assert len(flags) == 1
        assert "group=A" in flags.iloc[0].key
        assert flags.iloc[0].value == "pass->fail"


class TestVertebrateTaxidsDtype:
    def test_float_dtype_status_still_found(self) -> None:
        # An NA in the column makes pandas read it as float ("1.0"); a string
        # compare would silently match nothing. Numeric compare must still work.
        ann = pd.DataFrame(
            {
                "taxid": [10, 20, 30],
                "taxid_species": [10, 20, 30],
                "infection_status_vertebrate": [1, 0, None],  # NA -> float dtype
            }
        )
        assert ann["infection_status_vertebrate"].dtype == float
        assert dm.vertebrate_taxids(ann) == {10}


class TestJoinReadAssignmentsFixes:
    def _vh(self, rows: list[list]) -> pd.DataFrame:
        return pd.DataFrame(
            rows, columns=["group", "sample", "seq_id", "aligner_taxid_lca"]
        )

    def test_sample_in_key_prevents_cross_sample_collision(self) -> None:
        # Same seq_id in two samples of one group must not cartesian-join.
        reference = self._vh([["G", "S1", "r", 10], ["G", "S2", "r", 20]])
        candidate = self._vh([["G", "S1", "r", 10], ["G", "S2", "r", 20]])
        out = dm.join_read_assignments(reference, candidate)
        assert len(out) == 2
        assert (out.status == "same").all()

    def test_duplicate_key_raises(self) -> None:
        reference = self._vh([["G", "S", "r", 10], ["G", "S", "r", 20]])
        candidate = self._vh([["G", "S", "r", 10]])
        with pytest.raises(ValueError, match="Duplicate"):
            dm.join_read_assignments(reference, candidate)

    def test_na_taxid_on_shared_read_is_reassigned(self) -> None:
        # A shared read with a present taxid on one side and NA on the other must
        # be 'reassigned', never silently 'same' (NA != value is <NA> -> fillna).
        reference = self._vh([["G", "S", "r1", 10]])
        candidate = pd.DataFrame(
            [["G", "S", "r1", None]],
            columns=["group", "sample", "seq_id", "aligner_taxid_lca"],
        )
        out = dm.join_read_assignments(reference, candidate)
        assert out.iloc[0].status == "reassigned"


class TestReassignmentPairCounts:
    def test_non_top_severe_pair_still_present(self) -> None:
        joined = pd.DataFrame(
            {
                "group": ["G"] * 5,
                "seq_id": list("abcde"),
                "taxid_reference": [401, 401, 401, 401, 401],
                "taxid_candidate": [402, 402, 402, 402, 600],
                "status": ["reassigned"] * 5,
            }
        )
        out = dm.reassignment_pair_counts(joined, _synthetic_tree(), vert={401, 402})
        cross = out[out.bucket == dm.CROSS_ROOT]
        assert len(cross) == 2  # all + vertebrate scopes
        row = cross.iloc[0]
        assert row.group == "G"
        assert row.taxid_reference == 401
        assert row.taxid_candidate == 600
        assert row.n_reads == 1
        assert row.pair_frac == 0.2
        # The top pair is still present too (every pair is kept).
        assert {(401, 402), (401, 600)} <= set(
            zip(out.taxid_reference, out.taxid_candidate, strict=True)
        )
        assert set(out[out.taxid_candidate == 402].pair_frac) == {0.8}

    def test_empty_detail_emits_header_only(self) -> None:
        empty = pd.DataFrame(
            columns=["group", "seq_id", "taxid_reference", "taxid_candidate", "status"]
        )
        out = dm.reassignment_pair_counts(empty, _synthetic_tree(), vert=set())
        assert out.empty
        assert list(out.columns) == [
            "group",
            "scope",
            "taxid_reference",
            "taxid_candidate",
            "bucket",
            "n_reads",
            "pair_frac",
            "is_severe",
            "is_dominant",
        ]

    def test_is_severe_and_is_dominant(self) -> None:
        # Two reads to a same-genus sibling (the dominant pair) and one cross-root
        # read (severe but not dominant).
        joined = pd.DataFrame(
            {
                "group": ["G"] * 3,
                "seq_id": list("abc"),
                "taxid_reference": [401, 401, 401],
                "taxid_candidate": [402, 402, 600],
                "status": ["reassigned"] * 3,
            }
        )
        out = dm.reassignment_pair_counts(joined, _synthetic_tree(), vert={401, 402})
        all_scope = out[out.scope == "all"].set_index("taxid_candidate")
        # The cross-root pair is severe; the same-genus pair is not.
        assert bool(all_scope.loc[600].is_severe)
        assert not bool(all_scope.loc[402].is_severe)
        # The larger pair (2 reads) is the single dominant pair for the group/scope.
        assert bool(all_scope.loc[402].is_dominant)
        assert not bool(all_scope.loc[600].is_dominant)


class TestQcReadSurvival:
    def test_survival_fraction_and_delta(self) -> None:
        def rows(cleaned: int) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    _qc_row("G", "S", "raw", "illumina", n_reads_single=1000),
                    _qc_row(
                        "G",
                        "S",
                        "cleaned",
                        "illumina",
                        n_reads_single=cleaned,
                    ),
                ]
            )

        reference = rows(900)  # 90% survival
        candidate = rows(800)  # 80% survival
        out = dm.qc_read_survival(reference, candidate).iloc[0]
        assert abs(out.survival_reference - 0.9) < 1e-9
        assert abs(out.survival_candidate - 0.8) < 1e-9
        assert abs(out.delta_pp - (-10.0)) < 1e-9


class TestNaTaxidRobustness:
    def _tree(self) -> dm.TaxonomyTree:
        return _synthetic_tree()

    def test_na_taxid_reassigned_does_not_crash(self) -> None:
        # A reassigned read with a missing taxid (non-conformant input) must route
        # to unresolved-taxid, not crash the pipeline.
        joined = pd.DataFrame(
            {
                "group": ["G", "G"],
                "seq_id": ["r1", "r2"],
                "taxid_reference": pd.array([401, pd.NA], dtype="Int64"),
                "taxid_candidate": pd.array([402, 500], dtype="Int64"),
                "status": ["reassigned", "reassigned"],
            }
        )
        pairs = dm.reassignment_pair_counts(joined, self._tree(), vert={401, 402, 500})
        r2 = pairs[(pairs.scope == "all") & pairs.taxid_reference.isna()].iloc[0]
        assert r2.bucket == dm.UNRESOLVED_TAXID
        assert r2.n_reads == 1


class TestQcSurvivalPlatformCoalesce:
    def test_candidate_only_sample_keeps_platform(self) -> None:
        reference = pd.DataFrame(
            [
                _qc_row("G", "S1", "raw", "illumina", n_reads_single=1000),
                _qc_row("G", "S1", "cleaned", "illumina", n_reads_single=900),
            ]
        )
        # S2 exists only on the candidate side.
        candidate = pd.DataFrame(
            [
                _qc_row("G", "S1", "raw", "illumina", n_reads_single=1000),
                _qc_row("G", "S1", "cleaned", "illumina", n_reads_single=900),
                _qc_row("G", "S2", "raw", "illumina", n_reads_single=1000),
                _qc_row("G", "S2", "cleaned", "illumina", n_reads_single=950),
            ]
        )
        out = dm.qc_read_survival(reference, candidate)
        s2 = out[out["sample"] == "S2"].iloc[0]
        assert s2.platform == "illumina"
        assert pd.isna(s2.survival_reference)
        assert abs(s2.survival_candidate - 0.95) < 1e-9


class TestBuildFlagsBranches:
    def test_numeric_branches(self) -> None:
        survival = pd.DataFrame(
            {
                "group": ["G"],
                "sample": ["S"],
                "platform": ["illumina"],
                "delta_pp": [-12.0],  # > read_survival_pp default 5
            }
        )
        clade = pd.DataFrame(
            {
                "group": ["G", "G"],
                "rank_level": ["family", "family"],
                "count_type": ["reads_clade_total", "reads_clade_dedup"],
                "name": ["FamX", "FamX"],
                "delta_pp": [-20.0, -30.0],
            }
        )
        val = pd.DataFrame(
            {
                "group": ["Gdrop", "Gimprove"],
                "agreement_rate_reference": [0.9, 0.5],
                "agreement_rate_candidate": [0.7, 0.9],  # drop +0.2 ; drop -0.4
            }
        )
        flags = dm.build_flags(
            {
                "qc_survival": survival,
                "clade_rank_shares": clade,
                "viral_validation_agreement": val,
            }
        )
        assert (flags.metric.str.contains("survival")).any()
        assert sum(flags.metric.str.contains("clade")) == 1
        keys = list(flags[flags.metric.str.contains("agreement")].key)
        assert any("Gdrop" in k for k in keys)
        assert not any("Gimprove" in k for k in keys)


class TestKrakenTopMoversCutoff:
    def test_tie_at_cutoff_broken_by_taxid(self) -> None:
        # Two taxa tie on abs_diff at the n=1 boundary; lower taxid wins (stable).
        reference = pd.DataFrame(
            [_kr("G", False, "S", 5, "hi", 90), _kr("G", False, "S", 9, "lo", 10)]
        )
        candidate = pd.DataFrame(
            [_kr("G", False, "S", 5, "hi", 10), _kr("G", False, "S", 9, "lo", 90)]
        )
        out = dm.kraken_top_movers(reference, candidate, "S", n=1)
        assert out.iloc[0].taxid == 5  # both move 80pp; lower taxid kept


class TestMarkAgreementDrivers:
    def _by_taxon(self) -> pd.DataFrame:
        # taxon 28875: big move on many reads; taxon 99: a 1-read full flip.
        return pd.DataFrame(
            {
                "group": ["G", "G"],
                "taxid": [28875, 99],
                "n_validated_reference": [355, 1],
                "n_validated_candidate": [487, 1],
                "agreement_rate_reference": [0.73, 1.0],
                "agreement_rate_candidate": [0.06, 0.0],
                "delta_agreement": [-0.67, -1.0],
            }
        )

    def test_driver_is_high_impact_not_one_read_flip(self) -> None:
        out = dm.mark_agreement_drivers(self._by_taxon()).set_index("taxid")
        # The 1-read flip has the larger |delta_agreement| but the move on hundreds
        # of reads contributes more to the group-level agreement drop.
        assert bool(out.loc[28875].is_agreement_driver)
        assert not bool(out.loc[99].is_agreement_driver)
        assert (
            out.loc[28875].agreement_drop_contribution
            > out.loc[99].agreement_drop_contribution
        )

    def test_one_sided_taxon_can_be_driver(self) -> None:
        # A taxon validated only on the reference side (gone in the candidate)
        # contributes its full reference agreement to the group's drop; the prior
        # within-taxon delta ranking (null delta) could never select it.
        by_taxon = pd.DataFrame(
            {
                "group": ["G", "G"],
                "taxid": [10, 20],
                "n_validated_reference": [100, 5],
                "n_validated_candidate": [0, 5],
                "agreement_rate_reference": [0.9, 0.5],
                "agreement_rate_candidate": [None, 0.5],
                "delta_agreement": [None, 0.0],
            }
        )
        out = dm.mark_agreement_drivers(by_taxon).set_index("taxid")
        assert bool(out.loc[10].is_agreement_driver)
        assert not bool(out.loc[20].is_agreement_driver)

    def test_empty_input_gets_driver_columns(self) -> None:
        out = dm.mark_agreement_drivers(pd.DataFrame())
        for col in (
            "abs_delta_agreement",
            "agreement_drop_contribution",
            "is_agreement_driver",
        ):
            assert col in out.columns


class TestValidationAgreementDecomposition:
    def _vh(self, rows: list[list]) -> pd.DataFrame:
        return pd.DataFrame(
            rows,
            columns=[
                "group",
                "sample",
                "seq_id",
                "aligner_taxid_lca",
                "validation_distance_aligner",
                "validation_staxid_lca",
            ],
        )

    def test_four_way_loss_split(self) -> None:
        # r1 target-only (rename 28875->3432193); r2 aligner-only (10941->28875);
        # r3 both taxids moved (ambiguous); r4 neither taxid moved (distance flip);
        # r5 agrees on both sides (not a loss).
        reference = self._vh(
            [
                ["G", "s", "r1", 28875, 0, 28875],
                ["G", "s", "r2", 10941, 0, 10941],
                ["G", "s", "r3", 600, 0, 700],
                ["G", "s", "r4", 800, 0, 800],
                ["G", "s", "r5", 500, 0, 500],
            ]
        )
        candidate = self._vh(
            [
                ["G", "s", "r1", 28875, 1, 3432193],
                ["G", "s", "r2", 28875, 1, 10941],
                ["G", "s", "r3", 601, 1, 701],
                ["G", "s", "r4", 800, 1, 800],
                ["G", "s", "r5", 500, 0, 500],
            ]
        )
        out = dm.validation_agreement_decomposition(
            reference, candidate, name_map={3432193: "Rotavirus alphagastroenteritidis"}
        ).iloc[0]
        assert out.n_validated_both == 5
        assert out.n_agreement_lost == 4
        assert out.n_lost_target_only == 1
        assert out.n_lost_aligner_only == 1
        assert out.n_lost_both_changed == 1
        assert out.n_lost_neither_changed == 1
        # The dominant target-only shift is the rename pair.
        assert out.dominant_target_shift_taxid_reference == 28875
        assert out.dominant_target_shift_taxid_candidate == 3432193
        assert (
            out.dominant_target_shift_name_candidate
            == "Rotavirus alphagastroenteritidis"
        )
        assert out.dominant_target_shift_reads == 1

    def test_one_sided_validated_reads_are_residual_not_dropped(self) -> None:
        # The group's per-side rate can change purely from reads validated on one
        # side. The group must still get a row, with those reads counted as the
        # residual rather than silently excluded.
        reference = self._vh(
            [
                ["G", "s", "r1", 10, 0, 10],  # validated reference-only
                ["G", "s", "r2", 20, 0, 20],  # validated both, stays agreeing
            ]
        )
        candidate = self._vh(
            [
                ["G", "s", "r1", 10, None, 10],  # not validated on candidate
                ["G", "s", "r2", 20, 0, 20],
            ]
        )
        out = dm.validation_agreement_decomposition(reference, candidate).iloc[0]
        assert out.n_agreement_lost == 0  # no shared read flipped
        assert out.n_validated_reference_only == 1  # the residual is surfaced
        assert out.n_validated_both == 1

    def test_schema_bearing_empty_side_still_yields_residual(self) -> None:
        # The pipeline emits header-only validation_hits for groups with no reads.
        # A 0-row-but-columned reference frame must NOT collapse the table: the
        # candidate's validated reads must still surface as the candidate-only
        # residual (the row-per-group contract).
        empty_ref = self._vh([]).iloc[0:0]
        candidate = self._vh([["G", "s", "r1", 10, 0, 10]])
        out = dm.validation_agreement_decomposition(empty_ref, candidate)
        assert len(out) == 1
        row = out.iloc[0]
        assert row.n_validated_candidate_only == 1
        assert row.n_validated_both == 0
        assert row.n_agreement_lost == 0

    def test_missing_columns_returns_header_only(self) -> None:
        # A frame with no columns at all (not just no rows) cannot be compared.
        out = dm.validation_agreement_decomposition(pd.DataFrame(), pd.DataFrame())
        assert out.empty
        assert "n_lost_target_only" in out.columns


class TestBoundingNumbers:
    def test_survival_max_and_no_flag(self) -> None:
        survival = pd.DataFrame(
            {
                "group": ["A", "B"],
                "sample": ["A", "B"],
                "platform": ["illumina", "illumina"],
                "delta_pp": [0.02, -0.044],
            }
        )
        out = dm.bounding_numbers({"qc_survival": survival})
        row = out[out.metric == "QC read survival (pp)"].iloc[0]
        # The bounding number is the largest |deviation|, reported with where it is.
        assert row.max_abs_value == pytest.approx(0.044)
        assert "B" in row.max_abs_group
        assert row.n_flagged == 0  # default threshold is 5.0 pp

    def test_kraken_split_by_rank_and_ribosomal(self) -> None:
        bc = pd.DataFrame(
            {
                "group": ["A", "B", "C"],
                "rank": ["S", "S", "G"],
                "ribosomal": [False, False, False],
                "bray_curtis": [0.18, 0.05, 0.10],
            }
        )
        out = dm.bounding_numbers({"kraken_bray_curtis": bc})
        s = out[out.subset == "rank=S, ribosomal=False"].iloc[0]
        assert s.max_abs_value == pytest.approx(0.18)
        assert s.n_flagged == 1  # only 0.18 exceeds the 0.15 default

    def test_blast_improvement_not_counted_or_reported_as_drop(self) -> None:
        # The agreement metric is one-directional (a drop). Group B improves by
        # MORE than group A drops, so an abs-based bound would wrongly report B's
        # improvement as the largest "drop". The bound must describe the drop side.
        val = pd.DataFrame(
            {
                "group": ["A", "B"],
                "agreement_rate_reference": [0.9, 0.30],
                "agreement_rate_candidate": [0.4, 0.95],  # A drops 0.5; B improves 0.65
            }
        )
        out = dm.bounding_numbers({"viral_validation_agreement": val})
        row = out[out.metric == "BLAST-agreement rate drop"].iloc[0]
        assert row.n_flagged == 1  # only group A's drop
        assert row.max_abs_value == pytest.approx(0.5)  # A's drop, not B's 0.65 gain
        assert "A" in row.max_abs_group and "B" not in row.max_abs_group

    def test_not_computed_dimensions_emit_null_rows(self) -> None:
        # With no viral/clade/kraken sources, every dimension still gets a row so
        # the Checked section can say "not computed" (null bound) rather than omit.
        out = dm.bounding_numbers({})
        metrics = set(out.metric)
        for expected in (
            "vertebrate-viral reads reassigned (%)",
            "clade share change (pp)",
            "BLAST-agreement rate drop",
            "Kraken Bray-Curtis",
        ):
            assert expected in metrics
        reassigned = out[out.metric == "vertebrate-viral reads reassigned (%)"].iloc[0]
        assert pd.isna(reassigned.max_abs_value)  # not computed
        assert reassigned.n_flagged == 0


class TestBuildFindings:
    def _status(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "group": ["G"],
                "scope": ["vertebrate"],
                "pct_lost": [0.0],
                "pct_gained": [50.0],
                "pct_reassigned": [0.0],
                "dominant_gained_taxid": [3430604],
                "dominant_gained_name": ["Mahrahovirus faecivivens"],
                "dominant_lost_taxid": [None],
                "dominant_lost_name": [""],
            }
        )

    def test_gained_threshold_finding_carries_named_driver(self) -> None:
        out = dm.build_findings({"viral_read_status": self._status()})
        gained = out[out.finding_type == "viral_reads_gained"].iloc[0]
        assert gained.trigger == "threshold"
        assert gained.entity_taxid == 3430604
        assert gained.entity_name == "Mahrahovirus faecivivens"
        assert "viral_read_status.tsv?group=G" in gained.detail_source

    def test_clade_reaches_zero_below_threshold_is_enumerated(self) -> None:
        # A clade with a tiny (sub-threshold) drop that nonetheless reaches zero
        # candidate reads must still produce a finding row.
        clade = pd.DataFrame(
            {
                "group": ["G"],
                "rank_level": ["family"],
                "count_type": ["reads_clade_total"],
                "taxid": [2169574],
                "name": ["Smacoviridae"],
                "delta_pp": [-0.1],
                "reaches_zero": [True],
            }
        )
        out = dm.build_findings({"clade_rank_shares": clade})
        row = out[out.finding_type == "clade_reaches_zero"].iloc[0]
        assert row.trigger == "reaches_zero"
        assert row.entity_name == "Smacoviridae"
        assert row.entity_taxid == 2169574

    def test_severe_reassignment_named_from_map(self) -> None:
        pairs = pd.DataFrame(
            {
                "group": ["G"],
                "scope": ["all"],
                "taxid_reference": [2805939],
                "taxid_candidate": [3428315],
                "bucket": [dm.SHARED_HIGHER],
                "n_reads": [8],
                "pair_frac": [1.0],
                "is_severe": [True],
                "is_dominant": [True],
            }
        )
        out = dm.build_findings(
            {"viral_reassignment_pairs": pairs},
            name_map={3428315: "Dolmedivirus noldo"},
        )
        row = out[out.finding_type == "severe_reassignment"].iloc[0]
        assert row.entity_name == "Dolmedivirus noldo"
        assert "2805939->3428315" in row.metric

    def test_rank_in_type_orders_by_magnitude(self) -> None:
        clade = pd.DataFrame(
            {
                "group": ["G1", "G2"],
                "rank_level": ["family", "family"],
                "count_type": ["reads_clade_total", "reads_clade_total"],
                "taxid": [200, 210],
                "name": ["famX", "famY"],
                "delta_pp": [-4.0, -9.0],
                "reaches_zero": [False, False],
            }
        )
        out = dm.build_findings({"clade_rank_shares": clade})
        shift = out[out.finding_type == "clade_share_shift"].set_index("rank_in_type")
        # The larger |Δpp| (famY, -9) is rank 1.
        assert shift.loc[1].entity_name == "famY"
        assert shift.loc[2].entity_name == "famX"

    def test_qc_threshold_finding_enters_manifest(self) -> None:
        # A QC survival regression that build_flags would flag must also appear in
        # findings.tsv (the manifest the report author works from).
        survival = pd.DataFrame(
            {
                "group": ["G"],
                "sample": ["G"],
                "platform": ["illumina"],
                "delta_pp": [-8.0],  # exceeds the 5.0 pp default
            }
        )
        out = dm.build_findings({"qc_survival": survival})
        qc = out[out.finding_type == "qc_anomaly"]
        assert len(qc) == 1
        assert qc.iloc[0].direction == "down"

    def test_expected_output_missing_on_both_sides_is_flagged(self) -> None:
        inventory = pd.DataFrame(
            {
                "group": ["G", "G"],
                "file_type": ["validation_hits", "bracken"],
                "in_reference": [True, False],
                "in_candidate": [True, False],
            }
        )
        out = dm.build_findings({}, inventory=inventory)
        anomalies = out[out.finding_type == "output_anomaly"]
        # validation_hits is on both sides (fine); bracken is on neither (flagged).
        assert list(anomalies.metric.str.contains("bracken")) == [True]
        assert anomalies.iloc[0].trigger == "missing_both_sides"

    def test_one_sided_empty_file_flagged_both_sided_not(self) -> None:
        columns = pd.DataFrame(
            {
                "file_type": ["bracken", "kraken"],
                "missing_vs_schema_reference": ["(empty file)", "(empty file)"],
                "missing_vs_schema_candidate": ["(empty file)", ""],
                "extra_vs_schema_reference": ["", ""],
                "extra_vs_schema_candidate": ["", ""],
                "groups_consistent_reference": [True, True],
                "groups_consistent_candidate": [True, True],
            }
        )
        out = dm.build_findings({}, columns=columns)
        schema = out[out.finding_type == "schema_anomaly"]
        # bracken empty on both sides is benign; kraken empty on reference only is
        # an anomaly.
        assert list(schema.metric) == ["kraken: empty on reference only"]

    def test_platform_mismatch_flagged_once_per_group(self) -> None:
        # Same file types present on both sides, but the group is a platform
        # mismatch -> must still surface (once), not vanish because presence matches.
        inventory = pd.DataFrame(
            {
                "group": ["G", "G"],
                "platform": ["illumina/ont (mismatch)", "illumina/ont (mismatch)"],
                "file_type": ["kraken", "read_counts"],
                "in_reference": [True, True],
                "in_candidate": [True, True],
                "n_rows_reference": [5, 5],
                "n_rows_candidate": [5, 5],
            }
        )
        out = dm.build_findings({}, inventory=inventory)
        mism = out[out.trigger == "platform_mismatch"]
        assert len(mism) == 1
        assert mism.iloc[0].group == "G"

    def test_row_count_collapse_to_zero_is_flagged(self) -> None:
        # A file present on both sides but collapsing to zero rows on one side is
        # an unexpectedly empty output; a mere nonzero delta is not flagged here.
        inventory = pd.DataFrame(
            {
                "group": ["G", "G"],
                "platform": ["illumina", "illumina"],
                "file_type": ["validation_hits", "kraken"],
                "in_reference": [True, True],
                "in_candidate": [True, True],
                "n_rows_reference": [100, 80],
                "n_rows_candidate": [0, 120],  # validation_hits collapses; kraken grows
            }
        )
        out = dm.build_findings({}, inventory=inventory)
        collapses = out[out.trigger == "row_count_collapse"]
        assert list(collapses.metric.str.contains("validation_hits")) == [True]
        assert "candidate only" in collapses.iloc[0].metric
