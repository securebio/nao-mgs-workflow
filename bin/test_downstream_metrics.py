"""Unit tests for downstream_metrics.py (pure calculation functions).

Tests use small synthetic manifests/DataFrames only -- never real delivery data.
Test order mirrors the order of functions in downstream_metrics.py.
"""

import downstream_metrics as dm
import pandas as pd
import pytest


def _entry(
    present: bool = True,
    n_rows: int | None = None,
    columns: list[str] | None = None,
) -> dm.FileEntry:
    return dm.FileEntry(present=present, n_rows=n_rows, columns=columns)


def _manifest(spec: dict[str, tuple[str, dict[str, dm.FileEntry]]]) -> dm.SideManifest:
    """Build a SideManifest from {group: (platform, {file_type: FileEntry})}."""
    return {
        g: dm.GroupManifest(platform=plat, files=files)
        for g, (plat, files) in spec.items()
    }


#################################
# FOCUS 4: compare_file_inventory #
#################################


class TestCompareFileInventory:
    def test_row_delta_and_pct(self) -> None:
        reference = _manifest(
            {"G1": ("illumina", {"validation_hits": _entry(n_rows=100, columns=["a"])})}
        )
        candidate = _manifest(
            {"G1": ("illumina", {"validation_hits": _entry(n_rows=150, columns=["a"])})}
        )
        df = dm.compare_file_inventory(reference, candidate)
        row = df.iloc[0]
        assert row.row_delta == 50
        assert row.row_pct_change == 50.0
        assert row.in_reference and row.in_candidate

    def test_presence_mismatch_one_side(self) -> None:
        reference = _manifest(
            {"G1": ("ont", {"kraken": _entry(n_rows=10, columns=["t"])})}
        )
        candidate = _manifest(
            {
                "G1": ("ont", {"kraken": _entry(n_rows=10, columns=["t"])}),
                "G2": ("ont", {"kraken": _entry(n_rows=5, columns=["t"])}),
            }
        )
        df = dm.compare_file_inventory(reference, candidate)
        g2 = df[df.group == "G2"].iloc[0]
        assert not g2.in_reference
        assert g2.in_candidate
        # No row count on the absent side -> delta is null, not a spurious number.
        assert pd.isna(g2.row_delta)

    def test_zero_rows_reference_gives_null_pct_not_divide_by_zero(self) -> None:
        reference = _manifest(
            {"G1": ("illumina", {"bracken": _entry(n_rows=0, columns=[])})}
        )
        candidate = _manifest(
            {"G1": ("illumina", {"bracken": _entry(n_rows=3, columns=[])})}
        )
        df = dm.compare_file_inventory(reference, candidate)
        row = df.iloc[0]
        assert row.row_delta == 3
        assert pd.isna(row.row_pct_change)

    def test_json_file_has_null_rows(self) -> None:
        reference = _manifest({"G1": ("illumina", {"fastp": _entry(n_rows=None)})})
        candidate = _manifest({"G1": ("illumina", {"fastp": _entry(n_rows=None)})})
        df = dm.compare_file_inventory(reference, candidate)
        row = df.iloc[0]
        assert row.in_reference and row.in_candidate
        assert pd.isna(row.n_rows_reference) and pd.isna(row.row_delta)

    def test_platform_mismatch_unions_expected_types(self) -> None:
        # Illumina on reference, ONT on candidate (degraded): report the mismatch and still
        # surface Illumina-only expected types missing on both sides.
        reference = _manifest(
            {"G": ("illumina", {"clade_counts": _entry(n_rows=1, columns=["t"])})}
        )
        candidate = _manifest(
            {"G": ("ont", {"kraken": _entry(n_rows=1, columns=["t"])})}
        )
        expected = {
            "illumina": {"clade_counts", "kraken", "bracken"},
            "ont": {"kraken"},
        }
        df = dm.compare_file_inventory(reference, candidate, expected)
        assert "mismatch" in df.iloc[0].platform
        # bracken is illumina-expected and absent on both sides -> still a row.
        assert (df.file_type == "bracken").any()


#####################################
# FOCUS 4: compare_columns_to_schema #
#####################################


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
        man = _manifest({"G1": ("illumina", {"bracken": _entry(n_rows=0, columns=[])})})
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

    def test_groups_inconsistent_columns_within_side_flagged(self) -> None:
        # Two groups on the reference side disagree on kraken columns; the first-group
        # header alone would hide it, so groups_consistent_reference must be False.
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

    def test_later_group_missing_column_is_aggregated(self) -> None:
        # First group conforms, a LATER group drops a required column: the missing
        # field must still be reported (aggregated across all group headers).
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
        assert row.missing_vs_schema_reference == "name"


###############################
# FOCUS 3: QUALITY METRICS    #
###############################


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


###############################
# FOCUS 2: KRAKEN ABUNDANCES  #
###############################


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


###############################
# FOCUS 1: VIRAL ASSIGNMENTS  #
###############################


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
        detail = dm.reassignment_distances(joined, tax, vert={401, 402})
        buckets = dm.bucket_summary(detail)
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
        return pd.DataFrame(
            rows, columns=["group", "taxid", "reads_clade_total", "reads_clade_dedup"]
        )

    def test_family_shares_over_total_viral(self) -> None:
        # Total viral (Viruses root) = 200 reads on each side. familyX 80 -> 60.
        reference = self._clade(
            [["G", dm.VIRUSES_TAXID, 200, 200], ["G", 200, 80, 80], ["G", 210, 40, 40]]
        )
        candidate = self._clade(
            [["G", dm.VIRUSES_TAXID, 200, 200], ["G", 200, 60, 60], ["G", 210, 40, 40]]
        )
        out = dm.clade_rank_shares(
            reference,
            candidate,
            self.RANK,
            self.NAME,
            rank_levels=("family",),
            count_cols=("reads_clade_total",),
        )
        fx = out[out.taxid == 200].iloc[0]
        # Share is of TOTAL viral reads (200), not of the family-rank sum.
        assert abs(fx.share_reference - 0.40) < 1e-9
        assert abs(fx.share_candidate - 0.30) < 1e-9
        assert abs(fx.delta_pp - (-10.0)) < 1e-9
        assert fx.delta_reads == -20

    def test_unchanged_family_not_inflated_when_other_collapses(self) -> None:
        # The old within-rank bug: familyX's raw count is UNCHANGED, but familyY
        # collapses. With a within-rank denominator familyX's share would jump up
        # (a spurious positive delta). With the total-viral denominator it does
        # not move, and delta_reads is exactly 0.
        reference = self._clade(
            [
                ["G", dm.VIRUSES_TAXID, 200, 200],
                ["G", 200, 50, 50],
                ["G", 210, 100, 100],
            ]
        )
        candidate = self._clade(
            [["G", dm.VIRUSES_TAXID, 200, 200], ["G", 200, 50, 50], ["G", 210, 10, 10]]
        )
        out = dm.clade_rank_shares(
            reference,
            candidate,
            self.RANK,
            self.NAME,
            rank_levels=("family",),
            count_cols=("reads_clade_total",),
        )
        fx = out[out.taxid == 200].iloc[0]
        assert fx.delta_reads == 0
        assert abs(fx.delta_pp) < 1e-9  # NOT a positive gainer
        fy = out[out.taxid == 210].iloc[0]
        assert fy.delta_reads == -90
        assert fy.delta_pp < 0  # the family that actually fell

    def test_family_dropped_in_candidate(self) -> None:
        reference = self._clade(
            [["G", dm.VIRUSES_TAXID, 100, 100], ["G", 200, 50, 50], ["G", 210, 50, 50]]
        )
        candidate = self._clade([["G", dm.VIRUSES_TAXID, 100, 100], ["G", 200, 50, 50]])
        out = dm.clade_rank_shares(
            reference,
            candidate,
            self.RANK,
            self.NAME,
            rank_levels=("family",),
            count_cols=("reads_clade_total",),
        )
        fy = out[out.taxid == 210].iloc[0]
        assert fy.share_candidate == 0.0
        assert fy.delta_pp == -50.0
        assert fy.delta_reads == -50

    def test_missing_viruses_root_gives_nan_share(self) -> None:
        # No Viruses-root row -> denominator missing -> share NaN (surfaced, not 0).
        reference = self._clade([["G", 200, 50, 50]])
        candidate = self._clade([["G", 200, 50, 50]])
        out = dm.clade_rank_shares(
            reference,
            candidate,
            self.RANK,
            self.NAME,
            rank_levels=("family",),
            count_cols=("reads_clade_total",),
        )
        fx = out[out.taxid == 200].iloc[0]
        assert pd.isna(fx.share_reference) and pd.isna(fx.share_candidate)
        # Raw counts are still reported.
        assert fx.reads_reference == 50 and fx.delta_reads == 0

    def test_name_falls_back_to_taxid(self) -> None:
        reference = self._clade([["G", dm.VIRUSES_TAXID, 100, 100], ["G", 999, 50, 50]])
        candidate = self._clade([["G", dm.VIRUSES_TAXID, 100, 100], ["G", 999, 50, 50]])
        out = dm.clade_rank_shares(
            reference,
            candidate,
            {**self.RANK, 999: "family"},
            self.NAME,  # 999 absent from name_map -> falls back to "999"
            rank_levels=("family",),
            count_cols=("reads_clade_total",),
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


###############################
# FLAGGING                    #
###############################


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


###############################
# SCIENTIFIC-REVIEW FIXES     #
###############################


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

    def test_merge_map_canonicalizes_so_not_reassigned(self) -> None:
        # reference taxid 100 was merged into 200 in candidate; with the merge map the read
        # is 'same', not 'reassigned'.
        reference = self._vh([["G", "S", "r1", 100]])
        candidate = self._vh([["G", "S", "r1", 200]])
        out = dm.join_read_assignments(reference, candidate, merge_map={100: 200})
        assert out.iloc[0].status == "same"
        out_nomap = dm.join_read_assignments(reference, candidate)
        assert out_nomap.iloc[0].status == "reassigned"

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


class TestReassignmentConcentration:
    def test_top_pair_fraction(self) -> None:
        detail = pd.DataFrame(
            {
                "group": ["G"] * 5,
                "scope": ["all"] * 5,
                "seq_id": list("abcde"),
                "taxid_reference": [1, 1, 1, 1, 2],
                "taxid_candidate": [9, 9, 9, 9, 8],
                "bucket": ["same-genus"] * 5,
            }
        )
        out = dm.reassignment_concentration(detail).iloc[0]
        assert out.n_reassigned == 5
        assert out.n_distinct_pairs == 2
        assert out.top_pair == "1->9"
        assert abs(out.top_pair_frac - 0.8) < 1e-9


class TestReassignmentPairCounts:
    def test_non_top_severe_pair_still_present(self) -> None:
        # A severe cross-root pair (2->8) that is NOT the group's top pair (1->9,
        # 4 reads) must still appear as its own row, so the report can name it.
        detail = pd.DataFrame(
            {
                "group": ["G"] * 5,
                "scope": ["all"] * 5,
                "seq_id": list("abcde"),
                "taxid_reference": [1, 1, 1, 1, 2],
                "taxid_candidate": [9, 9, 9, 9, 8],
                "bucket": ["same-genus"] * 4 + [dm.CROSS_ROOT],
            }
        )
        out = dm.reassignment_pair_counts(detail)
        cross = out[out.bucket == dm.CROSS_ROOT]
        assert len(cross) == 1
        row = cross.iloc[0]
        assert row.group == "G"
        assert row.taxid_reference == 2
        assert row.taxid_candidate == 8
        assert row.n_reads == 1
        # The top pair is still present too (every pair is kept).
        assert {(1, 9), (2, 8)} <= set(
            zip(out.taxid_reference, out.taxid_candidate, strict=True)
        )

    def test_empty_detail_emits_header_only(self) -> None:
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
        out = dm.reassignment_pair_counts(empty)
        assert out.empty
        assert list(out.columns) == [
            "group",
            "scope",
            "taxid_reference",
            "taxid_candidate",
            "bucket",
            "n_reads",
        ]


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
        detail = dm.reassignment_distances(joined, self._tree(), vert={401, 402, 500})
        r2 = detail[(detail.scope == "all") & (detail.seq_id == "r2")].iloc[0]
        assert r2.bucket == dm.UNRESOLVED_TAXID
        # concentration must also survive the NA pair.
        conc = dm.reassignment_concentration(detail)
        assert not conc.empty
        assert (conc.n_reassigned > 0).all()


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


###############################
# TEST-QUALITY REVIEW: GAPS   #
###############################


class TestBuildFlagsBranches:
    def test_survival_branch_flags_large_pp_change(self) -> None:
        survival = pd.DataFrame(
            {
                "group": ["G"],
                "sample": ["S"],
                "platform": ["illumina"],
                "delta_pp": [-12.0],  # > read_survival_pp default 5
            }
        )
        flags = dm.build_flags({"qc_survival": survival})
        assert (flags.metric.str.contains("survival")).any()

    def test_clade_branch_flags_large_share_shift(self) -> None:
        clade = pd.DataFrame(
            {
                "group": ["G"],
                "rank_level": ["family"],
                "name": ["FamX"],
                "count_type": ["reads_clade_total"],
                "delta_pp": [-20.0],  # > clade_share_pp default 3
            }
        )
        flags = dm.build_flags({"clade_rank_shares": clade})
        assert (flags.metric.str.contains("clade")).any()

    def test_agreement_drop_flagged_but_improvement_not(self) -> None:
        # pos metric: a DROP over threshold flags; an equal-size improvement
        # (negative drop) must NOT flag (guards the direction=="pos" path).
        val = pd.DataFrame(
            {
                "group": ["Gdrop", "Gimprove"],
                "agreement_rate_reference": [0.9, 0.5],
                "agreement_rate_candidate": [0.7, 0.9],  # drop +0.2 ; drop -0.4
            }
        )
        flags = dm.build_flags({"viral_validation_agreement": val})
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
