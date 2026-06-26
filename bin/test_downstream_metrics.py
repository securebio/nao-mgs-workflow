"""Unit tests for downstream_metrics.py (pure calculation functions).

Tests use small synthetic manifests/DataFrames only -- never real delivery data.
Test order mirrors the order of functions in downstream_metrics.py.
"""

import downstream_metrics as dm
import pandas as pd


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
        main = _manifest(
            {"G1": ("illumina", {"validation_hits": _entry(n_rows=100, columns=["a"])})}
        )
        dev = _manifest(
            {"G1": ("illumina", {"validation_hits": _entry(n_rows=150, columns=["a"])})}
        )
        df = dm.compare_file_inventory(main, dev)
        row = df.iloc[0]
        assert row.row_delta == 50
        assert row.row_pct_change == 50.0
        assert row.in_main and row.in_dev

    def test_presence_mismatch_one_side(self) -> None:
        main = _manifest({"G1": ("ont", {"kraken": _entry(n_rows=10, columns=["t"])})})
        dev = _manifest(
            {
                "G1": ("ont", {"kraken": _entry(n_rows=10, columns=["t"])}),
                "G2": ("ont", {"kraken": _entry(n_rows=5, columns=["t"])}),
            }
        )
        df = dm.compare_file_inventory(main, dev)
        g2 = df[df.group == "G2"].iloc[0]
        assert not g2.in_main
        assert g2.in_dev
        # No row count on the absent side -> delta is null, not a spurious number.
        assert pd.isna(g2.row_delta)

    def test_zero_rows_main_gives_null_pct_not_divide_by_zero(self) -> None:
        main = _manifest(
            {"G1": ("illumina", {"bracken": _entry(n_rows=0, columns=[])})}
        )
        dev = _manifest({"G1": ("illumina", {"bracken": _entry(n_rows=3, columns=[])})})
        df = dm.compare_file_inventory(main, dev)
        row = df.iloc[0]
        assert row.row_delta == 3
        assert pd.isna(row.row_pct_change)

    def test_json_file_has_null_rows(self) -> None:
        main = _manifest({"G1": ("illumina", {"fastp": _entry(n_rows=None)})})
        dev = _manifest({"G1": ("illumina", {"fastp": _entry(n_rows=None)})})
        df = dm.compare_file_inventory(main, dev)
        row = df.iloc[0]
        assert row.in_main and row.in_dev
        assert pd.isna(row.n_rows_main) and pd.isna(row.row_delta)


#####################################
# FOCUS 4: compare_columns_to_schema #
#####################################


class TestCompareColumnsToSchema:
    def test_conformant_columns_report_clean(self) -> None:
        cols = ["seq_id", "group"]
        man = _manifest({"G1": ("illumina", {"validation_hits": _entry(columns=cols)})})
        df = dm.compare_columns_to_schema(man, man, {"validation_hits": cols})
        row = df.iloc[0]
        assert row.missing_vs_schema_main == ""
        assert row.extra_vs_schema_main == ""
        assert row.cols_only_in_main == ""
        assert not row.order_changed

    def test_empty_file_reports_empty_marker_not_full_schema(self) -> None:
        man = _manifest({"G1": ("illumina", {"bracken": _entry(n_rows=0, columns=[])})})
        df = dm.compare_columns_to_schema(
            man, man, {"bracken": ["taxid", "name", "fraction_total_reads"]}
        )
        row = df.iloc[0]
        assert row.missing_vs_schema_main == "(empty file)"
        assert row.missing_vs_schema_dev == "(empty file)"

    def test_column_added_in_dev_is_flagged(self) -> None:
        main = _manifest({"G1": ("illumina", {"kraken": _entry(columns=["taxid"])})})
        dev = _manifest(
            {"G1": ("illumina", {"kraken": _entry(columns=["taxid", "new_col"])})}
        )
        df = dm.compare_columns_to_schema(main, dev, {"kraken": ["taxid"]})
        row = df.iloc[0]
        assert row.cols_only_in_dev == "new_col"
        assert row.cols_only_in_main == ""
        assert row.extra_vs_schema_dev == "new_col"

    def test_reorder_only_sets_order_changed(self) -> None:
        main = _manifest({"G1": ("illumina", {"kraken": _entry(columns=["a", "b"])})})
        dev = _manifest({"G1": ("illumina", {"kraken": _entry(columns=["b", "a"])})})
        df = dm.compare_columns_to_schema(main, dev, {"kraken": ["a", "b"]})
        row = df.iloc[0]
        assert row.order_changed
        assert row.cols_only_in_main == "" and row.cols_only_in_dev == ""

    def test_file_type_without_schema_still_reported(self) -> None:
        man = _manifest({"G1": ("illumina", {"mystery": _entry(columns=["x"])})})
        df = dm.compare_columns_to_schema(man, man, {})
        row = df[df.file_type == "mystery"].iloc[0]
        # Without a schema we can't judge missing/extra; has_schema=False is the
        # signal that an output lacks a schema entirely.
        assert not row.has_schema
        assert row.extra_vs_schema_main == ""


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
        main = pd.DataFrame(
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
        dev = pd.DataFrame(
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
        out = dm.compare_qc_numeric(main, dev, metrics=("n_reads_single", "percent_gc"))
        reads = out[out.metric == "n_reads_single"].iloc[0]
        assert reads.delta == -100
        assert reads["pct_change"] == -10.0
        gc = out[out.metric == "percent_gc"].iloc[0]
        assert gc.delta == 1.0

    def test_na_metric_yields_na_delta(self) -> None:
        # ONT: n_read_pairs is NA on both sides.
        main = pd.DataFrame(
            [_qc_row("G1", "S1", "raw", "ont", n_read_pairs=None, n_reads_single=500)]
        )
        dev = pd.DataFrame(
            [_qc_row("G1", "S1", "raw", "ont", n_read_pairs=None, n_reads_single=480)]
        )
        out = dm.compare_qc_numeric(main, dev)
        pairs = out[out.metric == "n_read_pairs"].iloc[0]
        assert pd.isna(pairs.delta)
        single = out[out.metric == "n_reads_single"].iloc[0]
        assert single.delta == -20

    def test_zero_main_gives_na_pct(self) -> None:
        main = pd.DataFrame([_qc_row("G1", "S1", "raw", "illumina", n_reads_single=0)])
        dev = pd.DataFrame([_qc_row("G1", "S1", "raw", "illumina", n_reads_single=5)])
        out = dm.compare_qc_numeric(main, dev, metrics=("n_reads_single",))
        assert pd.isna(out.iloc[0]["pct_change"])
        assert out.iloc[0].delta == 5


class TestCompareQcFlags:
    FLAGS = ["per_base_sequence_quality", "per_tile_sequence_quality"]

    def test_only_changed_flags_returned(self) -> None:
        main = pd.DataFrame(
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
        dev = pd.DataFrame(
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
        out = dm.compare_qc_flags(main, dev, self.FLAGS)
        assert list(out.check) == ["per_base_sequence_quality"]
        assert out.iloc[0].main_flag == "pass" and out.iloc[0].dev_flag == "fail"

    def test_na_vs_na_not_flagged(self) -> None:
        # Both sides NA must NOT register as a change (the float-NaN bug).
        main = pd.DataFrame(
            [
                {
                    "group": "G1",
                    "sample": "S1",
                    "stage": "raw",
                    "per_tile_sequence_quality": None,
                }
            ]
        )
        dev = pd.DataFrame(
            [
                {
                    "group": "G1",
                    "sample": "S1",
                    "stage": "raw",
                    "per_tile_sequence_quality": None,
                }
            ]
        )
        out = dm.compare_qc_flags(main, dev, ["per_tile_sequence_quality"])
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
        main = pd.DataFrame([_kr("G1", False, "S", 1, "a", 100)])
        dev = pd.DataFrame([_kr("G1", False, "S", 2, "b", 100)])
        out = dm.kraken_bray_curtis(main, dev, ranks=("S",))
        assert out.iloc[0].bray_curtis == 1.0

    def test_known_intermediate_value(self) -> None:
        # main 80/20, dev 50/50 -> TVD = 0.5*(|.8-.5|+|.2-.5|) = 0.3
        main = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 80), _kr("G1", False, "S", 2, "b", 20)]
        )
        dev = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 50), _kr("G1", False, "S", 2, "b", 50)]
        )
        out = dm.kraken_bray_curtis(main, dev, ranks=("S",))
        assert abs(out.iloc[0].bray_curtis - 0.3) < 1e-9


class TestKrakenTopMovers:
    def test_orders_by_abs_change_and_signs_delta(self) -> None:
        main = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 90), _kr("G1", False, "S", 2, "b", 10)]
        )
        dev = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 10), _kr("G1", False, "S", 2, "b", 90)]
        )
        out = dm.kraken_top_movers(main, dev, "S", n=1)
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

    def test_identical(self) -> None:
        assert self.tax.divergence_bucket(401, 401) == "identical"

    def test_same_genus_siblings(self) -> None:
        # 401 and 402 share genus 300.
        assert self.tax.divergence_bucket(401, 402) == "same-genus"
        assert self.tax.edge_distance(401, 402) == 2

    def test_same_family_different_genus(self) -> None:
        # 401 (under genus 300/family 200) vs 410 (family 210) share realm 100.
        assert self.tax.divergence_bucket(401, 410) == "same-realm"

    def test_same_realm(self) -> None:
        # 401 (realm 100) vs 500 (realm 110) -> only share Viruses root.
        assert self.tax.divergence_bucket(401, 500) == dm.SHARED_HIGHER

    def test_reassigned_up_to_viral_root(self) -> None:
        # A specific species reassigned to Viruses (10239) shares the viral root.
        assert self.tax.divergence_bucket(401, 10239) == dm.SHARED_HIGHER

    def test_cross_root_virus_to_cellular(self) -> None:
        # 401 (virus) vs 600 (bacterial species) meet only at root.
        assert self.tax.divergence_bucket(401, 600) == dm.CROSS_ROOT

    def test_edge_distance_via_lca(self) -> None:
        # realmA and realmB sit under Viruses, so 401 and 500 meet at 10239:
        # 401->300->200->100->10239 (4 up); 500->110->10239 (2 up); total 6.
        assert self.tax.edge_distance(401, 500) == 6


class TestJoinReadAssignments:
    def _vh(self, rows: list[list]) -> pd.DataFrame:
        return pd.DataFrame(rows, columns=["group", "seq_id", "aligner_taxid_lca"])

    def test_lost_gained_same_reassigned(self) -> None:
        main = self._vh([["G", "r1", 401], ["G", "r2", 401], ["G", "r3", 401]])
        dev = self._vh([["G", "r1", 401], ["G", "r2", 402], ["G", "r4", 500]])
        out = dm.join_read_assignments(main, dev)
        status = dict(zip(out.seq_id, out.status, strict=True))
        assert status["r1"] == "same"
        assert status["r2"] == "reassigned"
        assert status["r3"] == "lost"
        assert status["r4"] == "gained"


class TestSummariseAndBuckets:
    def _joined(self) -> pd.DataFrame:
        main = pd.DataFrame(
            [["G", "r1", 401], ["G", "r2", 401], ["G", "r3", 600]],
            columns=["group", "seq_id", "aligner_taxid_lca"],
        )
        dev = pd.DataFrame(
            [["G", "r1", 402], ["G", "r2", 401], ["G", "r4", 401]],
            columns=["group", "seq_id", "aligner_taxid_lca"],
        )
        return dm.join_read_assignments(main, dev)

    def test_vertebrate_scope_filters(self) -> None:
        joined = self._joined()
        # 401/402 vertebrate; 600 (bacterial) not.
        summ = dm.summarize_read_status(joined, vert={401, 402})
        vert = summ[summ.scope == "vertebrate"].iloc[0]
        # r3 (600->lost) is excluded from vertebrate scope.
        assert vert.n_lost == 0
        all_ = summ[summ.scope == "all"].iloc[0]
        assert all_.n_lost == 1

    def test_bucket_summary_ordered(self) -> None:
        joined = self._joined()
        tax = _synthetic_tree()
        detail = dm.reassignment_distances(joined, tax, vert={401, 402})
        buckets = dm.bucket_summary(detail)
        # r1: 401->402 is same-genus; that should be the only reassigned read.
        all_buckets = buckets[buckets.scope == "all"]
        assert list(all_buckets.bucket) == ["same-genus"]
        assert all_buckets.iloc[0].n_reads == 1


class TestCladeRankShares:
    ANN = pd.DataFrame(
        [
            {"taxid": 200, "name": "familyX", "rank": "family"},
            {"taxid": 210, "name": "familyY", "rank": "family"},
            {"taxid": 300, "name": "genusG", "rank": "genus"},
        ]
    )

    def _clade(self, rows: list[list]) -> pd.DataFrame:
        return pd.DataFrame(
            rows, columns=["group", "taxid", "reads_clade_total", "reads_clade_dedup"]
        )

    def test_family_shares_and_delta(self) -> None:
        main = self._clade([["G", 200, 75, 75], ["G", 210, 25, 25], ["G", 300, 99, 99]])
        dev = self._clade([["G", 200, 50, 50], ["G", 210, 50, 50], ["G", 300, 99, 99]])
        out = dm.clade_rank_shares(
            main,
            dev,
            self.ANN,
            rank_levels=("family",),
            count_cols=("reads_clade_total",),
        )
        fx = out[out.taxid == 200].iloc[0]
        # genus row (300) ignored; familyX share 0.75 -> 0.50.
        assert abs(fx.share_main - 0.75) < 1e-9
        assert abs(fx.share_dev - 0.50) < 1e-9
        assert abs(fx.delta_pp - (-25.0)) < 1e-9

    def test_family_dropped_in_dev(self) -> None:
        main = self._clade([["G", 200, 50, 50], ["G", 210, 50, 50]])
        dev = self._clade([["G", 200, 100, 100]])
        out = dm.clade_rank_shares(
            main,
            dev,
            self.ANN,
            rank_levels=("family",),
            count_cols=("reads_clade_total",),
        )
        fy = out[out.taxid == 210].iloc[0]
        assert fy.share_dev == 0.0
        assert fy.delta_pp == -50.0


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


class TestMadOutlierMask:
    def test_flags_clear_outlier(self) -> None:
        s = pd.Series([1.0, 1.1, 0.9, 1.0, 10.0])
        mask = dm.mad_outlier_mask(s)
        assert mask.iloc[-1]
        assert not mask.iloc[0]

    def test_zero_mad_flags_nothing(self) -> None:
        # All identical -> MAD 0 -> no robust spread -> no flags.
        s = pd.Series([5.0, 5.0, 5.0, 5.0])
        assert not dm.mad_outlier_mask(s).any()


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
        assert "fixed" in flags.iloc[0].flag_type

    def test_cohort_outlier_suppressed_below_magnitude_floor(self) -> None:
        # mean_seq_len pct_change near zero everywhere; one slightly higher but
        # still trivial -> must NOT flag despite being a statistical outlier.
        qc = pd.DataFrame(
            {
                "group": [f"G{i}" for i in range(6)],
                "sample": [f"S{i}" for i in range(6)],
                "stage": ["cleaned"] * 6,
                "platform": ["illumina"] * 6,
                "metric": ["mean_seq_len"] * 6,
                "pct_change": [0.0, 0.001, -0.001, 0.002, -0.002, 0.03],
            }
        )
        flags = dm.build_flags({"qc_numeric": qc})
        assert flags.empty

    def test_cohort_outlier_flagged_when_magnitude_meaningful(self) -> None:
        # One group's reassignment rate is a clear, sizable cohort outlier.
        status = pd.DataFrame(
            {
                "group": [f"G{i}" for i in range(6)],
                "scope": ["vertebrate"] * 6,
                "pct_lost": [0.1, 0.2, 0.1, 0.15, 0.1, 0.12],
                "pct_reassigned": [1.0, 1.1, 0.9, 1.0, 1.2, 9.0],
            }
        )
        flags = dm.build_flags({"viral_read_status": status})
        reassign_flags = flags[flags.metric.str.contains("reassigned")]
        assert any("G5" in k for k in reassign_flags.key)
