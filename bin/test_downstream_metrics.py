"""Unit tests for downstream_metrics.py (pure calculation functions).

Tests use small synthetic manifests/DataFrames only -- never real delivery data.
Test order mirrors the order of functions in downstream_metrics.py.
"""

import downstream_metrics as dm
import pandas as pd


def _entry(present=True, n_rows=None, columns=None) -> dm.FileEntry:
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
    def test_row_delta_and_pct(self):
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

    def test_presence_mismatch_one_side(self):
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

    def test_zero_rows_main_gives_null_pct_not_divide_by_zero(self):
        main = _manifest(
            {"G1": ("illumina", {"bracken": _entry(n_rows=0, columns=[])})}
        )
        dev = _manifest({"G1": ("illumina", {"bracken": _entry(n_rows=3, columns=[])})})
        df = dm.compare_file_inventory(main, dev)
        row = df.iloc[0]
        assert row.row_delta == 3
        assert pd.isna(row.row_pct_change)

    def test_json_file_has_null_rows(self):
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
    def test_conformant_columns_report_clean(self):
        cols = ["seq_id", "group"]
        man = _manifest({"G1": ("illumina", {"validation_hits": _entry(columns=cols)})})
        df = dm.compare_columns_to_schema(man, man, {"validation_hits": cols})
        row = df.iloc[0]
        assert row.missing_vs_schema_main == ""
        assert row.extra_vs_schema_main == ""
        assert row.cols_only_in_main == ""
        assert not row.order_changed

    def test_empty_file_reports_empty_marker_not_full_schema(self):
        man = _manifest({"G1": ("illumina", {"bracken": _entry(n_rows=0, columns=[])})})
        df = dm.compare_columns_to_schema(
            man, man, {"bracken": ["taxid", "name", "fraction_total_reads"]}
        )
        row = df.iloc[0]
        assert row.missing_vs_schema_main == "(empty file)"
        assert row.missing_vs_schema_dev == "(empty file)"

    def test_column_added_in_dev_is_flagged(self):
        main = _manifest({"G1": ("illumina", {"kraken": _entry(columns=["taxid"])})})
        dev = _manifest(
            {"G1": ("illumina", {"kraken": _entry(columns=["taxid", "new_col"])})}
        )
        df = dm.compare_columns_to_schema(main, dev, {"kraken": ["taxid"]})
        row = df.iloc[0]
        assert row.cols_only_in_dev == "new_col"
        assert row.cols_only_in_main == ""
        assert row.extra_vs_schema_dev == "new_col"

    def test_reorder_only_sets_order_changed(self):
        main = _manifest({"G1": ("illumina", {"kraken": _entry(columns=["a", "b"])})})
        dev = _manifest({"G1": ("illumina", {"kraken": _entry(columns=["b", "a"])})})
        df = dm.compare_columns_to_schema(main, dev, {"kraken": ["a", "b"]})
        row = df.iloc[0]
        assert row.order_changed
        assert row.cols_only_in_main == "" and row.cols_only_in_dev == ""

    def test_file_type_without_schema_still_reported(self):
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


def _qc_row(group, sample, stage, platform, **vals) -> dict:
    base = {"group": group, "sample": sample, "stage": stage, "platform": platform}
    base.update(vals)
    return base


class TestCompareQcNumeric:
    def test_delta_and_pct_per_key(self):
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

    def test_na_metric_yields_na_delta(self):
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

    def test_zero_main_gives_na_pct(self):
        main = pd.DataFrame([_qc_row("G1", "S1", "raw", "illumina", n_reads_single=0)])
        dev = pd.DataFrame([_qc_row("G1", "S1", "raw", "illumina", n_reads_single=5)])
        out = dm.compare_qc_numeric(main, dev, metrics=("n_reads_single",))
        assert pd.isna(out.iloc[0]["pct_change"])
        assert out.iloc[0].delta == 5


class TestCompareQcFlags:
    FLAGS = ["per_base_sequence_quality", "per_tile_sequence_quality"]

    def test_only_changed_flags_returned(self):
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

    def test_na_vs_na_not_flagged(self):
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


def _kr(group, ribosomal, rank, taxid, name, n) -> dict:
    return {
        "group": group,
        "ribosomal": ribosomal,
        "rank": rank,
        "taxid": taxid,
        "name": name,
        "n_reads_clade": n,
    }


class TestKrakenRelativeAbundance:
    def test_filters_rank_and_aggregates_samples_to_fraction(self):
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

    def test_zero_total_set_dropped(self):
        df = pd.DataFrame([_kr("G1", False, "S", 1, "sp1", 0)])
        out = dm.kraken_relative_abundance(df, "S")
        assert out.empty


class TestKrakenBrayCurtis:
    def test_identical_profiles_give_zero(self):
        df = pd.DataFrame(
            [_kr("G1", False, "S", 1, "a", 50), _kr("G1", False, "S", 2, "b", 50)]
        )
        out = dm.kraken_bray_curtis(df, df, ranks=("S",))
        assert out.iloc[0].bray_curtis == 0.0

    def test_disjoint_profiles_give_one(self):
        main = pd.DataFrame([_kr("G1", False, "S", 1, "a", 100)])
        dev = pd.DataFrame([_kr("G1", False, "S", 2, "b", 100)])
        out = dm.kraken_bray_curtis(main, dev, ranks=("S",))
        assert out.iloc[0].bray_curtis == 1.0

    def test_known_intermediate_value(self):
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
    def test_orders_by_abs_change_and_signs_delta(self):
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
