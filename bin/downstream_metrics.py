#!/usr/bin/env python3
DESC = """
Pure calculation functions for the DOWNSTREAM dev-vs-main release comparison
(see compare_downstream_runs.py for the I/O and orchestration that feed these).

This module is deliberately free of network/filesystem/argparse code so the
*calculations* that drive the release-regression report can be reviewed and
unit-tested in isolation from the munging. Every function takes already-parsed
in-memory inputs (DataFrames, dicts, manifests) and returns DataFrames or plain
data structures.

Grouped by report focus:
  - Focus 4: schema-driven file/column inventory comparison.
  - (later focuses appended as the report is built up.)
"""

###########
# IMPORTS #
###########

from dataclasses import dataclass, field

import pandas as pd

#####################
# SHARED DATA MODEL #
#####################


@dataclass
class FileEntry:
    """One discovered per-group output file on one side (main or dev).

    Attributes:
        present: Whether the file exists for this group on this side.
        n_rows: Data row count (excluding header) for TSVs; None for JSON or
            when the file is absent / could not be read.
        columns: Ordered column names for TSVs; None for JSON or when absent.
    """

    present: bool = False
    n_rows: int | None = None
    columns: list[str] | None = None


@dataclass
class GroupManifest:
    """All discovered output files for one group on one side.

    Attributes:
        platform: 'illumina' or 'ont', inferred from file presence upstream.
        files: Maps file-type key (e.g. 'validation_hits', 'qc_basic_stats_raw')
            to its FileEntry.
    """

    platform: str
    files: dict[str, FileEntry] = field(default_factory=dict)


# A side manifest maps group name -> GroupManifest.
SideManifest = dict[str, GroupManifest]


##################################################
# FOCUS 4: SCHEMA-DRIVEN FILE/COLUMN COMPARISON  #
##################################################


def compare_file_inventory(main: SideManifest, dev: SideManifest) -> pd.DataFrame:
    """Compare presence and row counts of every per-group output file.

    Generic over file types: it simply walks whatever file-type keys appear in
    either manifest, so new outputs are picked up without code changes.

    Args:
        main: Side manifest for the reference (main) run.
        dev: Side manifest for the candidate (dev) run.

    Returns:
        Long-format DataFrame with one row per (group, file_type), columns:
        group, platform, file_type, in_main, in_dev, n_rows_main, n_rows_dev,
        row_delta, row_pct_change. n_rows_* are <NA> for JSON/unreadable files;
        row_delta/row_pct_change are <NA> unless both sides have a row count.
    """
    groups = sorted(set(main) | set(dev))
    records: list[dict[str, object]] = []
    for group in groups:
        gm_main = main.get(group)
        gm_dev = dev.get(group)
        gm_any = gm_dev or gm_main
        platform = gm_any.platform if gm_any else ""
        file_types = sorted(
            (set(gm_main.files) if gm_main else set())
            | (set(gm_dev.files) if gm_dev else set())
        )
        for ft in file_types:
            fe_main = gm_main.files.get(ft) if gm_main else None
            fe_dev = gm_dev.files.get(ft) if gm_dev else None
            in_main = bool(fe_main and fe_main.present)
            in_dev = bool(fe_dev and fe_dev.present)
            rows_main = fe_main.n_rows if fe_main else None
            rows_dev = fe_dev.n_rows if fe_dev else None
            if rows_main is not None and rows_dev is not None:
                d = rows_dev - rows_main
                delta = d
                pct = 100.0 * d / rows_main if rows_main else None
            else:
                delta = None
                pct = None
            records.append(
                {
                    "group": group,
                    "platform": platform,
                    "file_type": ft,
                    "in_main": in_main,
                    "in_dev": in_dev,
                    "n_rows_main": rows_main,
                    "n_rows_dev": rows_dev,
                    "row_delta": delta,
                    "row_pct_change": pct,
                }
            )
    df = pd.DataFrame.from_records(
        records,
        columns=[
            "group",
            "platform",
            "file_type",
            "in_main",
            "in_dev",
            "n_rows_main",
            "n_rows_dev",
            "row_delta",
            "row_pct_change",
        ],
    )
    # Nullable integer dtypes so missing row counts render as <NA>, not NaN/float.
    for col in ("n_rows_main", "n_rows_dev", "row_delta"):
        df[col] = df[col].astype("Int64")
    return df


def _columns_for_type(manifest: SideManifest, file_type: str) -> list[str] | None:
    """Return the column list seen for `file_type` in `manifest`.

    Columns are expected to be identical across all groups of a given platform,
    so the first group that has them is representative. Returns None if no group
    on this side carries columns for this file type (e.g. JSON or all absent).
    """
    for gm in manifest.values():
        fe = gm.files.get(file_type)
        if fe and fe.columns is not None:
            return fe.columns
    return None


def compare_columns_to_schema(
    main: SideManifest,
    dev: SideManifest,
    schema_columns: dict[str, list[str]],
) -> pd.DataFrame:
    """Check each file type's columns against its schema and across sides.

    For every tabular file type present on either side, compares the observed
    column set/order to the schema's declared fields (when a schema exists) and
    to the other side. Schema-driven: file types without a matching schema are
    still reported (with empty schema columns) so unschema'd outputs surface.

    Args:
        main: Side manifest for main.
        dev: Side manifest for dev.
        schema_columns: Maps file-type key -> ordered schema field names.

    Returns:
        DataFrame with one row per file_type, columns: file_type,
        has_schema, missing_vs_schema_main, extra_vs_schema_main,
        missing_vs_schema_dev, extra_vs_schema_dev, cols_only_in_main,
        cols_only_in_dev, order_changed. List-valued cells are
        comma-joined strings ('' when empty).
    """
    file_types: set[str] = set()
    for manifest in (main, dev):
        for gm in manifest.values():
            file_types.update(
                ft for ft, fe in gm.files.items() if fe.columns is not None
            )
    records: list[dict[str, object]] = []
    for ft in sorted(file_types):
        cols_main = _columns_for_type(main, ft)
        cols_dev = _columns_for_type(dev, ft)
        schema = schema_columns.get(ft)
        miss_main, extra_main = _schema_cells(schema, cols_main)
        miss_dev, extra_dev = _schema_cells(schema, cols_dev)
        rec: dict[str, object] = {
            "file_type": ft,
            "has_schema": schema is not None,
            "missing_vs_schema_main": miss_main,
            "extra_vs_schema_main": extra_main,
            "missing_vs_schema_dev": miss_dev,
            "extra_vs_schema_dev": extra_dev,
            "cols_only_in_main": _join(_only_in(cols_main, cols_dev)),
            "cols_only_in_dev": _join(_only_in(cols_dev, cols_main)),
            "order_changed": (
                bool(cols_main)
                and bool(cols_dev)
                and cols_main != cols_dev
                and set(cols_main or []) == set(cols_dev or [])
            ),
        }
        records.append(rec)
    return pd.DataFrame.from_records(
        records,
        columns=[
            "file_type",
            "has_schema",
            "missing_vs_schema_main",
            "extra_vs_schema_main",
            "missing_vs_schema_dev",
            "extra_vs_schema_dev",
            "cols_only_in_main",
            "cols_only_in_dev",
            "order_changed",
        ],
    )


def _schema_cells(
    schema: list[str] | None, observed: list[str] | None
) -> tuple[str, str]:
    """Format (missing, extra) schema-comparison cells for one side.

    Empty-but-present files (header absent) would otherwise report every schema
    field as 'missing'; surface them as '(empty file)' instead so the signal is
    'this output is empty', not a wall of column names.
    """
    if observed is None:
        return "", ""
    if observed == []:
        return "(empty file)", ""
    return _join(_missing(schema, observed)), _join(_extra(schema, observed))


def _missing(schema: list[str] | None, observed: list[str] | None) -> list[str]:
    """Schema fields absent from the observed columns (order preserved)."""
    if schema is None or observed is None:
        return []
    seen = set(observed)
    return [c for c in schema if c not in seen]


def _extra(schema: list[str] | None, observed: list[str] | None) -> list[str]:
    """Observed columns not declared in the schema (order preserved)."""
    if schema is None or observed is None:
        return []
    declared = set(schema)
    return [c for c in observed if c not in declared]


def _only_in(a: list[str] | None, b: list[str] | None) -> list[str]:
    """Columns in `a` but not `b` (order preserved); empty if either is None."""
    if a is None or b is None:
        return []
    other = set(b)
    return [c for c in a if c not in other]


def _join(items: list[str]) -> str:
    """Comma-join a list for a single table cell ('' when empty)."""
    return ", ".join(items)


########################################
# FOCUS 3: QUALITY METRICS (qc_basic)  #
########################################

# Numeric QC metrics compared per (group, sample, stage). n_read_pairs is NA for
# single-end (ONT) data; n_reads_single is populated for both platforms.
QC_NUMERIC_METRICS = (
    "n_reads_single",
    "n_read_pairs",
    "mean_seq_len",
    "percent_gc",
    "percent_duplicates",
    "n_bases_approx",
)

QC_KEYS = ["group", "sample", "stage"]


def _melt_qc(df: pd.DataFrame, metrics: tuple[str, ...]) -> pd.DataFrame:
    """Melt a wide qc_basic_stats frame to long (keys + platform, metric, value)."""
    present = [m for m in metrics if m in df.columns]
    return df.melt(
        id_vars=[*QC_KEYS, "platform"],
        value_vars=present,
        var_name="metric",
        value_name="value",
    )


def compare_qc_numeric(
    main: pd.DataFrame,
    dev: pd.DataFrame,
    metrics: tuple[str, ...] = QC_NUMERIC_METRICS,
) -> pd.DataFrame:
    """Compare numeric qc_basic_stats metrics per (group, sample, stage).

    Args:
        main: Concatenated qc_basic_stats (raw + cleaned) for the main run, with
            a `platform` column added.
        dev: Same for the dev run.
        metrics: Numeric metric column names to compare.

    Returns:
        Long DataFrame: group, sample, platform, stage, metric, main, dev,
        delta, pct_change. NA-valued metrics (e.g. n_read_pairs for ONT) yield
        <NA> deltas rather than spurious numbers.
    """
    long_main = _melt_qc(main, metrics)
    long_dev = _melt_qc(dev, metrics)
    merged = long_main.merge(
        long_dev,
        on=[*QC_KEYS, "metric"],
        how="outer",
        suffixes=("_main", "_dev"),
    )
    merged["platform"] = merged["platform_main"].fillna(merged["platform_dev"])
    main_val = pd.to_numeric(merged["value_main"], errors="coerce")
    dev_val = pd.to_numeric(merged["value_dev"], errors="coerce")
    delta = dev_val - main_val
    pct = delta.where(main_val != 0).div(main_val.where(main_val != 0)) * 100.0
    out = pd.DataFrame(
        {
            "group": merged["group"],
            "sample": merged["sample"],
            "platform": merged["platform"],
            "stage": merged["stage"],
            "metric": merged["metric"],
            "main": main_val,
            "dev": dev_val,
            "delta": delta,
            "pct_change": pct,
        }
    )
    return out.sort_values(["group", "sample", "stage", "metric"]).reset_index(
        drop=True
    )


def compare_qc_flags(
    main: pd.DataFrame, dev: pd.DataFrame, flag_cols: list[str]
) -> pd.DataFrame:
    """Compare FASTQC pass/warn/fail flags per (group, sample, stage, check).

    Args:
        main: Concatenated qc_basic_stats for main.
        dev: Same for dev.
        flag_cols: FASTQC flag column names (pass/warn/fail strings).

    Returns:
        Long DataFrame of only the flags that CHANGED: group, sample, stage,
        check, main_flag, dev_flag.
    """
    present = [c for c in flag_cols if c in main.columns and c in dev.columns]
    long_main = main.melt(
        id_vars=QC_KEYS, value_vars=present, var_name="check", value_name="main_flag"
    )
    long_dev = dev.melt(
        id_vars=QC_KEYS, value_vars=present, var_name="check", value_name="dev_flag"
    )
    merged = long_main.merge(long_dev, on=[*QC_KEYS, "check"], how="outer")
    # Normalize missing flags to a sentinel: astype(str) leaves NaN as a float
    # NaN (NaN != NaN is True), which would spuriously flag NA-vs-NA as changed.
    main_flag = merged["main_flag"].fillna("NA").astype(str)
    dev_flag = merged["dev_flag"].fillna("NA").astype(str)
    merged["main_flag"] = main_flag
    merged["dev_flag"] = dev_flag
    changed = main_flag != dev_flag
    return (
        merged[changed]
        .sort_values([*QC_KEYS, "check"])
        .reset_index(drop=True)[[*QC_KEYS, "check", "main_flag", "dev_flag"]]
    )
