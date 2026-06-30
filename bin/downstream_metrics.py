#!/usr/bin/env python3
DESC = """
Pure calculation functions for the DOWNSTREAM candidate-vs-reference comparison
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
from typing import Any, cast

import pandas as pd

#####################
# SHARED DATA MODEL #
#####################


@dataclass
class FileEntry:
    """One discovered per-group output file on one side (reference or candidate).

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


def compare_file_inventory(
    reference: SideManifest,
    candidate: SideManifest,
    expected_types: dict[str, set[str]] | None = None,
) -> pd.DataFrame:
    """Compare presence and row counts of every per-group output file.

    Generic over file types: it walks whatever file-type keys appear in either
    manifest, so new outputs are picked up without code changes. When
    `expected_types` is given, each group's platform-expected types are also
    included, so an output absent from BOTH sides still shows up as a row with
    in_reference = in_candidate = False (rather than being silently invisible).

    Args:
        reference: Side manifest for the reference run.
        candidate: Side manifest for the candidate run.
        expected_types: Optional {platform: {file_type, ...}} of expected
            per-group outputs, used to surface types missing on both sides.

    Returns:
        Long-format DataFrame with one row per (group, file_type), columns:
        group, platform, file_type, in_reference, in_candidate, n_rows_reference,
        n_rows_candidate, row_delta, row_pct_change. n_rows_* are <NA> for
        JSON/unreadable files; row_delta/row_pct_change are <NA> unless both
        sides have a row count.
    """
    expected_types = expected_types or {}
    groups = sorted(set(reference) | set(candidate))
    records: list[dict[str, object]] = []
    for group in groups:
        gm_reference = reference.get(group)
        gm_candidate = candidate.get(group)
        # Use BOTH sides' platforms: if they disagree (e.g. a group is Illumina
        # on reference but degraded to ONT on candidate), flag the mismatch and
        # union the expected types from both so no platform's expected outputs
        # are dropped.
        platforms = [gm.platform for gm in (gm_reference, gm_candidate) if gm]
        unique = sorted(set(platforms))
        if len(unique) == 1:
            platform = unique[0]
        elif not unique:
            platform = ""
        else:
            platform = "/".join(unique) + " (mismatch)"
        expected_union: set[str] = set()
        for p in unique:
            expected_union |= expected_types.get(p, set())
        file_types = sorted(
            (set(gm_reference.files) if gm_reference else set())
            | (set(gm_candidate.files) if gm_candidate else set())
            | expected_union
        )
        for ft in file_types:
            fe_reference = gm_reference.files.get(ft) if gm_reference else None
            fe_candidate = gm_candidate.files.get(ft) if gm_candidate else None
            in_reference = bool(fe_reference and fe_reference.present)
            in_candidate = bool(fe_candidate and fe_candidate.present)
            rows_reference = fe_reference.n_rows if fe_reference else None
            rows_candidate = fe_candidate.n_rows if fe_candidate else None
            if rows_reference is not None and rows_candidate is not None:
                d = rows_candidate - rows_reference
                delta = d
                pct = 100.0 * d / rows_reference if rows_reference else None
            else:
                delta = None
                pct = None
            records.append(
                {
                    "group": group,
                    "platform": platform,
                    "file_type": ft,
                    "in_reference": in_reference,
                    "in_candidate": in_candidate,
                    "n_rows_reference": rows_reference,
                    "n_rows_candidate": rows_candidate,
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
            "in_reference",
            "in_candidate",
            "n_rows_reference",
            "n_rows_candidate",
            "row_delta",
            "row_pct_change",
        ],
    )
    # Nullable integer dtypes so missing row counts render as <NA>, not NaN/float.
    for col in ("n_rows_reference", "n_rows_candidate", "row_delta"):
        df[col] = df[col].astype("Int64")
    return df


def _columns_consistent(manifest: SideManifest, file_type: str) -> bool:
    """Whether groups of the SAME platform share one header for `file_type`.

    Guards against the first-group-only assumption. Consistency is judged within
    each platform, so a benign cross-platform ordering difference (e.g. ONT places
    paired-end columns last) is not flagged — only genuine intra-platform
    disagreement is.
    """
    by_platform: dict[str, set[tuple[str, ...]]] = {}
    for gm in manifest.values():
        fe = gm.files.get(file_type)
        if fe and fe.columns is not None:
            by_platform.setdefault(gm.platform, set()).add(tuple(fe.columns))
    return all(len(headers) <= 1 for headers in by_platform.values())


def _all_columns(manifest: SideManifest, file_type: str) -> list[list[str]]:
    """Every distinct header observed for `file_type` across groups on this side."""
    seen: list[list[str]] = []
    for gm in manifest.values():
        fe = gm.files.get(file_type)
        if fe and fe.columns is not None and fe.columns not in seen:
            seen.append(fe.columns)
    return seen


def _schema_cells_aggregated(
    schema: list[str] | None, headers: list[list[str]]
) -> tuple[str, str]:
    """(missing, extra) vs schema aggregated over all group headers on a side.

    A schema field is 'missing' if absent from ANY group's header (so a single
    group dropping a required column is caught); a column is 'extra' if present in
    ANY group's header but not in the schema. Empty-but-present headers surface as
    '(empty file)'.
    """
    if not headers:
        return "", ""
    if any(h == [] for h in headers):
        return "(empty file)", ""
    if schema is None:
        return "", ""
    schema_set = set(schema)
    missing = [c for c in schema if any(c not in h for h in headers)]
    seen_extra: list[str] = []
    for h in headers:
        for c in h:
            if c not in schema_set and c not in seen_extra:
                seen_extra.append(c)
    return _join(missing), _join(seen_extra)


def compare_columns_to_schema(
    reference: SideManifest,
    candidate: SideManifest,
    schema_columns: dict[str, list[str]],
) -> pd.DataFrame:
    """Check each file type's columns against its schema for both sides.

    For every tabular file type present on either side, compares the observed
    columns to the schema's declared fields (when a schema exists). Schema-driven:
    file types without a matching schema are still reported (with empty schema
    columns) so unschema'd outputs surface.

    Missing/extra columns are aggregated across ALL group headers per side, so a
    later group's drop/add is caught. `groups_consistent_*` reports whether groups
    within a side agree on this file's columns. Cross-side column ORDER is not
    schema-checked because the schema legitimately permits platform-specific
    ordering (e.g. ONT places paired-end columns last).

    Args:
        reference: Side manifest for the reference run.
        candidate: Side manifest for the candidate run.
        schema_columns: Maps file-type key -> ordered schema field names.

    Returns:
        DataFrame with one row per file_type, columns: file_type,
        has_schema, missing_vs_schema_reference, extra_vs_schema_reference,
        missing_vs_schema_candidate, extra_vs_schema_candidate,
        groups_consistent_reference, groups_consistent_candidate. List-valued
        cells are comma-joined strings ('' when empty).
    """
    file_types: set[str] = set()
    for manifest in (reference, candidate):
        for gm in manifest.values():
            file_types.update(
                ft for ft, fe in gm.files.items() if fe.columns is not None
            )
    records: list[dict[str, object]] = []
    for ft in sorted(file_types):
        schema = schema_columns.get(ft)
        # Aggregate missing/extra across ALL groups' headers on each side, so a
        # later group dropping/adding a column is caught (not just the first).
        miss_reference, extra_reference = _schema_cells_aggregated(
            schema, _all_columns(reference, ft)
        )
        miss_candidate, extra_candidate = _schema_cells_aggregated(
            schema, _all_columns(candidate, ft)
        )
        rec: dict[str, object] = {
            "file_type": ft,
            "has_schema": schema is not None,
            "missing_vs_schema_reference": miss_reference,
            "extra_vs_schema_reference": extra_reference,
            "missing_vs_schema_candidate": miss_candidate,
            "extra_vs_schema_candidate": extra_candidate,
            # True when groups within a side agree on this file's columns.
            "groups_consistent_reference": _columns_consistent(reference, ft),
            "groups_consistent_candidate": _columns_consistent(candidate, ft),
        }
        records.append(rec)
    return pd.DataFrame.from_records(
        records,
        columns=[
            "file_type",
            "has_schema",
            "missing_vs_schema_reference",
            "extra_vs_schema_reference",
            "missing_vs_schema_candidate",
            "extra_vs_schema_candidate",
            "groups_consistent_reference",
            "groups_consistent_candidate",
        ],
    )


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
    reference: pd.DataFrame,
    candidate: pd.DataFrame,
    metrics: tuple[str, ...] = QC_NUMERIC_METRICS,
) -> pd.DataFrame:
    """Compare numeric qc_basic_stats metrics per (group, sample, stage).

    Args:
        reference: Concatenated qc_basic_stats (raw + cleaned) for the reference
            run, with a `platform` column added.
        candidate: Same for the candidate run.
        metrics: Numeric metric column names to compare.

    Returns:
        Long DataFrame: group, sample, platform, stage, metric, reference,
        candidate, delta, pct_change. NA-valued metrics (e.g. n_read_pairs for
        ONT) yield <NA> deltas rather than spurious numbers.
    """
    long_reference = _melt_qc(reference, metrics)
    long_candidate = _melt_qc(candidate, metrics)
    merged = long_reference.merge(
        long_candidate,
        on=[*QC_KEYS, "metric"],
        how="outer",
        suffixes=("_reference", "_candidate"),
    )
    merged["platform"] = merged["platform_reference"].fillna(
        merged["platform_candidate"]
    )
    reference_val = pd.to_numeric(merged["value_reference"], errors="coerce")
    candidate_val = pd.to_numeric(merged["value_candidate"], errors="coerce")
    delta = candidate_val - reference_val
    pct = (
        delta.where(reference_val != 0).div(reference_val.where(reference_val != 0))
        * 100.0
    )
    out = pd.DataFrame(
        {
            "group": merged["group"],
            "sample": merged["sample"],
            "platform": merged["platform"],
            "stage": merged["stage"],
            "metric": merged["metric"],
            "reference": reference_val,
            "candidate": candidate_val,
            "delta": delta,
            "pct_change": pct,
        }
    )
    return out.sort_values(["group", "sample", "stage", "metric"]).reset_index(
        drop=True
    )


def compare_qc_flags(
    reference: pd.DataFrame, candidate: pd.DataFrame, flag_cols: list[str]
) -> pd.DataFrame:
    """Compare FASTQC pass/warn/fail flags per (group, sample, stage, check).

    Args:
        reference: Concatenated qc_basic_stats for the reference run.
        candidate: Same for the candidate run.
        flag_cols: FASTQC flag column names (pass/warn/fail strings).

    Returns:
        Long DataFrame of only the flags that CHANGED: group, sample, stage,
        check, reference_flag, candidate_flag.
    """
    present = [
        c for c in flag_cols if c in reference.columns and c in candidate.columns
    ]
    long_reference = reference.melt(
        id_vars=QC_KEYS,
        value_vars=present,
        var_name="check",
        value_name="reference_flag",
    )
    long_candidate = candidate.melt(
        id_vars=QC_KEYS,
        value_vars=present,
        var_name="check",
        value_name="candidate_flag",
    )
    merged = long_reference.merge(long_candidate, on=[*QC_KEYS, "check"], how="outer")
    # Normalize missing flags to a sentinel: astype(str) leaves NaN as a float
    # NaN (NaN != NaN is True), which would spuriously flag NA-vs-NA as changed.
    reference_flag = merged["reference_flag"].fillna("NA").astype(str)
    candidate_flag = merged["candidate_flag"].fillna("NA").astype(str)
    merged["reference_flag"] = reference_flag
    merged["candidate_flag"] = candidate_flag
    changed = reference_flag != candidate_flag
    return (
        merged[changed]
        .sort_values([*QC_KEYS, "check"])
        .reset_index(drop=True)[[*QC_KEYS, "check", "reference_flag", "candidate_flag"]]
    )


def qc_read_survival(
    reference_qc: pd.DataFrame, candidate_qc: pd.DataFrame
) -> pd.DataFrame:
    """Compare the raw->cleaned read-survival fraction per (group, sample).

    Survival is computed WITHIN each run as cleaned/raw read count, then compared
    across runs. This is the metric that reflects a QC/screen change (e.g. a
    FASTP min-length change), unlike a cross-run change in the absolute cleaned
    count (which is masked when both runs subsample to the same depth upstream).

    Args:
        reference_qc: concatenated qc_basic_stats (raw + cleaned) for the
            reference run.
        candidate_qc: same for the candidate run.

    Returns:
        DataFrame: group, sample, platform, survival_reference,
        survival_candidate, delta_pp (candidate - reference, in percentage
        points). survival_* are <NA> when a stage is missing or raw count is 0.
    """

    def survival(df: pd.DataFrame) -> pd.DataFrame:
        sub = df[["group", "sample", "stage", "platform", "n_reads_single"]].copy()
        sub["n_reads_single"] = pd.to_numeric(sub["n_reads_single"], errors="coerce")
        piv = sub.pivot_table(
            index=["group", "sample", "platform"],
            columns="stage",
            values="n_reads_single",
            aggfunc="first",
        ).reset_index()
        raw = piv["raw"] if "raw" in piv else pd.Series(pd.NA, index=piv.index)
        cleaned = (
            piv["cleaned"] if "cleaned" in piv else pd.Series(pd.NA, index=piv.index)
        )
        piv["survival"] = cleaned.where(raw > 0) / raw.where(raw > 0)
        return piv[["group", "sample", "platform", "survival"]]

    a = survival(reference_qc).rename(columns={"survival": "survival_reference"})
    b = survival(candidate_qc).rename(columns={"survival": "survival_candidate"})
    merged = a.merge(
        b, on=["group", "sample"], how="outer", suffixes=("_reference", "_candidate")
    )
    # Coalesce platform from both sides so a candidate-only (or reference-only)
    # sample still carries a platform rather than dropping its survival row.
    merged["platform"] = merged["platform_reference"].fillna(
        merged["platform_candidate"]
    )
    merged["delta_pp"] = (
        merged["survival_candidate"] - merged["survival_reference"]
    ) * 100.0
    return (
        merged[
            [
                "group",
                "sample",
                "platform",
                "survival_reference",
                "survival_candidate",
                "delta_pp",
            ]
        ]
        .sort_values(["group", "sample"])
        .reset_index(drop=True)
    )


#########################################
# FOCUS 2: KRAKEN ABUNDANCES            #
#########################################

# Kraken ribosomal/non-ribosomal read sets are compared separately; abundance is
# compared at these rank codes by default.
KRAKEN_RANKS = ("G", "S")


def kraken_relative_abundance(df: pd.DataFrame, rank: str) -> pd.DataFrame:
    """Relative abundance of each taxon at `rank` per (group, ribosomal).

    Reads are aggregated across samples within a group using clade read counts
    (n_reads_clade), so sub-rank reads roll up into their rank-level ancestor's
    clade total. Relative abundance is each taxon's share of the total clade
    reads assigned at that rank within the (group, ribosomal) set.

    Args:
        df: Long kraken frame with columns group, ribosomal, rank, taxid, name,
            n_reads_clade.
        rank: Kraken rank code to filter to (e.g. 'S', 'G').

    Returns:
        DataFrame: group, ribosomal, taxid, name, n_reads_clade, rel. Sets whose
        total is zero are dropped (no abundance is defined).
    """
    sub = df[df["rank"] == rank].copy()
    agg = sub.groupby(["group", "ribosomal", "taxid"], as_index=False).agg(
        n_reads_clade=("n_reads_clade", "sum"),
        name=("name", "first"),
    )
    totals = agg.groupby(["group", "ribosomal"])["n_reads_clade"].transform("sum")
    agg = agg[totals > 0].copy()
    totals = totals[totals > 0]
    agg["rel"] = agg["n_reads_clade"] / totals
    return agg


def _merge_abundance(
    reference: pd.DataFrame, candidate: pd.DataFrame, rank: str
) -> pd.DataFrame:
    """Outer-join reference/candidate relative abundance at `rank`, fill absent 0."""
    a = kraken_relative_abundance(reference, rank)
    b = kraken_relative_abundance(candidate, rank)
    merged = a.merge(
        b,
        on=["group", "ribosomal", "taxid"],
        how="outer",
        suffixes=("_reference", "_candidate"),
    )
    merged["rel_reference"] = merged["rel_reference"].fillna(0.0)
    merged["rel_candidate"] = merged["rel_candidate"].fillna(0.0)
    merged["name"] = merged["name_reference"].fillna(merged["name_candidate"])
    merged["abs_diff"] = (merged["rel_reference"] - merged["rel_candidate"]).abs()
    return merged


def kraken_bray_curtis(
    reference: pd.DataFrame,
    candidate: pd.DataFrame,
    ranks: tuple[str, ...] = KRAKEN_RANKS,
) -> pd.DataFrame:
    """Bray-Curtis dissimilarity per (group, ribosomal, rank).

    For abundance vectors that each sum to 1, Bray-Curtis equals the total
    variation distance, 0.5 * sum|x_i - y_i| (0 = identical, 1 = disjoint).

    Returns:
        DataFrame: group, ribosomal, rank, bray_curtis, n_taxa_union.
    """
    records: list[dict[str, object]] = []
    for rank in ranks:
        merged = _merge_abundance(reference, candidate, rank)
        grouped = merged.groupby(["group", "ribosomal"])
        for (group, ribosomal), sub in grouped:
            # Bray-Curtis = sum|x_i - y_i| / (sum x + sum y). When both sides sum
            # to 1 (the usual case) this is 0.5 * L1; but if a (group, ribosomal)
            # set has reads on only one side the other side sums to 0, and this
            # general form correctly yields 1.0 (disjoint) rather than 0.5. Both
            # sides empty -> undefined (NaN).
            denom = sub["rel_reference"].sum() + sub["rel_candidate"].sum()
            bc = sub["abs_diff"].sum() / denom if denom > 0 else float("nan")
            records.append(
                {
                    "group": group,
                    "ribosomal": ribosomal,
                    "rank": rank,
                    "bray_curtis": bc,
                    "n_taxa_union": len(sub),
                }
            )
    return (
        pd.DataFrame.from_records(
            records,
            columns=["group", "ribosomal", "rank", "bray_curtis", "n_taxa_union"],
        )
        .sort_values(["group", "rank", "ribosomal"])
        .reset_index(drop=True)
    )


def kraken_top_movers(
    reference: pd.DataFrame,
    candidate: pd.DataFrame,
    rank: str,
    n: int = 10,
) -> pd.DataFrame:
    """Top `n` taxa by absolute abundance change per (group, ribosomal) at `rank`.

    Returns:
        DataFrame: group, ribosomal, rank, taxid, name, pct_reference,
        pct_candidate, delta_pp (percentage-point change, candidate - reference),
        ordered by |delta_pp|.
    """
    merged = _merge_abundance(reference, candidate, rank)
    merged["pct_reference"] = merged["rel_reference"] * 100.0
    merged["pct_candidate"] = merged["rel_candidate"] * 100.0
    merged["delta_pp"] = merged["pct_candidate"] - merged["pct_reference"]
    merged["rank"] = rank
    # taxid tiebreaker so the top-n cutoff is deterministic when taxa tie on
    # abs_diff right at the boundary.
    ordered = merged.sort_values(
        ["abs_diff", "taxid"], ascending=[False, True], kind="stable"
    )
    top = ordered.groupby(
        ["group", "ribosomal"], as_index=False, group_keys=False
    ).head(n)
    return top.sort_values(
        ["group", "ribosomal", "abs_diff"], ascending=[True, True, False]
    )[
        [
            "group",
            "ribosomal",
            "rank",
            "taxid",
            "name",
            "pct_reference",
            "pct_candidate",
            "delta_pp",
        ]
    ].reset_index(drop=True)


#########################################
# FOCUS 1: VIRAL ASSIGNMENTS (taxonomy) #
#########################################

# Standard ranks from most specific to least, used to bucket the taxonomic
# distance between two assignments by the lowest rank at which they still agree.
# `realm` is the highest viral rank in NCBI (viruses have no superkingdom); the
# viral root `Viruses` (10239) sits above it with rank `acellular root`.
ORDERED_RANKS = (
    "species",
    "genus",
    "family",
    "order",
    "class",
    "phylum",
    "kingdom",
    "realm",
    "superkingdom",
)

ROOT_TAXID = 1

# The viral root `Viruses` in NCBI taxonomy. Its clade-count row holds a group's
# total viral reads, used as the denominator for clade family/order shares.
VIRUSES_TAXID = 10239

# Buckets used when two assignments share an ancestor but not at any standard
# rank: SHARED_HIGHER means they meet above the standard ranks (e.g. both under
# `Viruses` but in different realms); CROSS_ROOT means their only common
# ancestor is the tree root (e.g. a virus reassigned to a cellular organism).
SHARED_HIGHER = "shared-higher-taxon"
CROSS_ROOT = "cross-root"
# A taxid that is not present in the candidate-index taxonomy at all — e.g. a
# reference-side assignment whose taxid was merged or deleted by the time of the
# candidate-index taxonomy.
# This is a taxonomy-versioning artifact, NOT a severe biological reassignment,
# so it gets its own bucket rather than being lumped into cross-root.
UNRESOLVED_TAXID = "unresolved-taxid"


class TaxonomyTree:
    """NCBI taxonomy tree for taxonomic-distance calculations.

    Built from a parent map and rank map (parsed from taxonomy-nodes.dmp). All
    methods are pure functions of those maps; lineages are cached per taxid.
    """

    def __init__(self, parent: dict[int, int], rank: dict[int, str]) -> None:
        self.parent = parent
        self.rank = rank
        self._lineage_cache: dict[int, list[int]] = {}
        self._rank_anc_cache: dict[int, dict[str, int]] = {}

    def lineage(self, taxid: int) -> list[int]:
        """Ancestor chain from `taxid` up to the root (inclusive)."""
        if taxid in self._lineage_cache:
            return self._lineage_cache[taxid]
        chain: list[int] = []
        seen: set[int] = set()
        cur = taxid
        while cur not in seen:
            chain.append(cur)
            seen.add(cur)
            parent = self.parent.get(cur)
            if parent is None or parent == cur:
                break
            cur = parent
        self._lineage_cache[taxid] = chain
        return chain

    def rank_ancestors(self, taxid: int) -> dict[str, int]:
        """Map standard rank -> nearest ancestor taxid of that rank (or self)."""
        if taxid in self._rank_anc_cache:
            return self._rank_anc_cache[taxid]
        out: dict[str, int] = {}
        for anc in self.lineage(taxid):
            r = self.rank.get(anc)
            if r in ORDERED_RANKS and r not in out:
                out[r] = anc
        self._rank_anc_cache[taxid] = out
        return out

    def lca(self, a: int, b: int) -> int | None:
        """Lowest common ancestor of `a` and `b`, or None if no shared ancestor."""
        ancestors_a = set(self.lineage(a))
        for anc in self.lineage(b):
            if anc in ancestors_a:
                return anc
        return None

    def divergence_bucket(self, a: int, b: int) -> str:
        """Lowest rank at which assignments `a` and `b` still agree.

        Returns 'identical' when equal, or 'same-<rank>' for the lowest shared
        standard rank. When they share an ancestor only above the standard ranks
        (e.g. both under `Viruses` but different realms, or one is an ancestor of
        the other at an unranked node) returns 'shared-higher-taxon'. When their
        only common ancestor is the tree root (e.g. a virus reassigned to a
        cellular organism) returns 'cross-root'. When either taxid is absent from
        the taxonomy entirely (merged/deleted across index versions) returns
        'unresolved-taxid' — a versioning artifact, distinct from cross-root.
        """
        if a == b:
            return "identical"
        if a not in self.parent or b not in self.parent:
            return UNRESOLVED_TAXID
        ra = self.rank_ancestors(a)
        rb = self.rank_ancestors(b)
        for rank in ORDERED_RANKS:
            if rank in ra and ra[rank] == rb.get(rank):
                return f"same-{rank}"
        lca = self.lca(a, b)
        if lca is None or lca == ROOT_TAXID:
            return CROSS_ROOT
        return SHARED_HIGHER


def vertebrate_taxids(annotated_db: pd.DataFrame, host: str = "vertebrate") -> set[int]:
    """Taxids affirmatively marked as infecting `host` (status 1), with rollup.

    Mirrors the index's own surveillance predicate: a taxon counts if its
    infection_status_<host> is 1 (MATCH), or if its species-rollup taxon is.
    Status 3 ('likely') is intentionally excluded; see the report notes.

    Args:
        annotated_db: total-virus-db-annotated, with taxid, taxid_species, and
            infection_status_<host> columns.
        host: Host group name (default 'vertebrate').

    Returns:
        Set of integer taxids considered host-infecting.
    """
    col = f"infection_status_{host}"
    if col not in annotated_db.columns or "taxid" not in annotated_db.columns:
        return set()
    # Compare numerically, not as strings: if the column has any NA, pandas reads
    # it as float, so str values become "1.0" and a == "1" test would silently
    # match nothing (an empty vertebrate set with no error).
    status = pd.to_numeric(annotated_db[col], errors="coerce")
    positive = set(annotated_db.loc[status == 1, "taxid"].astype(int))
    if "taxid_species" in annotated_db.columns:
        species = annotated_db["taxid_species"].astype("Int64")
        rollup = annotated_db.loc[species.isin(positive), "taxid"].astype(int)
        positive.update(rollup)
    return positive


def join_read_assignments(
    reference_vh: pd.DataFrame,
    candidate_vh: pd.DataFrame,
    merge_map: dict[int, int] | None = None,
) -> pd.DataFrame:
    """Join per-read pipeline assignments across sides.

    Joins on (group, sample, seq_id) when a `sample` column is present on both
    sides, else (group, seq_id); raises on duplicate keys.

    Args:
        reference_vh: validation_hits for the reference run (needs group, seq_id,
            aligner_taxid_lca; sample used for the key when present).
        candidate_vh: validation_hits for the candidate run (same columns).
        merge_map: optional {old_taxid: canonical_taxid} from the candidate
            index's merged.dmp, applied to both sides so taxid renumbering across
            index versions is not counted as a reassignment.

    Returns:
        DataFrame: group, seq_id, taxid_reference, taxid_candidate, status, where
        status is 'lost' (reference only), 'gained' (candidate only), 'same'
        (shared, same taxid), or 'reassigned' (shared, different taxid).
    """
    # Include sample in the join key when available: seq_id is the instrument
    # query name, unique only within a sample, and a group can hold several
    # samples — so (group, seq_id) alone risks a many-to-many cartesian merge.
    if "sample" in reference_vh.columns and "sample" in candidate_vh.columns:
        key = ["group", "sample", "seq_id"]
    else:
        key = ["group", "seq_id"]
    m = reference_vh[[*key, "aligner_taxid_lca"]].rename(
        columns={"aligner_taxid_lca": "taxid_reference"}
    )
    d = candidate_vh[[*key, "aligner_taxid_lca"]].rename(
        columns={"aligner_taxid_lca": "taxid_candidate"}
    )
    for side, df in (("reference", m), ("candidate", d)):
        if df.duplicated(key).any():
            raise ValueError(
                f"Duplicate {key} rows in {side} validation_hits; cannot join "
                "reads unambiguously."
            )
    if merge_map:
        # Canonicalize taxids through the candidate index's merged.dmp so a read
        # that only changed because its taxid was merged across index versions is
        # not counted as a biological reassignment.
        m["taxid_reference"] = m["taxid_reference"].map(lambda t: merge_map.get(t, t))
        d["taxid_candidate"] = d["taxid_candidate"].map(lambda t: merge_map.get(t, t))
    merged = m.merge(d, on=key, how="outer", indicator=True)
    merged["taxid_reference"] = merged["taxid_reference"].astype("Int64")
    merged["taxid_candidate"] = merged["taxid_candidate"].astype("Int64")
    status = pd.Series("same", index=merged.index, dtype="object")
    status[merged["_merge"] == "left_only"] = "lost"
    status[merged["_merge"] == "right_only"] = "gained"
    both = merged["_merge"] == "both"
    # NA != value is <NA> in pandas; .fillna(True) so a malformed missing taxid on
    # a shared read is treated as reassigned, never silently "same".
    reassigned = both & (merged["taxid_reference"] != merged["taxid_candidate"]).fillna(
        True
    )
    status[reassigned] = "reassigned"
    merged["status"] = status
    # Keep sample in the output when it was part of the key, so repeated seq_id
    # values stay individually traceable in the per-read detail.
    out_cols = ["group", "seq_id", "taxid_reference", "taxid_candidate", "status"]
    if "sample" in key:
        out_cols.insert(1, "sample")
    return merged[out_cols]


def _add_vertebrate_flag(joined: pd.DataFrame, vert: set[int]) -> pd.DataFrame:
    """Add is_vertebrate: assigned taxid (either side) is vertebrate-infecting."""
    out = joined.copy()
    reference_vert = out["taxid_reference"].isin(vert)
    candidate_vert = out["taxid_candidate"].isin(vert)
    out["is_vertebrate"] = reference_vert | candidate_vert
    return out


def summarize_read_status(joined: pd.DataFrame, vert: set[int]) -> pd.DataFrame:
    """Per-group read-status counts, for all reads and the vertebrate subset.

    Returns:
        DataFrame: group, scope ('all'|'vertebrate'), n_reference, n_candidate,
        n_shared, n_same, n_reassigned, n_lost, n_gained, and three percentages
        with DIFFERENT denominators: pct_lost = lost/n_reference,
        pct_gained = gained/n_candidate, pct_reassigned = reassigned/n_shared.
    """
    flagged = _add_vertebrate_flag(joined, vert)
    all_groups = sorted(flagged["group"].unique())
    records: list[dict[str, object]] = []
    for scope in ("all", "vertebrate"):
        subset = flagged if scope == "all" else flagged[flagged["is_vertebrate"]]
        by_group = dict(list(subset.groupby("group")))
        # Iterate ALL groups (not just those with rows in this scope) so a group
        # with zero vertebrate reads still gets an explicit zero row.
        for group in all_groups:
            g = by_group.get(group)
            counts = (
                g["status"].value_counts() if g is not None else pd.Series(dtype=int)
            )
            n_lost = int(counts.get("lost", 0))
            n_gained = int(counts.get("gained", 0))
            n_same = int(counts.get("same", 0))
            n_reassigned = int(counts.get("reassigned", 0))
            n_reference = n_lost + n_same + n_reassigned
            n_candidate = n_gained + n_same + n_reassigned
            n_shared = n_same + n_reassigned
            records.append(
                {
                    "group": group,
                    "scope": scope,
                    "n_reference": n_reference,
                    "n_candidate": n_candidate,
                    "n_shared": n_shared,
                    "n_same": n_same,
                    "n_reassigned": n_reassigned,
                    "n_lost": n_lost,
                    "n_gained": n_gained,
                    "pct_lost": 100.0 * n_lost / n_reference if n_reference else None,
                    "pct_gained": (
                        100.0 * n_gained / n_candidate if n_candidate else None
                    ),
                    "pct_reassigned": (
                        100.0 * n_reassigned / n_shared if n_shared else None
                    ),
                }
            )
    return pd.DataFrame.from_records(
        records,
        columns=[
            "group",
            "scope",
            "n_reference",
            "n_candidate",
            "n_shared",
            "n_same",
            "n_reassigned",
            "n_lost",
            "n_gained",
            "pct_lost",
            "pct_gained",
            "pct_reassigned",
        ],
    )


def reassignment_distances(
    joined: pd.DataFrame, tax: TaxonomyTree, vert: set[int]
) -> pd.DataFrame:
    """Divergence bucket for each reassigned read.

    Computes the bucket once per distinct (taxid_reference, taxid_candidate) pair
    (cached in the tree) and joins back to reads.

    Returns:
        DataFrame of reassigned reads: group, scope, seq_id, taxid_reference,
        taxid_candidate, bucket. Emitted for scope 'all' and 'vertebrate'
        (vertebrate rows are a subset, re-labelled).
    """
    flagged = _add_vertebrate_flag(joined, vert)
    reassigned = flagged[flagged["status"] == "reassigned"].copy()
    if reassigned.empty:
        # Keep the sample-aware column set stable on the empty path too.
        cols = [
            "group",
            "scope",
            "seq_id",
            "taxid_reference",
            "taxid_candidate",
            "bucket",
        ]
        if "sample" in joined.columns:
            cols.insert(2, "sample")
        return pd.DataFrame(columns=cols)

    def _pair_key(a: Any, b: Any) -> tuple[int, int] | None:
        # A reassigned read with a missing taxid on either side (non-conformant
        # input — aligner_taxid_lca is schema-required) has no resolvable pair.
        if pd.isna(a) or pd.isna(b):
            return None
        return (int(a), int(b))

    pairs = reassigned[["taxid_reference", "taxid_candidate"]].drop_duplicates()
    bucket_map: dict[tuple[int, int] | None, str] = {None: UNRESOLVED_TAXID}
    for a, b in zip(pairs["taxid_reference"], pairs["taxid_candidate"], strict=True):
        key = _pair_key(a, b)
        if key is None:
            continue
        bucket_map[key] = tax.divergence_bucket(*key)
    keys = [
        _pair_key(a, b)
        for a, b in zip(
            reassigned["taxid_reference"], reassigned["taxid_candidate"], strict=True
        )
    ]
    reassigned["bucket"] = [bucket_map[k] for k in keys]

    frames = []
    for scope in ("all", "vertebrate"):
        sub = reassigned if scope == "all" else reassigned[reassigned["is_vertebrate"]]
        sub = sub.assign(scope=scope)
        frames.append(sub)
    out = pd.concat(frames, ignore_index=True)
    cols = [
        "group",
        "scope",
        "seq_id",
        "taxid_reference",
        "taxid_candidate",
        "bucket",
    ]
    if "sample" in out.columns:
        cols.insert(2, "sample")
    return out[cols]


def bucket_summary(reassignment_detail: pd.DataFrame) -> pd.DataFrame:
    """Counts of reassigned reads per (scope, bucket), all buckets shown.

    Every canonical bucket is emitted for each scope (0 when none) so a reader can
    tell "0 reads" from "not checked" — e.g. a 0 in `unresolved-taxid` is the
    reassuring result that no assignment used a taxid missing from the
    candidate-index taxonomy. unresolved-taxid sits at the FRONT, outside the
    same-species ->
    cross-root biological severity gradient (placing it after cross-root would
    wrongly read as the most severe category).

    Returns:
        DataFrame: scope, bucket, n_reads.
    """
    # 'identical' is excluded: reassigned reads by definition are not identical.
    display_buckets = [
        UNRESOLVED_TAXID,
        *(f"same-{r}" for r in ORDERED_RANKS),
        SHARED_HIGHER,
        CROSS_ROOT,
    ]
    # Always emit BOTH scopes for every canonical bucket, so "none observed"
    # (e.g. zero vertebrate reassignments even when 'all' has some) stays
    # distinguishable from "not computed".
    if reassignment_detail.empty:
        counts: dict[Any, int] = {}
    else:
        counts = reassignment_detail.groupby(["scope", "bucket"]).size().to_dict()
    records: list[dict[str, object]] = []
    for scope in ("all", "vertebrate"):
        for bucket in display_buckets:
            records.append(
                {
                    "scope": scope,
                    "bucket": bucket,
                    "n_reads": int(counts.get((scope, bucket), 0)),
                }
            )
    return pd.DataFrame.from_records(records, columns=["scope", "bucket", "n_reads"])


def reassignment_concentration(reassignment_detail: pd.DataFrame) -> pd.DataFrame:
    """How concentrated each group's reassignments are in a few taxid pairs.

    A high read-level reassignment % can come from one systematic taxid remap
    counted across many (possibly duplicate) reads. This reports, per
    (group, scope): the reassigned read count, the number of distinct
    (taxid_reference, taxid_candidate) pairs, the top pair, and the fraction of
    reassigned
    reads it accounts for, so a reviewer can tell broad instability from a single
    clade-wide LCA shift.

    Returns:
        DataFrame: group, scope, n_reassigned, n_distinct_pairs, top_pair,
        top_pair_reads, top_pair_frac.
    """
    cols = [
        "group",
        "scope",
        "n_reassigned",
        "n_distinct_pairs",
        "top_pair",
        "top_pair_reads",
        "top_pair_frac",
    ]
    if reassignment_detail.empty:
        return pd.DataFrame(columns=cols)
    records: list[dict[str, object]] = []
    for (group, scope), sub in reassignment_detail.groupby(["group", "scope"]):
        # dropna=False so reads with a missing taxid (non-conformant input) are
        # still counted as their own pair rather than silently dropped.
        pair_counts = sub.groupby(
            ["taxid_reference", "taxid_candidate"], dropna=False
        ).size()
        n = int(pair_counts.sum())
        if pair_counts.empty:
            continue
        top_reference, top_candidate = cast(tuple[object, object], pair_counts.idxmax())
        top_reads = int(pair_counts.max())

        def _fmt(t: Any) -> str:
            return "NA" if pd.isna(t) else str(int(t))

        records.append(
            {
                "group": group,
                "scope": scope,
                "n_reassigned": n,
                "n_distinct_pairs": int(pair_counts.size),
                "top_pair": f"{_fmt(top_reference)}->{_fmt(top_candidate)}",
                "top_pair_reads": top_reads,
                "top_pair_frac": top_reads / n if n else None,
            }
        )
    return (
        pd.DataFrame.from_records(records, columns=cols)
        .sort_values(["scope", "group"])
        .reset_index(drop=True)
    )


def reassignment_pair_counts(reassignment_detail: pd.DataFrame) -> pd.DataFrame:
    """Per-(group, scope, taxid pair) reassigned-read counts with bucket.

    A compact aggregate of the reassignment detail: one row per distinct
    (group, scope, taxid_reference, taxid_candidate) with its bucket and read
    count. Unlike `reassignment_concentration` (top pair per group only), this
    keeps EVERY pair, so the report can name example pairs for any bucket — e.g. a
    severe cross-root or shared-higher-taxon pair that is not a group's single top
    pair.

    Returns:
        DataFrame: group, scope, taxid_reference, taxid_candidate, bucket,
        n_reads, sorted by group, scope, bucket, n_reads (desc). Empty
        (header-only) when there are no reassigned reads.
    """
    cols = [
        "group",
        "scope",
        "taxid_reference",
        "taxid_candidate",
        "bucket",
        "n_reads",
    ]
    if reassignment_detail.empty:
        return pd.DataFrame(columns=cols)
    counts = (
        # dropna=False so a pair with a missing taxid (non-conformant input) is
        # still counted rather than silently dropped.
        reassignment_detail.groupby(
            ["group", "scope", "taxid_reference", "taxid_candidate", "bucket"],
            dropna=False,
        )
        .size()
        .reset_index(name="n_reads")
    )
    return counts.sort_values(
        ["group", "scope", "bucket", "n_reads"],
        ascending=[True, True, True, False],
    ).reset_index(drop=True)[cols]


def clade_rank_shares(
    clade_reference: pd.DataFrame,
    clade_candidate: pd.DataFrame,
    rank_map: dict[int, str],
    name_map: dict[int, str],
    rank_levels: tuple[str, ...] = ("family", "order"),
    count_cols: tuple[str, ...] = ("reads_clade_total", "reads_clade_dedup"),
) -> pd.DataFrame:
    """Compare the high-level taxonomic breakdown of clade counts.

    For each rank level (family, order), the clade-count row at a rank-level
    taxon already holds that clade's total reads, so we filter to those rows and
    compute each taxon's share PER GROUP, on both sides. The denominator is the
    group's TOTAL viral reads — the count on its Viruses-root row (taxid 10239),
    per side, per count column — NOT a within-rank sum over the family/order rows.
    Within-rank normalization was removed because it mechanically inflated the
    surviving families when another family vanished (its denominator shrank),
    reporting a positive share change for a family whose raw count was unchanged
    or falling. A total-viral denominator moves only when a clade's own reads or
    the group's total viral reads move, so the sign of `delta_pp` is meaningful.

    Rank is looked up from the candidate index's full NCBI taxonomy (nodes.dmp); a
    taxid deleted from the candidate-index taxonomy drops from this table. Raw
    counts (`reads_reference`, `reads_candidate`, `delta_reads`) are reported
    alongside the shares so a reviewer can read the absolute change directly.

    Args:
        clade_reference: clade_counts for the reference run (group, taxid,
            reads_clade_* columns).
        clade_candidate: clade_counts for the candidate run.
        rank_map: taxid -> rank from the candidate-index taxonomy (complete).
        name_map: taxid -> name (from the candidate index's annotated DB; taxids
            absent from it fall back to their stringified taxid).
        rank_levels: taxonomic ranks to roll up to.
        count_cols: which clade-count columns to compute shares for.

    Returns:
        Long DataFrame: group, rank_level, count_type, taxid, name,
        reads_reference, reads_candidate, delta_reads (candidate - reference),
        share_reference, share_candidate (each a share of the group's total viral
        reads), delta_pp (share change in pp). If a group has no Viruses-root
        (10239) row, its denominator is missing and the shares (and delta_pp) are
        NaN for that group.
    """

    def viral_totals(df: pd.DataFrame, count_col: str) -> dict[object, float]:
        """Group -> total viral reads (the Viruses-root 10239 row), per count_col."""
        root = df[df["taxid"].astype(int) == VIRUSES_TAXID]
        return root.groupby("group")[count_col].sum().to_dict()

    def side_shares(df: pd.DataFrame, rank_level: str, count_col: str) -> pd.DataFrame:
        d = df[["group", "taxid", count_col]].copy()
        d["rk"] = d["taxid"].astype(int).map(rank_map)
        sub = d[d["rk"] == rank_level].copy()
        if sub.empty:
            return sub.assign(share=pd.Series(dtype=float))[
                ["group", "taxid", count_col, "share"]
            ]
        totals = viral_totals(df, count_col)
        # Missing root -> NaN denominator -> NaN share (surfaced, not silently 0).
        denom = sub["group"].map(totals)
        sub["share"] = sub[count_col].div(denom.where(denom != 0))
        return sub[["group", "taxid", count_col, "share"]]

    frames: list[pd.DataFrame] = []
    for rank_level in rank_levels:
        for count_col in count_cols:
            # Groups that have a (nonzero) Viruses-root on each side: a family
            # absent from such a group has share 0, but in a group with NO root the
            # share is genuinely undefined (NaN), so the two cases stay distinct.
            reference_root_groups = {
                g for g, v in viral_totals(clade_reference, count_col).items() if v
            }
            candidate_root_groups = {
                g for g, v in viral_totals(clade_candidate, count_col).items() if v
            }
            a = side_shares(clade_reference, rank_level, count_col).rename(
                columns={count_col: "reads_reference", "share": "share_reference"}
            )
            b = side_shares(clade_candidate, rank_level, count_col).rename(
                columns={count_col: "reads_candidate", "share": "share_candidate"}
            )
            merged = a.merge(b, on=["group", "taxid"], how="outer")
            # Raw counts default to 0 for a side where the family is absent.
            for col in ("reads_reference", "reads_candidate"):
                merged[col] = merged[col].fillna(0.0)
            # Fill an absent family's share with 0 only when its group HAS a root on
            # that side; leave NaN (no-root) groups NaN so they stay surfaced.
            reference_has_root = merged["group"].isin(reference_root_groups)
            candidate_has_root = merged["group"].isin(candidate_root_groups)
            merged.loc[
                merged["share_reference"].isna() & reference_has_root, "share_reference"
            ] = 0.0
            merged.loc[
                merged["share_candidate"].isna() & candidate_has_root, "share_candidate"
            ] = 0.0
            merged["name"] = (
                merged["taxid"]
                .astype(int)
                .map(name_map)
                .fillna(merged["taxid"].astype(str))
            )
            merged["rank_level"] = rank_level
            merged["count_type"] = count_col
            merged["delta_reads"] = (
                merged["reads_candidate"] - merged["reads_reference"]
            )
            merged["delta_pp"] = (
                merged["share_candidate"] - merged["share_reference"]
            ) * 100.0
            frames.append(merged)
    out = pd.concat(frames, ignore_index=True)
    return (
        out[
            [
                "group",
                "rank_level",
                "count_type",
                "taxid",
                "name",
                "reads_reference",
                "reads_candidate",
                "delta_reads",
                "share_reference",
                "share_candidate",
                "delta_pp",
            ]
        ]
        .sort_values(["rank_level", "count_type", "group", "delta_pp"])
        .reset_index(drop=True)
    )


def validation_agreement(vh: pd.DataFrame) -> pd.DataFrame:
    """Per-group BLAST-validation agreement summary for one side (secondary).

    A read is 'validated' when validation_distance_aligner is non-null; among
    validated reads, agreement means a taxonomic distance of 0 between the
    pipeline (aligner) assignment and the BLAST validation LCA.

    Args:
        vh: validation_hits with group and validation_distance_aligner columns.

    Returns:
        DataFrame: group, n_reads, n_validated, frac_validated, agreement_rate
        (fraction of validated reads with distance 0), mean_distance.
    """
    records: list[dict[str, object]] = []
    dist = pd.to_numeric(vh["validation_distance_aligner"], errors="coerce")
    vh = vh.assign(_dist=dist)
    for group, g in vh.groupby("group"):
        n_reads = len(g)
        validated = g["_dist"].notna()
        n_validated = int(validated.sum())
        agree = int((g.loc[validated, "_dist"] == 0).sum())
        records.append(
            {
                "group": group,
                "n_reads": n_reads,
                "n_validated": n_validated,
                "frac_validated": n_validated / n_reads if n_reads else None,
                "agreement_rate": agree / n_validated if n_validated else None,
                "mean_distance": (
                    g.loc[validated, "_dist"].mean() if n_validated else None
                ),
            }
        )
    return pd.DataFrame.from_records(
        records,
        columns=[
            "group",
            "n_reads",
            "n_validated",
            "frac_validated",
            "agreement_rate",
            "mean_distance",
        ],
    )


def validation_agreement_by_taxon(vh: pd.DataFrame) -> pd.DataFrame:
    """Per-(group, aligner taxon) BLAST-validation agreement for one side.

    Breaks the per-group `validation_agreement` down by the pipeline's assigned
    taxon (`aligner_taxid_lca`), so a group-level agreement-rate change can be
    localized to the taxa driving it ("which taxa are most affected, and how far
    off are the new disagreements"). A read is 'validated' when
    validation_distance_aligner is non-null; agreement means a taxonomic distance
    of 0 to the BLAST validation LCA, and a larger mean distance is a worse
    disagreement.

    This groups by taxon WITHIN each side independently (BLAST validation is a
    per-side measurement), mirroring `validation_agreement`; the caller merges the
    two sides on (group, taxid) to get the per-taxon delta.

    Args:
        vh: validation_hits with group, aligner_taxid_lca, and
            validation_distance_aligner columns.

    Returns:
        DataFrame: group, taxid (the aligner_taxid_lca), n_reads, n_validated,
        agreement_rate (fraction of validated reads with distance 0),
        mean_distance (over ALL validated reads, agreements included), and
        mean_distance_disagree (over only the disagreeing reads, distance > 0).
        Use `mean_distance_disagree` for "how far off are the disagreements" —
        `mean_distance` is diluted toward 0 when agreement is high. Empty
        (header-only) when no rows.
    """
    cols = [
        "group",
        "taxid",
        "n_reads",
        "n_validated",
        "agreement_rate",
        "mean_distance",
        "mean_distance_disagree",
    ]
    if vh.empty or "aligner_taxid_lca" not in vh.columns:
        return pd.DataFrame(columns=cols)
    dist = pd.to_numeric(vh["validation_distance_aligner"], errors="coerce")
    vh = vh.assign(_dist=dist)
    records: list[dict[str, object]] = []
    for (group, taxid), g in vh.groupby(["group", "aligner_taxid_lca"]):
        n_reads = len(g)
        validated = g["_dist"].notna()
        n_validated = int(validated.sum())
        validated_dist = g.loc[validated, "_dist"]
        agree = int((validated_dist == 0).sum())
        disagree_dist = validated_dist[validated_dist > 0]
        records.append(
            {
                "group": group,
                "taxid": int(cast(Any, taxid)),
                "n_reads": n_reads,
                "n_validated": n_validated,
                "agreement_rate": agree / n_validated if n_validated else None,
                "mean_distance": validated_dist.mean() if n_validated else None,
                "mean_distance_disagree": (
                    disagree_dist.mean() if not disagree_dist.empty else None
                ),
            }
        )
    return pd.DataFrame.from_records(records, columns=cols)


def vertebrate_status_flips(
    old_annotated: pd.DataFrame,
    new_annotated: pd.DataFrame,
    host: str = "vertebrate",
) -> pd.DataFrame:
    """Taxa whose host-infecting membership changed between two index annotations.

    Separates a TRUE status flip (a taxon present in BOTH annotated DBs whose
    `infection_status_<host>` changed) from a presence change (a taxon added to or
    removed from the annotated DB entirely). Conflating them is wrong: only a true
    flip is evidence of a re-annotation, whereas an added/removed taxon reflects a
    genome being added to or dropped from the index. The `change` vocabulary:
    - `gained_<host>` / `lost_<host>` — present in both DBs, status crossed 1 (a
      genuine re-annotation).
    - `added_<host>` — present only in the candidate (new) DB and host-infecting
      there (a newly added taxon, not a flip).
    - `removed_<host>` — present only in the reference (old) DB where it was
      host-infecting (dropped from the candidate DB).

    Args:
        old_annotated: annotated viral DB from the reference index.
        new_annotated: annotated viral DB from the candidate index.
        host: host group name.

    Returns:
        DataFrame: taxid, name, change (one of the four values above).
    """
    old_pos = vertebrate_taxids(old_annotated, host)
    new_pos = vertebrate_taxids(new_annotated, host)
    # Full taxid universe of each annotated DB, to tell a status flip (taxon in
    # both) from an added/removed taxon (taxon in only one).
    old_taxids = set(old_annotated["taxid"].astype(int))
    new_taxids = set(new_annotated["taxid"].astype(int))
    namemap = dict(
        zip(new_annotated["taxid"].astype(int), new_annotated["name"], strict=True)
    )
    namemap.update(
        dict(
            zip(
                old_annotated["taxid"].astype(int),
                old_annotated["name"],
                strict=True,
            )
        )
    )
    records: list[dict[str, object]] = []
    for taxid in sorted(new_pos - old_pos):
        # Host-infecting in candidate but not reference: a true gain only if the
        # taxon also existed in the reference DB (else it is newly added).
        change = f"gained_{host}" if taxid in old_taxids else f"added_{host}"
        records.append({"taxid": taxid, "name": namemap.get(taxid), "change": change})
    for taxid in sorted(old_pos - new_pos):
        # Host-infecting in reference but not candidate: a true loss only if the
        # taxon still exists in the candidate DB (else it was removed entirely).
        change = f"lost_{host}" if taxid in new_taxids else f"removed_{host}"
        records.append({"taxid": taxid, "name": namemap.get(taxid), "change": change})
    return pd.DataFrame.from_records(records, columns=["taxid", "name", "change"])


#########################################
# FLAGGING (fixed thresholds)           #
#########################################

# Default thresholds for flagging a difference as worth human review. All are
# exposed as CLI flags; they are deliberate judgment calls, documented in the
# skill. A flag is advisory -- the report always shows the underlying numbers.
DEFAULT_THRESHOLDS: dict[str, float] = {
    "read_survival_pp": 5.0,  # |pp| change in raw->cleaned read-survival fraction
    "qc_pct_change": 10.0,  # |%| change in other qc metrics
    "bray_curtis": 0.15,  # kraken whole-profile dissimilarity
    "viral_pct_lost": 2.0,  # |%| of vertebrate-viral reads lost
    "viral_pct_gained": 25.0,  # |%| of candidate vertebrate-viral reads that are new
    "viral_pct_reassigned": 10.0,  # |%| of shared reads reassigned
    "clade_share_pp": 3.0,  # |pp| change in a family/order share
    "validation_agreement_drop": 0.10,  # drop in BLAST-agreement rate
}


def _flag_records(
    df: pd.DataFrame,
    value_col: str,
    threshold: float,
    focus: str,
    metric: str,
    key_cols: list[str],
    direction: str = "abs",
) -> list[dict[str, object]]:
    """Build flag records for rows whose value trips the fixed threshold.

    Args:
        df: Source comparison table.
        value_col: Column holding the magnitude to test.
        threshold: Fixed threshold for the magnitude.
        focus: Report focus label (e.g. 'kraken').
        metric: Human-readable metric name.
        key_cols: Columns identifying the flagged row (e.g. group, rank).
        direction: 'abs' flags |value| > threshold (two-sided); 'pos' flags
            value > threshold (one-sided, for already-signed magnitudes like a
            dissimilarity or an agreement-rate drop).

    Returns:
        List of flag dicts (focus, key, metric, value, threshold, flag_type).
        flag_type is always 'fixed'.
    """
    if df.empty or value_col not in df.columns:
        return []
    work = df.copy()
    vals = pd.to_numeric(work[value_col], errors="coerce")
    compare = vals.abs() if direction == "abs" else vals
    fixed = (compare > threshold).fillna(False)
    records: list[dict[str, object]] = []
    for idx in work.index[fixed]:
        key = ", ".join(f"{c}={work.at[idx, c]}" for c in key_cols)
        records.append(
            {
                "focus": focus,
                "key": key,
                "metric": metric,
                "value": vals[idx],
                "threshold": threshold,
                "flag_type": "fixed",
            }
        )
    return records


def build_flags(
    outputs: dict[str, pd.DataFrame],
    thresholds: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Assemble the consolidated flags table across focuses.

    Applies fixed thresholds to the quantitative comparison tables. A flag is
    advisory -- the report always shows the underlying numbers.

    Args:
        outputs: Mapping of table name -> comparison DataFrame, using the names
            written by compare_downstream_runs.py.
        thresholds: Override thresholds (falls back to DEFAULT_THRESHOLDS).

    Returns:
        DataFrame: focus, key, metric, value, threshold, flag_type.
    """
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    records: list[dict[str, object]] = []

    survival = outputs.get("qc_survival")
    if survival is not None and not survival.empty:
        records += _flag_records(
            survival,
            "delta_pp",
            t["read_survival_pp"],
            "qc",
            "raw->cleaned read survival change (pp)",
            ["group", "sample"],
            "abs",
        )

    qc = outputs.get("qc_numeric")
    if qc is not None and not qc.empty:
        others = qc[
            ~qc["metric"].isin(["n_reads_single", "n_read_pairs", "n_bases_approx"])
        ]
        records += _flag_records(
            others,
            "pct_change",
            t["qc_pct_change"],
            "qc",
            "qc metric (%)",
            ["group", "sample", "stage", "metric"],
            "abs",
        )

    # FASTQC flag transitions: flag only WORSENING moves (pass < warn < fail), so a
    # pass->fail cannot slip past the deterministic Main-findings coverage rule. An
    # improvement (e.g. warn->pass) changes the flag table but is not flagged.
    qc_flags = outputs.get("qc_flag_changes")
    if qc_flags is not None and not qc_flags.empty:
        rank = {"pass": 0, "warn": 1, "fail": 2}
        for idx in qc_flags.index:
            ref_rank = rank.get(str(qc_flags.at[idx, "reference_flag"]).lower())
            cand_rank = rank.get(str(qc_flags.at[idx, "candidate_flag"]).lower())
            if ref_rank is None or cand_rank is None or cand_rank <= ref_rank:
                continue
            key = ", ".join(
                f"{c}={qc_flags.at[idx, c]}"
                for c in ("group", "sample", "stage", "check")
            )
            records.append(
                {
                    "focus": "qc",
                    "key": key,
                    "metric": "FASTQC flag worsened (pass<warn<fail)",
                    "value": (
                        f"{qc_flags.at[idx, 'reference_flag']}->"
                        f"{qc_flags.at[idx, 'candidate_flag']}"
                    ),
                    "threshold": "any worsening",
                    "flag_type": "fixed",
                }
            )

    bc = outputs.get("kraken_bray_curtis")
    if bc is not None and not bc.empty:
        records += _flag_records(
            bc,
            "bray_curtis",
            t["bray_curtis"],
            "kraken",
            "Bray-Curtis dissimilarity",
            ["group", "rank", "ribosomal"],
            "pos",
        )

    status = outputs.get("viral_read_status")
    if status is not None and not status.empty:
        vert = status[status["scope"] == "vertebrate"]
        records += _flag_records(
            vert,
            "pct_lost",
            t["viral_pct_lost"],
            "viral",
            "vertebrate-viral reads lost (%)",
            ["group"],
            "pos",
        )
        records += _flag_records(
            vert,
            "pct_gained",
            t["viral_pct_gained"],
            "viral",
            "vertebrate-viral reads gained (%)",
            ["group"],
            "pos",
        )
        records += _flag_records(
            vert,
            "pct_reassigned",
            t["viral_pct_reassigned"],
            "viral",
            "vertebrate-viral reads reassigned (%)",
            ["group"],
            "pos",
        )

    clade = outputs.get("clade_rank_shares")
    if clade is not None and not clade.empty:
        total = clade[clade["count_type"] == "reads_clade_total"]
        records += _flag_records(
            total,
            "delta_pp",
            t["clade_share_pp"],
            "viral",
            "clade share change (pp)",
            ["group", "rank_level", "name"],
            "abs",
        )

    val = outputs.get("viral_validation_agreement")
    if val is not None and not val.empty and "agreement_rate_reference" in val.columns:
        v = val.copy()
        v["agreement_drop"] = (
            v["agreement_rate_reference"] - v["agreement_rate_candidate"]
        )
        records += _flag_records(
            v,
            "agreement_drop",
            t["validation_agreement_drop"],
            "viral",
            "BLAST-agreement rate drop",
            ["group"],
            "pos",
        )

    return pd.DataFrame.from_records(
        records,
        columns=["focus", "key", "metric", "value", "threshold", "flag_type"],
    )
