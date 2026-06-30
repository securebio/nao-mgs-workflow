#!/usr/bin/env python3
"""Pure calculations for compare_downstream_runs.py."""

from dataclasses import dataclass, field
from typing import Any, cast

import pandas as pd


@dataclass
class FileEntry:
    """One discovered per-group output file."""

    n_rows: int | None = None
    columns: list[str] | None = None


@dataclass
class GroupManifest:
    """Discovered files and inferred platform for one group."""

    platform: str
    files: dict[str, FileEntry] = field(default_factory=dict)


SideManifest = dict[str, GroupManifest]


def compare_file_inventory(
    reference: SideManifest,
    candidate: SideManifest,
    expected_types: dict[str, set[str]] | None = None,
) -> pd.DataFrame:
    """Compare file presence and row counts, including expected missing files."""
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
            rows_reference = fe_reference.n_rows if fe_reference else None
            rows_candidate = fe_candidate.n_rows if fe_candidate else None
            if rows_reference is not None and rows_candidate is not None:
                row_delta = rows_candidate - rows_reference
                row_pct_change = (
                    100.0 * row_delta / rows_reference if rows_reference else None
                )
            else:
                row_delta = None
                row_pct_change = None
            records.append(
                {
                    "group": group,
                    "platform": platform,
                    "file_type": ft,
                    "in_reference": fe_reference is not None,
                    "in_candidate": fe_candidate is not None,
                    "n_rows_reference": rows_reference,
                    "n_rows_candidate": rows_candidate,
                    "row_delta": row_delta,
                    "row_pct_change": row_pct_change,
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
    for col in ("n_rows_reference", "n_rows_candidate", "row_delta"):
        df[col] = df[col].astype("Int64")
    return df


def _columns_consistent(manifest: SideManifest, file_type: str) -> bool:
    """Whether groups of the same platform share one header for `file_type`."""
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
    """Aggregate missing/extra schema fields over every observed header."""
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
    """Compare observed headers with schemas and within-platform consistency."""
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
    """Compare numeric QC metrics per (group, sample, stage)."""
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
    """Return changed FASTQC flags per (group, sample, stage, check)."""
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
    """Compare each run's cleaned/raw read fraction in percentage points."""

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


# Kraken ribosomal/non-ribosomal read sets are compared separately; abundance is
# compared at these rank codes by default.
KRAKEN_RANKS = ("G", "S")


def kraken_relative_abundance(df: pd.DataFrame, rank: str) -> pd.DataFrame:
    """Compute rank-level abundance per (group, ribosomal); drop zero-total sets."""
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
    """Compute Bray-Curtis dissimilarity per (group, ribosomal, rank)."""
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
    """Return the top `n` absolute abundance changes per group/read set."""
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
    """NCBI taxonomy tree with cached lineages and rank ancestors."""

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
        """Return identical, same-rank, shared-higher, cross-root, or unresolved."""
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
    """Return status-1 host taxids plus rows rolling up to positive species."""
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
) -> pd.DataFrame:
    """Join read assignments and classify them as same/reassigned/lost/gained."""
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
    """Summarize statuses for all/vertebrate scopes using metric-specific denominators."""
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


def bucket_summary(reassignment_pairs: pd.DataFrame) -> pd.DataFrame:
    """Count reads per scope/bucket, emitting zero rows for every bucket."""
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
    if reassignment_pairs.empty:
        counts: dict[Any, int] = {}
    else:
        counts = (
            reassignment_pairs.groupby(["scope", "bucket"])["n_reads"].sum().to_dict()
        )
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


def reassignment_pair_counts(
    joined: pd.DataFrame, tax: TaxonomyTree, vert: set[int]
) -> pd.DataFrame:
    """Aggregate reassigned reads by group/scope/taxid pair with count and fraction."""
    cols = [
        "group",
        "scope",
        "taxid_reference",
        "taxid_candidate",
        "bucket",
        "n_reads",
        "pair_frac",
    ]
    if joined.empty:
        return pd.DataFrame(columns=cols)
    reassigned = _add_vertebrate_flag(joined, vert)
    reassigned = reassigned[reassigned["status"] == "reassigned"]
    if reassigned.empty:
        return pd.DataFrame(columns=cols)

    frames: list[pd.DataFrame] = []
    for scope in ("all", "vertebrate"):
        sub = reassigned if scope == "all" else reassigned[reassigned["is_vertebrate"]]
        counts = (
            sub.groupby(["group", "taxid_reference", "taxid_candidate"], dropna=False)
            .size()
            .reset_index(name="n_reads")
        )
        if not counts.empty:
            counts["scope"] = scope
            frames.append(counts)
    if not frames:
        return pd.DataFrame(columns=cols)

    out = pd.concat(frames, ignore_index=True)

    def bucket(a: Any, b: Any) -> str:
        if pd.isna(a) or pd.isna(b):
            return UNRESOLVED_TAXID
        return tax.divergence_bucket(int(a), int(b))

    out["bucket"] = [
        bucket(a, b)
        for a, b in zip(out["taxid_reference"], out["taxid_candidate"], strict=True)
    ]
    totals = out.groupby(["group", "scope"])["n_reads"].transform("sum")
    out["pair_frac"] = out["n_reads"] / totals
    return out.sort_values(
        ["group", "scope", "n_reads", "taxid_reference", "taxid_candidate"],
        ascending=[True, True, False, True, True],
        na_position="last",
    ).reset_index(drop=True)[cols]


def clade_rank_shares(
    clade_reference: pd.DataFrame,
    clade_candidate: pd.DataFrame,
    rank_map: dict[int, str],
    name_map: dict[int, str],
    rank_levels: tuple[str, ...] = ("family", "order"),
    count_cols: tuple[str, ...] = ("reads_clade_total", "reads_clade_dedup"),
) -> pd.DataFrame:
    """Compare family/order raw counts and shares of each side's Viruses-root total.

    A missing family is zero only when that side has a nonzero Viruses-root row;
    without a root, its share remains undefined.
    """

    def viral_totals(df: pd.DataFrame, count_col: str) -> dict[object, float]:
        """Group -> total viral reads (the Viruses-root 10239 row)."""
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
            for col in ("reads_reference", "reads_candidate"):
                merged[col] = merged[col].fillna(0.0)
            reference_has_root = merged["group"].isin(reference_root_groups)
            candidate_has_root = merged["group"].isin(candidate_root_groups)
            merged.loc[
                merged["share_reference"].isna() & reference_has_root,
                "share_reference",
            ] = 0.0
            merged.loc[
                merged["share_candidate"].isna() & candidate_has_root,
                "share_candidate",
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
    """Summarize validated fraction and distance-zero agreement by group."""
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
        ],
    )


def validation_agreement_by_taxon(vh: pd.DataFrame) -> pd.DataFrame:
    """Summarize agreement and disagreement-only distance by group/aligner taxon."""
    cols = [
        "group",
        "taxid",
        "n_reads",
        "n_validated",
        "agreement_rate",
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
    """Classify host membership changes as gained/lost status or added/removed taxa."""
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
    """Build fixed-threshold flag records; `direction` is `abs` or `pos`."""
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
    """Assemble the consolidated fixed-threshold flags table."""
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
