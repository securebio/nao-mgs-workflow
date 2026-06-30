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
    merged["abs_delta_pp"] = merged["delta_pp"].abs()
    merged["rank"] = rank
    # taxid tiebreaker so the top-n cutoff is deterministic when taxa tie on
    # abs_diff right at the boundary.
    ordered = merged.sort_values(
        ["abs_diff", "taxid"], ascending=[False, True], kind="stable"
    )
    top = ordered.groupby(
        ["group", "ribosomal"], as_index=False, group_keys=False
    ).head(n)
    out = top.sort_values(
        ["group", "ribosomal", "abs_diff"], ascending=[True, True, False]
    ).reset_index(drop=True)
    # mover_rank: 1 = largest |Δpp| within (group, ribosomal) for this rank, so
    # the dominant mover is `mover_rank == 1` rather than a manual sort the reader
    # has to redo (and can get wrong by reading a smaller change first).
    out["mover_rank"] = out.groupby(["group", "ribosomal"]).cumcount() + 1
    return out[
        [
            "group",
            "ribosomal",
            "rank",
            "taxid",
            "name",
            "pct_reference",
            "pct_candidate",
            "delta_pp",
            "abs_delta_pp",
            "mover_rank",
        ]
    ]


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


def _dominant_taxon(
    sub: pd.DataFrame, taxid_col: str, name_map: dict[int, str] | None
) -> tuple[object, object, object, object]:
    """Most frequent taxon in `sub`: (taxid, name, reads, frac of `sub`).

    Returns Nones when `sub` is empty so a group with no lost/gained reads still
    emits an explicit empty driver rather than dropping the row.
    """
    if sub.empty:
        return (None, None, None, None)
    counts = sub[taxid_col].dropna().astype(int).value_counts()
    if counts.empty:
        return (None, None, None, None)
    taxid = int(counts.index[0])
    reads = int(counts.iloc[0])
    name = (name_map or {}).get(taxid, str(taxid))
    return (taxid, name, reads, reads / len(sub))


def summarize_read_status(
    joined: pd.DataFrame,
    vert: set[int],
    name_map: dict[int, str] | None = None,
) -> pd.DataFrame:
    """Summarize statuses for all/vertebrate scopes using metric-specific denominators.

    Also records the single dominant lost/gained taxon per group/scope so a
    concentrated turnover (e.g. one newly added species driving a gain) is a
    one-row lookup rather than a manual per-read join. `name_map` resolves the
    driver taxid to a name; absent it, the taxid string is used.
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
            empty = flagged.iloc[:0]
            gained_rows = g[g["status"] == "gained"] if g is not None else empty
            lost_rows = g[g["status"] == "lost"] if g is not None else empty
            dg_taxid, dg_name, dg_reads, dg_frac = _dominant_taxon(
                gained_rows, "taxid_candidate", name_map
            )
            dl_taxid, dl_name, dl_reads, dl_frac = _dominant_taxon(
                lost_rows, "taxid_reference", name_map
            )
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
                    "dominant_gained_taxid": dg_taxid,
                    "dominant_gained_name": dg_name,
                    "dominant_gained_reads": dg_reads,
                    "dominant_gained_frac": dg_frac,
                    "dominant_lost_taxid": dl_taxid,
                    "dominant_lost_name": dl_name,
                    "dominant_lost_reads": dl_reads,
                    "dominant_lost_frac": dl_frac,
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
            "dominant_gained_taxid",
            "dominant_gained_name",
            "dominant_gained_reads",
            "dominant_gained_frac",
            "dominant_lost_taxid",
            "dominant_lost_name",
            "dominant_lost_reads",
            "dominant_lost_frac",
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
        "is_severe",
        "is_dominant",
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
    # is_severe flags the buckets the coverage rule requires a finding for even
    # below the per-group reassignment threshold (a read leaving the viral tree
    # or meeting another assignment only above the standard ranks).
    out["is_severe"] = out["bucket"].isin([CROSS_ROOT, SHARED_HIGHER])
    out = out.sort_values(
        ["group", "scope", "n_reads", "taxid_reference", "taxid_candidate"],
        ascending=[True, True, False, True, True],
        na_position="last",
    ).reset_index(drop=True)
    # is_dominant marks the largest pair per (group, scope) (rows are n_reads-desc
    # within each), so the headline remap is a one-row lookup.
    out["is_dominant"] = out.groupby(["group", "scope"]).cumcount() == 0
    return out[cols]


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
            # reaches_zero: present on the reference side but gone in the
            # candidate. The coverage rule requires a finding for every clade
            # reaching zero candidate share, even below the share threshold, so
            # flagging it here keeps small-count drops (a few reads) from being
            # silently skipped. Require a real candidate denominator
            # (`candidate_has_root`): without a candidate Viruses-root row the
            # zero candidate count is "no candidate clade data for this group",
            # not a genuine drop, and would be a false positive.
            merged["reaches_zero"] = (
                (merged["reads_reference"] > 0)
                & (merged["reads_candidate"] == 0)
                & candidate_has_root
            )
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
                "reaches_zero",
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


_AGREEMENT_DRIVER_COLS = (
    "n_validated_reference",
    "n_validated_candidate",
    "agreement_rate_reference",
    "agreement_rate_candidate",
)


def mark_agreement_drivers(by_taxon: pd.DataFrame) -> pd.DataFrame:
    """Flag the taxon that drives each group's BLAST-agreement change.

    A group's agreement rate is `sum(agreeing reads) / sum(validated reads)`, so
    its change decomposes per taxon as
    `agree_reference/N_reference - agree_candidate/N_candidate` (with N the
    group's validated total on each side). Summing that over taxa gives the exact
    group-level drop. `agreement_drop_contribution` is each taxon's signed term;
    `is_agreement_driver` is the taxon contributing most to the drop. This handles
    a taxon present on only one side (its missing side contributes zero) and a
    composition shift with unchanged per-taxon rates, neither of which a
    within-taxon rate-delta ranking would catch. `abs_delta_agreement` is kept for
    reference.
    """
    out = by_taxon.copy()
    if out.empty or not set(_AGREEMENT_DRIVER_COLS).issubset(out.columns):
        for col in ("abs_delta_agreement", "agreement_drop_contribution"):
            out[col] = pd.Series(dtype=float)
        out["is_agreement_driver"] = pd.Series(dtype=bool)
        return out
    if "delta_agreement" in out.columns:
        out["abs_delta_agreement"] = pd.to_numeric(
            out["delta_agreement"], errors="coerce"
        ).abs()
    else:
        out["abs_delta_agreement"] = pd.Series(pd.NA, index=out.index, dtype="Float64")
    n_ref = pd.to_numeric(out["n_validated_reference"], errors="coerce").fillna(0.0)
    n_cand = pd.to_numeric(out["n_validated_candidate"], errors="coerce").fillna(0.0)
    rate_ref = pd.to_numeric(out["agreement_rate_reference"], errors="coerce").fillna(
        0.0
    )
    rate_cand = pd.to_numeric(out["agreement_rate_candidate"], errors="coerce").fillna(
        0.0
    )
    agree_ref = rate_ref * n_ref
    agree_cand = rate_cand * n_cand
    tot_ref = n_ref.groupby(out["group"]).transform("sum")
    tot_cand = n_cand.groupby(out["group"]).transform("sum")
    contribution = agree_ref.div(tot_ref.where(tot_ref > 0)) - agree_cand.div(
        tot_cand.where(tot_cand > 0)
    )
    out["agreement_drop_contribution"] = contribution
    driver = pd.Series(False, index=out.index)
    for _group, idx in out.groupby("group").groups.items():
        sub = contribution.loc[idx].dropna()
        if not sub.empty and sub.max() > 0:
            driver.loc[cast(Any, sub.idxmax())] = True
    out["is_agreement_driver"] = driver
    return out


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


_FASTQC_RANK = {"pass": 0, "warn": 1, "fail": 2}


def fastqc_worsenings(qc_flags: pd.DataFrame | None) -> list[dict[str, object]]:
    """Worsening FASTQC transitions (pass<warn<fail) as structured rows.

    Shared by `build_flags` and `build_findings` so the two cannot disagree on
    which transitions count as worsening. An improvement (e.g. warn->pass) changes
    the flag table but is not returned.
    """
    out: list[dict[str, object]] = []
    if qc_flags is None or qc_flags.empty:
        return out
    for idx in qc_flags.index:
        ref_rank = _FASTQC_RANK.get(str(qc_flags.at[idx, "reference_flag"]).lower())
        cand_rank = _FASTQC_RANK.get(str(qc_flags.at[idx, "candidate_flag"]).lower())
        if ref_rank is None or cand_rank is None or cand_rank <= ref_rank:
            continue
        out.append(
            {
                "group": qc_flags.at[idx, "group"],
                "sample": qc_flags.at[idx, "sample"],
                "stage": qc_flags.at[idx, "stage"],
                "check": qc_flags.at[idx, "check"],
                "transition": (
                    f"{qc_flags.at[idx, 'reference_flag']}->"
                    f"{qc_flags.at[idx, 'candidate_flag']}"
                ),
            }
        )
    return out


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
    # pass->fail cannot slip past the deterministic Main-findings coverage rule.
    for w in fastqc_worsenings(outputs.get("qc_flag_changes")):
        key = ", ".join(f"{c}={w[c]}" for c in ("group", "sample", "stage", "check"))
        records.append(
            {
                "focus": "qc",
                "key": key,
                "metric": "FASTQC flag worsened (pass<warn<fail)",
                "value": w["transition"],
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


def bounding_numbers(
    outputs: dict[str, pd.DataFrame],
    thresholds: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Largest deviation per checked metric, for the 'Checked, no action needed' bullets.

    Each stable dimension gets a bounding number (max |deviation| and where it
    occurred) so the reader can distinguish "checked and within X" from "not
    mentioned", computed here rather than by scanning the full table by eye (which
    is where a maximum is easy to misread).
    """
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    records: list[dict[str, object]] = []

    def add(
        metric: str,
        subset: str,
        df: pd.DataFrame | None,
        value_col: str,
        key_cols: list[str],
        threshold: float,
        direction: str = "abs",
    ) -> None:
        # `direction` matches build_flags: "abs" counts |value| over threshold
        # (bidirectional metrics like clade-share pp), "pos" counts only positive
        # exceedances (one-directional metrics like an agreement *drop*, where a
        # negative value is an improvement and must not count as flagged).
        def null_row() -> None:
            records.append(
                {
                    "metric": metric,
                    "subset": subset,
                    "max_abs_value": None,
                    "max_abs_group": "",
                    "threshold": threshold,
                    "n_flagged": 0,
                }
            )

        if df is None or df.empty or value_col not in df.columns:
            null_row()
            return
        signed = pd.to_numeric(df[value_col], errors="coerce")
        if not signed.notna().any():
            null_row()
            return
        # Pick the bound in the flagged direction: for "abs" the largest |value|;
        # for "pos" the largest signed value (the largest drop), so an improvement
        # (negative) is never reported as the biggest "drop". `compare` is the same
        # series the n_flagged count uses, keeping bound and count consistent.
        compare = signed.abs() if direction == "abs" else signed
        i = compare.idxmax()
        where = ", ".join(f"{c}={df.at[i, c]}" for c in key_cols if c in df.columns)
        records.append(
            {
                "metric": metric,
                "subset": subset,
                "max_abs_value": float(compare.loc[i]),
                "max_abs_group": where,
                "threshold": threshold,
                "n_flagged": int((compare > threshold).sum()),
            }
        )

    # Every flaggable dimension gets a row, even when its source analysis was not
    # computed (df is None): an empty `max_abs_value` then means "not computed",
    # distinct from a real value with n_flagged 0 ("checked, within threshold").
    add(
        "QC read survival (pp)",
        "raw->cleaned",
        outputs.get("qc_survival"),
        "delta_pp",
        ["group", "sample"],
        t["read_survival_pp"],
    )

    qc = outputs.get("qc_numeric")
    qc_others = (
        qc[~qc["metric"].isin(["n_reads_single", "n_read_pairs", "n_bases_approx"])]
        if qc is not None and not qc.empty
        else None
    )
    add(
        "QC numeric metric (% change)",
        "length/GC/duplication",
        qc_others,
        "pct_change",
        ["group", "sample", "stage", "metric"],
        t["qc_pct_change"],
    )

    bc = outputs.get("kraken_bray_curtis")
    if bc is not None and not bc.empty:
        # One bounding row per (rank, ribosomal) subset so the reader sees that
        # genus and the ribosomal subsets stayed below threshold even when a
        # species subset triggered.
        for (rank, ribosomal), sub in bc.groupby(["rank", "ribosomal"]):
            add(
                "Kraken Bray-Curtis",
                f"rank={rank}, ribosomal={ribosomal}",
                sub.reset_index(drop=True),
                "bray_curtis",
                ["group"],
                t["bray_curtis"],
            )
    else:
        add(
            "Kraken Bray-Curtis",
            "all subsets",
            None,
            "bray_curtis",
            ["group"],
            t["bray_curtis"],
        )

    status = outputs.get("viral_read_status")
    vert = (
        status[status["scope"] == "vertebrate"]
        if status is not None and not status.empty
        else None
    )
    for col, label, thr_key in (
        ("pct_lost", "vertebrate-viral reads lost (%)", "viral_pct_lost"),
        ("pct_gained", "vertebrate-viral reads gained (%)", "viral_pct_gained"),
        (
            "pct_reassigned",
            "vertebrate-viral reads reassigned (%)",
            "viral_pct_reassigned",
        ),
    ):
        add(label, "vertebrate", vert, col, ["group"], t[thr_key])

    clade = outputs.get("clade_rank_shares")
    clade_total = (
        clade[clade["count_type"] == "reads_clade_total"]
        if clade is not None and not clade.empty
        else None
    )
    add(
        "clade share change (pp)",
        "family/order",
        clade_total,
        "delta_pp",
        ["group", "name"],
        t["clade_share_pp"],
    )

    val = outputs.get("viral_validation_agreement")
    agreement = None
    if val is not None and not val.empty and "agreement_rate_reference" in val.columns:
        agreement = val.assign(
            agreement_drop=pd.to_numeric(
                val["agreement_rate_reference"], errors="coerce"
            )
            - pd.to_numeric(val["agreement_rate_candidate"], errors="coerce")
        )
    add(
        "BLAST-agreement rate drop",
        "per group",
        agreement,
        "agreement_drop",
        ["group"],
        t["validation_agreement_drop"],
        direction="pos",
    )

    return pd.DataFrame.from_records(
        records,
        columns=[
            "metric",
            "subset",
            "max_abs_value",
            "max_abs_group",
            "threshold",
            "n_flagged",
        ],
    )


# Maps a viral_read_status percent column to its finding_type, threshold key,
# reader-facing label, and movement direction. Drives both the threshold check
# and the manifest row so the two cannot drift.
_VIRAL_STATUS_FINDINGS = (
    (
        "pct_lost",
        "viral_reads_lost",
        "viral_pct_lost",
        "vertebrate-viral reads lost (%)",
        "down",
        "lost",
    ),
    (
        "pct_gained",
        "viral_reads_gained",
        "viral_pct_gained",
        "vertebrate-viral reads gained (%)",
        "up",
        "gained",
    ),
    (
        "pct_reassigned",
        "viral_reads_reassigned",
        "viral_pct_reassigned",
        "vertebrate-viral reads reassigned (%)",
        "na",
        None,
    ),
)


def build_findings(
    outputs: dict[str, pd.DataFrame],
    inventory: pd.DataFrame | None = None,
    columns: pd.DataFrame | None = None,
    skipped: pd.DataFrame | None = None,
    thresholds: dict[str, float] | None = None,
    name_map: dict[int, str] | None = None,
) -> pd.DataFrame:
    """Enumerate every required Main-finding as one routed, named manifest row.

    Supersets flags.tsv: alongside the threshold flags it carries the non-threshold
    coverage triggers (clades reaching zero candidate share, severe reassignments,
    output/schema anomalies, skipped groups) so the report's finding coverage is
    "walk this table" rather than "remember to scan four others". Each row names
    its entity from a source row (never a remembered taxid) and points to the TSV
    rows holding its drivers via `detail_source`.
    """
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    names = name_map or {}
    rec: list[dict[str, object]] = []

    def add(**kw: object) -> None:
        row: dict[str, object] = {
            "finding_type": "",
            "trigger": "",
            "group": "",
            "scope": "",
            "rank": "",
            "entity_taxid": None,
            "entity_name": "",
            "metric": "",
            "value": None,
            "threshold": None,
            "direction": "",
            "detail_source": "",
        }
        row.update(kw)
        rec.append(row)

    status = outputs.get("viral_read_status")
    if status is not None and not status.empty:
        vert = status[status["scope"] == "vertebrate"]
        for col, ftype, thr_key, label, direction, driver in _VIRAL_STATUS_FINDINGS:
            thr = t[thr_key]
            vals = pd.to_numeric(vert[col], errors="coerce")
            for idx in vert.index[(vals > thr).fillna(False)]:
                group = vert.at[idx, "group"]
                taxid = name = None
                if driver is not None:
                    taxid = vert.at[idx, f"dominant_{driver}_taxid"]
                    name = vert.at[idx, f"dominant_{driver}_name"]
                add(
                    finding_type=ftype,
                    trigger="threshold",
                    group=group,
                    scope="vertebrate",
                    entity_taxid=taxid if pd.notna(taxid) else None,
                    entity_name=name if isinstance(name, str) else "",
                    metric=label,
                    value=float(vals[idx]),
                    threshold=thr,
                    direction=direction,
                    detail_source=(
                        f"viral_read_status.tsv?group={group}&scope=vertebrate"
                        if driver
                        else f"viral_reassignment_pairs.tsv?group={group}&scope=vertebrate"
                    ),
                )

    clade = outputs.get("clade_rank_shares")
    if clade is not None and not clade.empty:
        total = clade[clade["count_type"] == "reads_clade_total"]
        thr = t["clade_share_pp"]
        for idx in total.index:
            delta = pd.to_numeric(total.at[idx, "delta_pp"], errors="coerce")
            zero = bool(total.at[idx, "reaches_zero"])
            crossed = pd.notna(delta) and abs(delta) > thr
            if not (zero or crossed):
                continue
            group = total.at[idx, "group"]
            taxid = int(cast(Any, total.at[idx, "taxid"]))
            add(
                finding_type="clade_reaches_zero" if zero else "clade_share_shift",
                trigger="threshold" if crossed else "reaches_zero",
                group=group,
                rank=total.at[idx, "rank_level"],
                entity_taxid=taxid,
                entity_name=str(total.at[idx, "name"]),
                metric="clade share change (pp)",
                value=float(delta) if pd.notna(delta) else None,
                threshold=thr,
                direction="zero" if zero else ("up" if delta > 0 else "down"),
                detail_source=(
                    f"clade_rank_shares.tsv?group={group}"
                    f"&taxid={taxid}&count_type=reads_clade_total"
                ),
            )

    bc = outputs.get("kraken_bray_curtis")
    if bc is not None and not bc.empty:
        thr = t["bray_curtis"]
        vals = pd.to_numeric(bc["bray_curtis"], errors="coerce")
        for idx in bc.index[(vals > thr).fillna(False)]:
            group = bc.at[idx, "group"]
            rank = bc.at[idx, "rank"]
            ribosomal = bc.at[idx, "ribosomal"]
            add(
                finding_type="kraken_community_shift",
                trigger="threshold",
                group=group,
                scope=f"ribosomal={ribosomal}",
                rank=rank,
                metric="Bray-Curtis dissimilarity",
                value=float(vals[idx]),
                threshold=thr,
                direction="na",
                detail_source=(
                    f"kraken_top_movers.tsv?group={group}&rank={rank}"
                    f"&ribosomal={ribosomal} (mover_rank==1)"
                ),
            )

    val = outputs.get("viral_validation_agreement")
    if val is not None and not val.empty and "agreement_rate_reference" in val.columns:
        thr = t["validation_agreement_drop"]
        drop = pd.to_numeric(
            val["agreement_rate_reference"], errors="coerce"
        ) - pd.to_numeric(val["agreement_rate_candidate"], errors="coerce")
        for idx in val.index[(drop > thr).fillna(False)]:
            group = val.at[idx, "group"]
            add(
                finding_type="blast_agreement_drop",
                trigger="threshold",
                group=group,
                metric="BLAST-agreement rate drop",
                value=float(drop[idx]),
                threshold=thr,
                direction="down",
                detail_source=(
                    f"viral_validation_agreement_by_taxon.tsv?group={group}"
                    " (is_agreement_driver==True)"
                ),
            )

    # QC threshold findings, mirroring build_flags so a QC regression that lands
    # in flags.tsv also appears in the manifest the report author works from.
    survival = outputs.get("qc_survival")
    if survival is not None and not survival.empty:
        thr = t["read_survival_pp"]
        vals = pd.to_numeric(survival["delta_pp"], errors="coerce")
        for idx in survival.index[(vals.abs() > thr).fillna(False)]:
            group = survival.at[idx, "group"]
            sample = survival.at[idx, "sample"]
            add(
                finding_type="qc_anomaly",
                trigger="threshold",
                group=group,
                metric=f"raw->cleaned read survival change (pp), sample {sample}",
                value=float(vals[idx]),
                threshold=thr,
                direction="up" if vals[idx] > 0 else "down",
                detail_source=f"qc_survival.tsv?group={group}&sample={sample}",
            )

    qc = outputs.get("qc_numeric")
    if qc is not None and not qc.empty:
        thr = t["qc_pct_change"]
        others = qc[
            ~qc["metric"].isin(["n_reads_single", "n_read_pairs", "n_bases_approx"])
        ]
        vals = pd.to_numeric(others["pct_change"], errors="coerce")
        for idx in others.index[(vals.abs() > thr).fillna(False)]:
            group = others.at[idx, "group"]
            add(
                finding_type="qc_anomaly",
                trigger="threshold",
                group=group,
                metric=(
                    f"{others.at[idx, 'metric']} (% change), "
                    f"sample {others.at[idx, 'sample']} {others.at[idx, 'stage']}"
                ),
                value=float(vals[idx]),
                threshold=thr,
                direction="up" if vals[idx] > 0 else "down",
                detail_source=f"qc_numeric.tsv?group={group}",
            )

    for w in fastqc_worsenings(outputs.get("qc_flag_changes")):
        add(
            finding_type="qc_anomaly",
            trigger="fastqc_worsening",
            group=w["group"],
            metric=f"FASTQC {w['check']} worsened {w['transition']} (sample {w['sample']})",
            direction="down",
            detail_source="qc_flag_changes.tsv",
        )

    pairs = outputs.get("viral_reassignment_pairs")
    if pairs is not None and not pairs.empty and "is_severe" in pairs.columns:
        sev = pairs[pairs["is_severe"].fillna(False) & (pairs["scope"] == "all")]
        for idx in sev.index:
            group = sev.at[idx, "group"]
            tx_ref = sev.at[idx, "taxid_reference"]
            tx_cand = sev.at[idx, "taxid_candidate"]
            cand_int = int(cast(Any, tx_cand)) if pd.notna(tx_cand) else None
            add(
                finding_type="severe_reassignment",
                trigger=str(sev.at[idx, "bucket"]),
                group=group,
                scope="all",
                entity_taxid=cand_int,
                entity_name=names.get(cand_int, "") if cand_int is not None else "",
                metric=(
                    f"{sev.at[idx, 'bucket']}: "
                    f"{_fmt_taxid(tx_ref)}->{_fmt_taxid(tx_cand)}"
                ),
                value=float(cast(Any, sev.at[idx, "n_reads"])),
                threshold=None,
                direction="na",
                detail_source=f"viral_reassignment_pairs.tsv?group={group}&scope=all",
            )

    if inventory is not None and not inventory.empty:
        # Platform mismatch: a group inferred as a different platform on each side
        # (e.g. Illumina degraded to ONT) is an anomaly even when the same file
        # types happen to be present on both sides, so it would not surface as a
        # presence difference below. Emit one finding per affected group (platform
        # repeats across that group's file rows).
        if "platform" in inventory.columns:
            mismatched = sorted(
                {
                    str(inventory.at[i, "group"])
                    for i in inventory.index
                    if "mismatch" in str(inventory.at[i, "platform"])
                }
            )
            for group in mismatched:
                add(
                    finding_type="output_anomaly",
                    trigger="platform_mismatch",
                    group=group,
                    metric="platform differs between runs",
                    detail_source=f"file_inventory.tsv?group={group}",
                )
        for idx in inventory.index:
            in_ref = bool(inventory.at[idx, "in_reference"])
            in_cand = bool(inventory.at[idx, "in_candidate"])
            group = inventory.at[idx, "group"]
            ft = inventory.at[idx, "file_type"]
            if in_ref != in_cand:
                side = "reference" if in_ref else "candidate"
                add(
                    finding_type="output_anomaly",
                    trigger="missing_file",
                    group=group,
                    metric=f"{ft} present on {side} only",
                    detail_source=f"file_inventory.tsv?group={group}&file_type={ft}",
                )
            elif not in_ref and not in_cand:
                # A row that is present on neither side only exists because the
                # type is expected for this platform: an expected output absent
                # from both runs (not a difference, but a coverage gap worth a
                # finding rather than silent omission).
                add(
                    finding_type="output_anomaly",
                    trigger="missing_both_sides",
                    group=group,
                    metric=f"{ft} expected but absent on both sides",
                    detail_source=f"file_inventory.tsv?group={group}&file_type={ft}",
                )
            elif {"n_rows_reference", "n_rows_candidate"}.issubset(inventory.columns):
                # Present on both sides, but a file that collapses to zero rows on
                # one side (or is newly populated) stays schema-conformant and so
                # would not flag elsewhere; an unexpectedly empty output is worth a
                # finding. (General non-zero row-count deltas are interpreted in
                # the Checked section from file_inventory, not flagged here.)
                nr = inventory.at[idx, "n_rows_reference"]
                nc = inventory.at[idx, "n_rows_candidate"]
                if pd.notna(nr) and pd.notna(nc) and ((nr == 0) != (nc == 0)):
                    empty_side = "candidate" if nc == 0 else "reference"
                    add(
                        finding_type="output_anomaly",
                        trigger="row_count_collapse",
                        group=group,
                        metric=f"{ft} has zero rows on {empty_side} only "
                        f"({int(cast(Any, nr))} vs {int(cast(Any, nc))})",
                        detail_source=(
                            f"file_inventory.tsv?group={group}&file_type={ft}"
                        ),
                    )

    if columns is not None and not columns.empty:
        for idx in columns.index:
            # Real missing/extra columns (the "(empty file)" marker is handled
            # separately because empty-on-both-sides is benign, not an anomaly).
            problems = [
                str(columns.at[idx, c])
                for c in (
                    "missing_vs_schema_reference",
                    "extra_vs_schema_reference",
                    "missing_vs_schema_candidate",
                    "extra_vs_schema_candidate",
                )
                if str(columns.at[idx, c]) not in ("", "(empty file)")
            ]
            ref_empty = str(columns.at[idx, "missing_vs_schema_reference"]) == (
                "(empty file)"
            )
            cand_empty = str(columns.at[idx, "missing_vs_schema_candidate"]) == (
                "(empty file)"
            )
            # An empty file on exactly one side is an anomaly; empty on both is
            # consistent (e.g. bracken intentionally not produced).
            if ref_empty != cand_empty:
                problems.append(
                    f"empty on {'reference' if ref_empty else 'candidate'} only"
                )
            inconsistent = not (
                bool(columns.at[idx, "groups_consistent_reference"])
                and bool(columns.at[idx, "groups_consistent_candidate"])
            )
            if not problems and not inconsistent:
                continue
            ft = columns.at[idx, "file_type"]
            detail = "; ".join(problems) or "columns inconsistent across groups"
            add(
                finding_type="schema_anomaly",
                trigger="column_mismatch",
                metric=f"{ft}: {detail}",
                detail_source=f"column_conformance.tsv?file_type={ft}",
            )

    if skipped is not None and not skipped.empty:
        for idx in skipped.index:
            add(
                finding_type="skipped_group",
                trigger="one_sided_input",
                group=skipped.at[idx, "group"],
                metric=f"{skipped.at[idx, 'metric']}: {skipped.at[idx, 'reason']}",
                detail_source="skipped_groups.tsv",
            )

    df = pd.DataFrame.from_records(
        rec,
        columns=[
            "finding_type",
            "trigger",
            "group",
            "scope",
            "rank",
            "entity_taxid",
            "entity_name",
            "metric",
            "value",
            "threshold",
            "direction",
            "detail_source",
        ],
    )
    # rank_in_type: 1 = largest |value| within each finding_type, so the report can
    # lead each subsection with its biggest instance. Rows without a numeric value
    # (output/schema/skipped triggers) sort last but keep a stable order.
    if df.empty:
        df["rank_in_type"] = pd.Series(dtype="Int64")
        return df
    magnitude = pd.to_numeric(df["value"], errors="coerce").abs().fillna(-1.0)
    df = df.assign(_m=magnitude).sort_values(
        ["finding_type", "_m"], ascending=[True, False], kind="stable"
    )
    df["rank_in_type"] = df.groupby("finding_type").cumcount() + 1
    return df.drop(columns="_m").reset_index(drop=True)


def _fmt_taxid(value: Any) -> str:
    """Render a possibly-NA taxid as a bare integer string for a pair label."""
    return str(int(value)) if pd.notna(value) else "NA"


def summarize_findings(findings: pd.DataFrame) -> pd.DataFrame:
    """Per-finding_type aggregates for report topic sentences.

    Every Main-finding subsection (and the Summary) opens with the same shape of
    aggregate -- "<finding> in N groups, M over threshold, ranging A to B" -- which
    the author would otherwise count by hand off `findings.tsv` and can get wrong.
    Distinct-group counts dedupe the family/order double-listing (a co-extensive
    family and order are one event across the same groups), so
    `n_distinct_groups` is the count to cite, and `n_distinct_groups_over_threshold`
    is the "M of N exceeded the threshold" sub-count.
    """
    cols = [
        "finding_type",
        "n_findings",
        "n_distinct_groups",
        "n_distinct_groups_over_threshold",
        "value_min",
        "value_max",
    ]
    if findings.empty or "finding_type" not in findings.columns:
        return pd.DataFrame(columns=cols)

    def real_groups(series: pd.Series) -> pd.Series:
        s = series.astype(str)
        return series[(s.str.len() > 0) & (s != "*")]

    records: list[dict[str, object]] = []
    for ftype, g in findings.groupby("finding_type"):
        vals = pd.to_numeric(g["value"], errors="coerce")
        thr_groups = real_groups(g.loc[g["trigger"] == "threshold", "group"])
        records.append(
            {
                "finding_type": ftype,
                "n_findings": int(len(g)),
                "n_distinct_groups": int(real_groups(g["group"]).nunique()),
                "n_distinct_groups_over_threshold": int(thr_groups.nunique()),
                "value_min": float(vals.min()) if vals.notna().any() else None,
                "value_max": float(vals.max()) if vals.notna().any() else None,
            }
        )
    return (
        pd.DataFrame.from_records(records, columns=cols)
        .sort_values("finding_type")
        .reset_index(drop=True)
    )
