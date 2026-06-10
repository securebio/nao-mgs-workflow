#!/usr/bin/env python3
DESC = """
Compare two mgs-workflow index releases and emit per-DB size diffs, genome
add/drop/per-species deltas, infection-status transitions, and a params diff.
Intended to be run before promoting a new index to production so reviewers can
spot regressions driven by upstream Virus-Host-DB or NCBI taxonomy drift.

Accepts s3:// URIs or local directories for both --old and --new, each pointing
at the *root* of an index release (the parent of `output/`).

Usage:
    python bin/benchmark_index.py \\
        --old s3://nao-mgs-index/20250825 \\
        --new s3://nao-mgs-index/20260518 \\
        --out ./bench-20250825-vs-20260518/
"""

###########
# IMPORTS #
###########

import argparse
import difflib
import gzip
import json
import logging
import re
import shutil
import subprocess
import tempfile
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)


###########
# STAGING #
###########


def list_recursive_sizes(prefix: str) -> dict[str, int]:
    """Return a mapping from top-level entry name under `prefix/output/results/`
    to total byte size. Top-level directories are summed across all files; files
    at `output/results/` itself are keyed by basename. Accepts s3:// or local."""
    base = f"{prefix.rstrip('/')}/output/results/"
    bucket: dict[str, int] = {}
    if prefix.startswith("s3://"):
        out = subprocess.run(
            ["aws", "s3", "ls", "--recursive", base],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        prefix_key = base.split("/", 3)[-1]  # strip "s3://bucket/"
        for line in out.splitlines():
            parts = line.split()
            if len(parts) < 4 or parts[2] == "0":
                continue
            size, key = int(parts[2]), parts[3]
            rel_str = key[len(prefix_key) :] if key.startswith(prefix_key) else key
            top = rel_str.split("/", 1)[0] or rel_str
            bucket[top] = bucket.get(top, 0) + size
    else:
        base_path = Path(base)
        for f in base_path.rglob("*"):
            if not f.is_file():
                continue
            top = f.relative_to(base_path).parts[0]
            bucket[top] = bucket.get(top, 0) + f.stat().st_size
    return bucket


def fetch(prefix: str, subpath: str, local_dir: Path) -> Path:
    """Stage `prefix/subpath` to `local_dir/<basename>` and return the local path."""
    src = f"{prefix.rstrip('/')}/{subpath}"
    dst = local_dir / Path(subpath).name
    if src.startswith("s3://"):
        logger.info(f"Downloading {src} -> {dst}")
        subprocess.run(["aws", "s3", "cp", src, str(dst)], check=True)
    else:
        logger.info(f"Copying {src} -> {dst}")
        shutil.copy(src, dst)
    return dst


###############
# COMPARISONS #
###############


def compare_size_listings(
    old_sizes: dict[str, int], new_sizes: dict[str, int]
) -> pd.DataFrame:
    """Return a DataFrame with columns name, old_bytes, new_bytes, delta_bytes,
    pct_change. Sorted by absolute delta descending."""
    rows: list[dict[str, int | str | float]] = []
    for name in sorted(set(old_sizes) | set(new_sizes)):
        o = old_sizes.get(name, 0)
        n = new_sizes.get(name, 0)
        pct = ((n - o) / o * 100) if o else float("nan")
        rows.append(
            {
                "name": name,
                "old_bytes": o,
                "new_bytes": n,
                "delta_bytes": n - o,
                "pct_change": round(pct, 2),
            }
        )
    df = pd.DataFrame(rows)
    df["_abs"] = df["delta_bytes"].abs()
    return (
        df.sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .reset_index(drop=True)
    )


def diff_genome_metadata(
    old_meta: pd.DataFrame, new_meta: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (added, removed, per_species_delta). 'added' and 'removed' index
    by genome_id with name + species_taxid context. 'per_species_delta' groups
    by species_taxid and shows old/new genome counts."""
    # assembly_accession is the join key into the new index's pre-filter raw
    # metadata (virus-genome-metadata-raw.tsv.gz), used to recover a lost
    # genome's build-time taxid + assembly_status for loss categorization.
    common_cols = [
        "assembly_accession",
        "genome_id",
        "taxid",
        "species_taxid",
        "organism_name",
    ]
    for df, label in [(old_meta, "old"), (new_meta, "new")]:
        missing = set(common_cols) - set(df.columns)
        if missing:
            raise ValueError(f"{label} metadata missing required columns: {missing}")
    old_ids = set(old_meta["genome_id"])
    new_ids = set(new_meta["genome_id"])
    added = new_meta[new_meta["genome_id"].isin(new_ids - old_ids)][common_cols]
    removed = old_meta[old_meta["genome_id"].isin(old_ids - new_ids)][common_cols]

    old_counts = old_meta["species_taxid"].value_counts().rename("old_count")
    new_counts = new_meta["species_taxid"].value_counts().rename("new_count")
    species = pd.concat([old_counts, new_counts], axis=1).fillna(0).astype(int)
    species["delta"] = species["new_count"] - species["old_count"]
    # Attach a representative organism name (prefer new, fall back to old).
    name_lookup = {
        **dict(zip(old_meta["species_taxid"], old_meta["organism_name"], strict=False)),
        **dict(zip(new_meta["species_taxid"], new_meta["organism_name"], strict=False)),
    }
    species["organism_name"] = species.index.map(name_lookup)
    species = species.reset_index().rename(columns={"index": "species_taxid"})
    species["_abs"] = species["delta"].abs()
    species = (
        species.sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .reset_index(drop=True)
    )
    return (
        added.sort_values("organism_name").reset_index(drop=True),
        removed.sort_values("organism_name").reset_index(drop=True),
        species[["species_taxid", "organism_name", "old_count", "new_count", "delta"]],
    )


def diff_reassignments(old_meta: pd.DataFrame, new_meta: pd.DataFrame) -> pd.DataFrame:
    """Genome_ids present in BOTH builds whose `species_taxid` changed —
    taxonomically reassigned while staying in the concern set. Coverage is
    unchanged (still screened), so this is informational: a value large relative
    to the kept set flags a mass re-assignment. Keyed on `species_taxid` (not the
    leaf) since the question is whether the genome is labelled differently
    downstream. Returns a per-flow table (old_species_taxid, new_species_taxid,
    organism_name, n_genomes) sorted by n_genomes desc; empty if none."""
    o = old_meta[["genome_id", "species_taxid"]].rename(
        columns={"species_taxid": "old_species_taxid"}
    )
    n = new_meta[["genome_id", "species_taxid", "organism_name"]].rename(
        columns={"species_taxid": "new_species_taxid"}
    )
    merged = o.merge(n, on="genome_id")  # inner join = genome_ids in both builds
    reassigned = merged[merged["old_species_taxid"] != merged["new_species_taxid"]]
    if reassigned.empty:
        return pd.DataFrame(
            columns=[
                "old_species_taxid",
                "new_species_taxid",
                "organism_name",
                "n_genomes",
            ]
        )
    flows = (
        reassigned.groupby(["old_species_taxid", "new_species_taxid"], dropna=False)
        .agg(n_genomes=("genome_id", "size"), organism_name=("organism_name", "first"))
        .reset_index()
        .sort_values("n_genomes", ascending=False)
        .reset_index(drop=True)
    )
    return flows[
        ["old_species_taxid", "new_species_taxid", "organism_name", "n_genomes"]
    ]


def diff_taxonomy(
    old_db: pd.DataFrame, new_db: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (added_taxa, removed_taxa). Each carries taxid, name, rank."""
    cols = ["taxid", "name", "rank"]
    old_ids = set(old_db["taxid"])
    new_ids = set(new_db["taxid"])
    added = new_db[new_db["taxid"].isin(new_ids - old_ids)][cols].reset_index(drop=True)
    removed = old_db[old_db["taxid"].isin(old_ids - new_ids)][cols].reset_index(
        drop=True
    )
    return added, removed


def infection_status_columns(db: pd.DataFrame) -> list[str]:
    return [c for c in db.columns if c.startswith("infection_status_")]


def infection_status_transitions(
    old_db: pd.DataFrame, new_db: pd.DataFrame, column: str
) -> pd.DataFrame:
    """Return a DataFrame of (old, new, count) for shared taxa whose status changed
    in `column`."""
    shared = old_db.merge(
        new_db[["taxid", column]], on="taxid", suffixes=("_old", "_new")
    )
    old_col = f"{column}_old" if f"{column}_old" in shared.columns else column
    new_col = f"{column}_new"
    changes = shared[shared[old_col] != shared[new_col]]
    counts = Counter(zip(changes[old_col], changes[new_col], strict=False))
    rows: list[dict[str, object]] = [
        {"old": o, "new": n, "count": c} for (o, n), c in counts.most_common()
    ]
    return pd.DataFrame(rows)


def infection_status_changes(
    old_db: pd.DataFrame, new_db: pd.DataFrame, column: str
) -> pd.DataFrame:
    """Return per-taxon status changes in `column` (taxid, name, rank, old, new)."""
    shared = old_db[["taxid", "name", "rank", column]].merge(
        new_db[["taxid", column]], on="taxid", suffixes=("_old", "_new")
    )
    old_col = f"{column}_old"
    new_col = f"{column}_new"
    changes = shared[shared[old_col] != shared[new_col]].rename(
        columns={old_col: "old_status", new_col: "new_status"}
    )
    return changes.reset_index(drop=True)


def build_parent_map(new_db: pd.DataFrame) -> dict[str, str]:
    """taxid -> parent_taxid lookup from the new annotated virus DB."""
    return dict(zip(new_db["taxid"], new_db["parent_taxid"], strict=False))


def _ancestor_in(taxid: str, parent_map: dict[str, str], target: set[str]) -> str:
    """Walk the lineage of `taxid` and return the first ancestor (or self) in
    `target`, or "" if none. Stops at roots (parent missing or self-loop)."""
    cur: str | None = taxid
    while cur:
        if cur in target:
            return cur
        parent = parent_map.get(cur)
        if parent is None or parent == cur:
            return ""
        cur = parent
    return ""


def surveilled_species(new_db: pd.DataFrame, screened_hosts: list[str]) -> set[str]:
    """Set of taxids whose `infection_status_<host>` is positive for any
    screened host in the new annotated virus DB — i.e. taxa that pass the
    surveillance host screen (`host_taxa_screen`)."""
    cols = [
        f"infection_status_{h}"
        for h in screened_hosts
        if f"infection_status_{h}" in new_db.columns
    ]
    if not cols or "taxid" not in new_db.columns:
        return set()
    mask = (new_db[cols] == "1").any(axis=1)
    return set(new_db.loc[mask, "taxid"].astype(str))


def categorize_lost_genomes_raw(
    removed: pd.DataFrame,
    raw_meta: pd.DataFrame,
    new_db: pd.DataFrame,
    parent_map: dict[str, str],
    excluded_taxids: set[str],
    screened_hosts: list[str],
) -> pd.DataFrame:
    """Categorize lost genome IDs by first matching rule in the decision tree.

    The genome identity is the assigned leaf taxon. The species rollup is used
    only by the surveillance predicate, matching `filter_viral_genbank_metadata.py`.
    """
    out = removed.copy()
    if out.empty:
        out["reason"] = pd.Series(dtype=str)
        out["reason_taxid"] = pd.Series(dtype=str)
        return out

    raw = raw_meta[["assembly_accession", "taxid", "assembly_status"]].rename(
        columns={"taxid": "_new_leaf", "assembly_status": "_new_status"}
    )
    out = out.merge(raw, on="assembly_accession", how="left", sort=False)
    old_leaf = out["taxid"].astype(str)
    new_leaf = out["_new_leaf"].fillna("").astype(str)
    raw_present = out["_new_leaf"].notna()
    current = out["_new_status"] == "current"

    species_of = (
        dict(
            zip(
                new_db["taxid"].astype(str),
                new_db["taxid_species"].astype(str),
                strict=False,
            )
        )
        if "taxid_species" in new_db.columns
        else {}
    )
    surveilled = surveilled_species(new_db, screened_hosts)
    new_surveilled = new_leaf.isin(surveilled) | new_leaf.map(species_of).isin(
        surveilled
    )
    hard_exclude = new_leaf.apply(
        lambda taxid: _ancestor_in(taxid, parent_map, excluded_taxids) if taxid else ""
    )

    reason = pd.Series("", index=out.index, dtype=str)
    reason_taxid = pd.Series("", index=out.index, dtype=str)

    def assign(mask: pd.Series, label: str, taxids: pd.Series | str = "") -> None:
        target = mask & (reason == "")
        reason.loc[target] = label
        if isinstance(taxids, pd.Series):
            reason_taxid.loc[target] = taxids.loc[target]
        else:
            reason_taxid.loc[target] = taxids

    present_current = raw_present & current
    assign(~raw_present, "absent_from_ncbi")
    assign(raw_present & ~current, "non_current_genome_version")
    assign(present_current & (hard_exclude != ""), "hard_excluded", hard_exclude)
    assign(
        present_current & ~new_surveilled & (new_leaf != old_leaf),
        "reassigned_to_excluded",
        new_leaf,
    )
    assign(
        present_current & ~new_surveilled & (new_leaf == old_leaf),
        "infection_status_demotion",
        new_leaf,
    )
    assign(reason == "", "other", new_leaf)

    out["reason"] = reason
    out["reason_taxid"] = reason_taxid
    return out.drop(columns=["_new_leaf", "_new_status"])


def categorize_gained_genomes_raw(
    added: pd.DataFrame,
    raw_meta: pd.DataFrame,
    old_db: pd.DataFrame,
    parent_map: dict[str, str],
    included_taxids: dict[str, set[str]],
    screened_hosts: list[str],
    old_build_date: str,
) -> pd.DataFrame:
    """Categorize gained genome IDs by first matching rule in the decision tree."""
    out = added.copy()
    if out.empty:
        out["reason"] = pd.Series(dtype=str)
        out["reason_taxid"] = pd.Series(dtype=str)
        out["source_database"] = pd.Series(dtype=str)
        return out

    raw = raw_meta[["assembly_accession", "release_date", "source_database"]].rename(
        columns={"release_date": "_release_date"}
    )
    out = out.merge(raw, on="assembly_accession", how="left", sort=False)
    new_leaf = out["taxid"].astype(str)
    release = out["_release_date"].fillna("").astype(str)

    all_included = set().union(*included_taxids.values()) if included_taxids else set()
    hard_include = new_leaf.apply(
        lambda taxid: _ancestor_in(taxid, parent_map, all_included) if taxid else ""
    )
    old_db_taxids = (
        set(old_db["taxid"].astype(str)) if "taxid" in old_db.columns else set()
    )
    old_surv = surveilled_species(old_db, screened_hosts)
    old_species_of = (
        dict(
            zip(
                old_db["taxid"].astype(str),
                old_db["taxid_species"].astype(str),
                strict=False,
            )
        )
        if "taxid_species" in old_db.columns
        else {}
    )
    old_surveilled = new_leaf.isin(old_surv) | new_leaf.map(old_species_of).isin(
        old_surv
    )

    reason = pd.Series("", index=out.index, dtype=str)
    reason_taxid = pd.Series("", index=out.index, dtype=str)

    def assign(mask: pd.Series, label: str, taxids: pd.Series) -> None:
        target = mask & (reason == "")
        reason.loc[target] = label
        reason_taxid.loc[target] = taxids.loc[target]

    assign((release != "") & (release > old_build_date), "newly_deposited", new_leaf)
    assign(hard_include != "", "hard_included", hard_include)
    assign(~new_leaf.isin(old_db_taxids), "new_taxon_in_taxonomy", new_leaf)
    assign(~old_surveilled, "infection_status_promotion", new_leaf)
    assign(release != "", "pre_existing_reincluded", new_leaf)
    assign(reason == "", "no_release_date", new_leaf)

    out["reason"] = reason
    out["reason_taxid"] = reason_taxid
    out["source_database"] = out["source_database"].fillna("").astype(str)
    return out.drop(columns=["_release_date"])


def classify_coverage(
    taxid: str,
    parent_map: dict[str, str],
    excluded_taxids: set[str],
    included_taxids: dict[str, set[str]],
    host: str,
) -> tuple[str, str]:
    """For one transition, walk the taxid up its lineage and return (covered_by,
    rule_taxid) — "excluded"/"included"/"" — describing whether an existing
    config rule already explains the observed status change for `host`.

    `included_taxids` is host -> set of taxids that are hard-included for that host."""
    host_includes = included_taxids.get(host, set())
    cur: str | None = taxid
    while cur:
        if cur in excluded_taxids:
            return "excluded", cur
        if cur in host_includes:
            return "included", cur
        parent = parent_map.get(cur)
        if parent is None or parent == cur:
            break
        cur = parent
    return "", ""


def includes_for_other_hosts(
    taxid: str,
    parent_map: dict[str, str],
    included_taxids: dict[str, set[str]],
    host: str,
) -> list[str]:
    """Return the list of *other* hosts for which the taxid (or any ancestor) is
    in the include rules. Used to flag policy/scope issues — e.g. a primate
    demotion of a taxid that we DID override for human/vertebrate. Empty list
    if no other host has this taxid included."""
    other_hosts: list[str] = []
    for h, taxids in included_taxids.items():
        if h == host:
            continue
        cur: str | None = taxid
        while cur:
            if cur in taxids:
                other_hosts.append(h)
                break
            parent = parent_map.get(cur)
            if parent is None or parent == cur:
                break
            cur = parent
    return sorted(other_hosts)


def annotate_changes_with_coverage(
    changes: pd.DataFrame,
    host: str,
    parent_map: dict[str, str],
    excluded_taxids: set[str],
    included_taxids: dict[str, set[str]],
) -> pd.DataFrame:
    """Add three columns:
    - `covered_by` ("excluded" | "included" | "")
    - `covered_rule_taxid` (the lineage taxid matched by the rule, or "")
    - `included_for_other_hosts` (comma-separated host names where the same
      taxid IS in include rules, when covered_by != "included" — flags policy
      gaps like a primate demotion of a taxid we overrode for human only)
    """
    out = changes.copy()
    if out.empty:
        for col in ("covered_by", "covered_rule_taxid", "included_for_other_hosts"):
            out[col] = pd.Series(dtype=str)
        return out
    coverage = out["taxid"].apply(
        lambda t: classify_coverage(
            t, parent_map, excluded_taxids, included_taxids, host
        )
    )
    out["covered_by"] = coverage.apply(lambda x: x[0])
    out["covered_rule_taxid"] = coverage.apply(lambda x: x[1])
    out["included_for_other_hosts"] = out.apply(
        lambda row: (
            ",".join(
                includes_for_other_hosts(
                    row["taxid"], parent_map, included_taxids, host
                )
            )
            if row["covered_by"] != "included"
            else ""
        ),
        axis=1,
    )
    return out


def summarise_params_changes(old_params: dict, new_params: dict) -> pd.DataFrame:
    """Top-level key-by-key change summary for `index-params.json`. Each row
    describes one key with kind ∈ {added, removed, changed} and short string
    representations of the values. Nested dict/list values are JSON-stringified
    for display; long values are truncated."""
    rows: list[dict[str, str]] = []
    all_keys = sorted(set(old_params) | set(new_params))
    for k in all_keys:
        in_old = k in old_params
        in_new = k in new_params
        if in_old and in_new and old_params[k] == new_params[k]:
            continue
        old_v = _stringify_param(old_params.get(k)) if in_old else ""
        new_v = _stringify_param(new_params.get(k)) if in_new else ""
        if not in_old:
            kind = "added"
        elif not in_new:
            kind = "removed"
        else:
            kind = "changed"
        rows.append({"key": k, "kind": kind, "old": old_v, "new": new_v})
    return pd.DataFrame(rows, columns=["key", "kind", "old", "new"]).astype(str)


def _stringify_param(v: object, max_len: int = 120) -> str:
    """Compact one-line stringification of a param value for table display."""
    if v is None:
        return ""
    s = json.dumps(v, sort_keys=True) if isinstance(v, dict | list) else str(v)
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s


def diff_params(old_params: dict, new_params: dict) -> str:
    """Return a unified diff between two pretty-printed params dicts."""
    old_lines = json.dumps(old_params, indent=2, sort_keys=True).splitlines(
        keepends=True
    )
    new_lines = json.dumps(new_params, indent=2, sort_keys=True).splitlines(
        keepends=True
    )
    return "".join(
        difflib.unified_diff(
            old_lines,
            new_lines,
            fromfile="old/index-params.json",
            tofile="new/index-params.json",
        )
    )


####################
# CONTENT METRICS  #
####################


def fasta_content_stats(path: Path) -> dict[str, int]:
    """Count records, total bp, and masked (N) bp in a (optionally gzipped) FASTA."""
    opener = gzip.open if str(path).endswith(".gz") else open
    records = 0
    total_bp = 0
    n_bp = 0
    with opener(path, "rt") as f:  # type: ignore[arg-type]
        for line in f:
            if line.startswith(">"):
                records += 1
                continue
            seq = line.rstrip("\n")
            total_bp += len(seq)
            n_bp += seq.count("N") + seq.count("n")
    return {"records": records, "total_bp": total_bp, "n_bp": n_bp}


def tsv_row_count(path: Path) -> int:
    """Count data rows in a (optionally gzipped) TSV (excluding header)."""
    opener = gzip.open if str(path).endswith(".gz") else open
    rows = 0
    with opener(path, "rt") as f:  # type: ignore[arg-type]
        for _ in f:
            rows += 1
    return max(rows - 1, 0)


######################
# REFERENCE STALENESS #
######################

KRAKEN_BUCKET_LIST_CMD = [
    "aws",
    "s3",
    "ls",
    "s3://genome-idx/kraken/",
    "--no-sign-request",
]
SILVA_FTP_INDEX = "https://ftp.arb-silva.de/"


def parse_kraken_url_date(url: str) -> str:
    """Extract YYYYMMDD from a Kraken2 standard-bundle URL, or '' if absent."""
    m = re.search(r"k2_standard_(\d{8})\.tar\.gz", url)
    return m.group(1) if m else ""


def latest_kraken_release() -> tuple[str, str] | None:
    """Return (date_str, filename) of the most recent k2_standard_*.tar.gz bundle
    in the public Kraken2 S3 bucket, or None if the listing fails."""
    try:
        out = subprocess.run(
            KRAKEN_BUCKET_LIST_CMD,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout
    except (subprocess.SubprocessError, OSError):
        return None
    dated: list[tuple[str, str]] = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        name = parts[-1]
        m = re.match(r"k2_standard_(\d{8})\.tar\.gz$", name)
        if m:
            dated.append((m.group(1), name))
    if not dated:
        return None
    dated.sort()
    return dated[-1]


def parse_silva_url_release(url: str) -> str:
    """Extract release identifier (e.g. '138.2') from a SILVA URL, or '' if absent."""
    m = re.search(r"release_(\d+(?:[._]\d+)?)", url)
    return m.group(1).replace("_", ".") if m else ""


def latest_silva_release() -> str | None:
    """Return the highest-numbered release_NN[.M] directory in the SILVA FTP root,
    or None if the fetch fails. Compared as (major, minor) tuples."""
    try:
        with urllib.request.urlopen(SILVA_FTP_INDEX, timeout=15) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError, TimeoutError):
        return None
    releases: set[tuple[int, int]] = set()
    for m in re.finditer(r"release_(\d+)(?:[._](\d+))?", body):
        major = int(m.group(1))
        minor = int(m.group(2)) if m.group(2) else 0
        releases.add((major, minor))
    if not releases:
        return None
    top = max(releases)
    return f"{top[0]}.{top[1]}" if top[1] else str(top[0])


def check_ref_staleness(new_params: dict) -> list[dict[str, str]]:
    """Build a list of {ref_name, current, current_date, latest, latest_date, status}
    rows describing each external reference's freshness.

    `status` is one of: "current" (matches latest available), "stale" (newer
    available), "unknown" (no automated check available; reviewer should confirm
    manually), or "error" (check attempted but failed)."""
    rows: list[dict[str, str]] = []

    kraken_url = new_params.get("kraken_db", "")
    if kraken_url:
        cur_date = parse_kraken_url_date(kraken_url)
        latest = latest_kraken_release()
        if latest is None:
            rows.append(
                {
                    "ref": "kraken_db",
                    "current": kraken_url,
                    "current_date": cur_date,
                    "latest": "",
                    "latest_date": "",
                    "status": "error",
                }
            )
        else:
            latest_date, latest_name = latest
            status = "current" if cur_date == latest_date else "stale"
            rows.append(
                {
                    "ref": "kraken_db",
                    "current": kraken_url,
                    "current_date": cur_date,
                    "latest": latest_name,
                    "latest_date": latest_date,
                    "status": status,
                }
            )

    # Hoist the SILVA index fetch out of the per-URL loop — the FTP root
    # listing is the same for ssu/lsu, so one HTTPS round-trip suffices.
    silva_keys = [k for k in ("ssu_url", "lsu_url") if new_params.get(k)]
    latest_rel = latest_silva_release() if silva_keys else None
    for key in silva_keys:
        url = new_params[key]
        cur_rel = parse_silva_url_release(url)
        if latest_rel is None:
            rows.append(
                {
                    "ref": key,
                    "current": url,
                    "current_date": cur_rel,
                    "latest": "",
                    "latest_date": "",
                    "status": "error",
                }
            )
        else:
            status = "current" if cur_rel == latest_rel else "stale"
            rows.append(
                {
                    "ref": key,
                    "current": url,
                    "current_date": cur_rel,
                    "latest": f"release_{latest_rel}",
                    "latest_date": latest_rel,
                    "status": status,
                }
            )

    for key in ("human_url", "taxonomy_url", "virus_host_db_url"):
        url = new_params.get(key, "")
        if not url:
            continue
        rows.append(
            {
                "ref": key,
                "current": url,
                "current_date": "",
                "latest": "",
                "latest_date": "",
                "status": "unknown",
            }
        )
    return rows


###############
# FACTS WRITER #
###############

OUTPUT_FILES = {
    "sizes": "sizes.tsv",
    "genomes_added": "genomes_added.tsv",
    "genomes_removed": "genomes_removed.tsv",
    "genomes_by_species": "genomes_by_species.tsv",
    "genomes_reassigned": "genomes_reassigned.tsv",
    "taxa_added": "taxa_added.tsv",
    "taxa_removed": "taxa_removed.tsv",
    "species_lost_all_genomes": "species_lost_all_genomes.tsv",
    "species_gained_all_genomes": "species_gained_all_genomes.tsv",
    "infection_status_transitions": "infection_status_transitions.tsv",
    "infection_status_changes_pattern": "infection_status_changes_<host>.tsv",
    "species_transitions_pattern": "species_transitions_<host>.tsv",
    "genomes_lost_categorized": "genomes_lost_categorized.tsv",
    "genomes_gained_categorized": "genomes_gained_categorized.tsv",
    "params_diff": "params_diff.txt",
    "facts": "facts.json",
}


def _reason_counts(df: pd.DataFrame) -> dict[str, int]:
    if df.empty or "reason" not in df.columns:
        return {}
    return {str(k): int(v) for k, v in df["reason"].value_counts().items()}


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_json_ready(v) for v in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return _json_ready(value.item())
    return value


def _records(df: pd.DataFrame) -> list[dict[str, object]]:
    return [
        {str(k): _json_ready(v) for k, v in record.items()}
        for record in df.to_dict(orient="records")
    ]


def _zero_species_transition_counts() -> dict[str, int]:
    return {
        "species_promotions": 0,
        "uncovered_species_promotions": 0,
        "species_demotions": 0,
        "uncovered_species_demotions": 0,
        "override_scope_gaps": 0,
    }


def _species_transition_counts(
    per_host_changes: dict[str, pd.DataFrame],
    coverage_available: bool,
) -> dict[str, dict[str, int]]:
    host_counts: dict[str, dict[str, int]] = {}
    for host, df in sorted(per_host_changes.items()):
        if not {"rank", "old_status", "new_status"}.issubset(df.columns):
            host_counts[host] = _zero_species_transition_counts()
            continue
        species = df[df["rank"] == "species"]
        promotions = species[
            (species["old_status"].astype(str) == "0")
            & (species["new_status"].astype(str) == "1")
        ]
        demotions = species[
            (species["old_status"].astype(str) == "1")
            & (species["new_status"].astype(str) == "0")
        ]

        if coverage_available and "covered_by" in species.columns:
            actionable_promotions = promotions[promotions["covered_by"] == ""]
            actionable_demotions = demotions[demotions["covered_by"] == ""]
        else:
            actionable_promotions = promotions
            actionable_demotions = demotions

        if "included_for_other_hosts" in actionable_demotions.columns:
            policy_gaps = actionable_demotions[
                actionable_demotions["included_for_other_hosts"] != ""
            ]
        else:
            policy_gaps = actionable_demotions.iloc[0:0]

        host_counts[host] = {
            "species_promotions": len(promotions),
            "uncovered_species_promotions": len(actionable_promotions),
            "species_demotions": len(demotions),
            "uncovered_species_demotions": len(actionable_demotions),
            "override_scope_gaps": len(policy_gaps),
        }
    return host_counts


def write_facts_json(
    out_dir: Path,
    old: str,
    new: str,
    sizes: pd.DataFrame,
    content_rows: list[dict[str, object]],
    metadata_schema_diff: tuple[list[str], list[str]],
    staleness_rows: list[dict[str, str]],
    lost_categorized: pd.DataFrame,
    gained_categorized: pd.DataFrame,
    species_lost: pd.DataFrame,
    species_gained: pd.DataFrame,
    added_taxa: pd.DataFrame,
    removed_taxa: pd.DataFrame,
    per_host_changes: dict[str, pd.DataFrame],
    coverage_available: bool,
    params_changes: pd.DataFrame,
    reassigned_flows: pd.DataFrame,
    n_kept: int,
) -> None:
    """Write compact facts consumed by the review skill.

    Detailed evidence stays in TSVs. This file only carries stable counts,
    metadata, and filenames so report prose does not live in the script.
    """
    removed_cols, added_cols = metadata_schema_diff
    n_reassigned = (
        int(reassigned_flows["n_genomes"].sum()) if not reassigned_flows.empty else 0
    )
    size_changed = sizes[sizes["delta_bytes"] != 0]
    facts = {
        "old": old,
        "new": new,
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "outputs": OUTPUT_FILES,
        "staleness": staleness_rows,
        "database_sizes": {
            "shrunk": int((sizes["delta_bytes"] < 0).sum()),
            "grown": int((sizes["delta_bytes"] > 0).sum()),
            "unchanged": int((sizes["delta_bytes"] == 0).sum()),
            "changed_entries": _records(size_changed),
        },
        "content_metrics": content_rows,
        "metadata_schema_diff": {
            "removed": removed_cols,
            "added": added_cols,
        },
        "genomes": {
            "lost_total": len(lost_categorized),
            "gained_total": len(gained_categorized),
            "lost_by_reason": _reason_counts(lost_categorized),
            "gained_by_reason": _reason_counts(gained_categorized),
            "species_lost_all_genomes": len(species_lost),
            "species_gained_all_genomes": len(species_gained),
            "reassigned_genomes": n_reassigned,
            "kept_genomes": n_kept,
        },
        "taxonomy": {
            "taxa_added": len(added_taxa),
            "taxa_removed": len(removed_taxa),
        },
        "infection_status": {
            "coverage_available": coverage_available,
            "hosts": _species_transition_counts(
                per_host_changes, coverage_available=coverage_available
            ),
        },
        "params": {
            "changes": _records(params_changes),
            "diff": OUTPUT_FILES["params_diff"],
        },
    }
    (out_dir / "facts.json").write_text(
        json.dumps(_json_ready(facts), allow_nan=False, indent=2, sort_keys=True) + "\n"
    )


########
# MAIN #
########


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=DESC, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--old",
        required=True,
        help="Old index root (s3://... or local path), parent of output/.",
    )
    parser.add_argument(
        "--new",
        required=True,
        help="New index root (s3://... or local path), parent of output/.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output directory for TSVs and facts.json.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Path to a mgs-workflow checkout. When given, the script reads "
        "ref/host-infection-overrides.json and uses the new index's "
        "viral_taxids_exclude_hard to annotate per-species transitions with "
        "which existing rule (if any) covers them.",
    )
    return parser.parse_args()


def load_existing_overrides(repo_root: Path) -> dict[str, set[str]]:
    """Read ref/host-infection-overrides.json and return host -> set of taxids."""
    path = repo_root / "ref" / "host-infection-overrides.json"
    out: dict[str, set[str]] = defaultdict(set)
    if not path.exists():
        logger.warning(
            f"No overrides file at {path}; coverage will treat all transitions as uncovered."
        )
        return dict(out)
    data = json.loads(path.read_text())
    for entry in data.get("overrides", []):
        taxid = str(entry["taxid"])
        for host in entry["hosts"]:
            out[host].add(taxid)
    return dict(out)


def main() -> None:
    args = parse_arguments()
    args.out.mkdir(parents=True, exist_ok=True)
    logger.info(f"Benchmarking {args.old} -> {args.new}")

    # Per-DB sizes
    logger.info("Listing per-DB sizes.")
    old_sizes = list_recursive_sizes(args.old)
    new_sizes = list_recursive_sizes(args.new)
    sizes = compare_size_listings(old_sizes, new_sizes)
    sizes.to_csv(args.out / "sizes.tsv", sep="\t", index=False)

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "old").mkdir()
        (td / "new").mkdir()

        # Genome metadata diff
        logger.info("Diffing virus genome metadata.")
        old_meta_path = fetch(
            args.old, "output/results/virus-genome-metadata-gid.tsv.gz", td / "old"
        )
        new_meta_path = fetch(
            args.new, "output/results/virus-genome-metadata-gid.tsv.gz", td / "new"
        )
        old_meta = pd.read_csv(old_meta_path, sep="\t", dtype=str)
        new_meta = pd.read_csv(new_meta_path, sep="\t", dtype=str)
        # New index's pre-filter assembly metadata (accession -> build-time taxid
        # + assembly_status). Required for lost-genome categorization, which
        # recovers each lost genome's build-time assignment from it.
        try:
            new_raw_path = fetch(
                args.new,
                "output/results/virus-genome-metadata-raw.tsv.gz",
                td / "new",
            )
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            raise ValueError(
                "Target index has no output/results/virus-genome-metadata-raw.tsv.gz,"
                " which is required for lost-genome categorization. Rebuild the index"
                " with a pipeline version that publishes the pre-filter assembly"
                " metadata."
            ) from exc
        new_raw_meta = pd.read_csv(new_raw_path, sep="\t", dtype=str)
        # Schema diff (column-set change is a major driver of compressed-bytes
        # change independent of row count).
        old_cols, new_cols = set(old_meta.columns), set(new_meta.columns)
        metadata_schema_diff = (
            [c for c in old_meta.columns if c not in new_cols],
            [c for c in new_meta.columns if c not in old_cols],
        )
        added_g, removed_g, by_species = diff_genome_metadata(old_meta, new_meta)
        added_g.to_csv(args.out / "genomes_added.tsv", sep="\t", index=False)
        removed_g.to_csv(args.out / "genomes_removed.tsv", sep="\t", index=False)
        by_species.to_csv(args.out / "genomes_by_species.tsv", sep="\t", index=False)
        # Genomes present in both builds whose species_taxid changed (reassigned
        # within the concern set; coverage unchanged — informational).
        reassigned_flows = diff_reassignments(old_meta, new_meta)
        n_kept = len(set(old_meta["genome_id"]) & set(new_meta["genome_id"]))
        reassigned_flows.to_csv(
            args.out / "genomes_reassigned.tsv", sep="\t", index=False
        )
        species_lost = (
            by_species[(by_species["new_count"] == 0) & (by_species["old_count"] > 0)]
            .sort_values("old_count", ascending=False)
            .reset_index(drop=True)
        )

        # Taxonomy + infection-status diff
        logger.info("Diffing virus taxonomy DB and infection-status annotations.")
        old_db_path = fetch(
            args.old, "output/results/total-virus-db-annotated.tsv.gz", td / "old"
        )
        new_db_path = fetch(
            args.new, "output/results/total-virus-db-annotated.tsv.gz", td / "new"
        )
        old_db = pd.read_csv(old_db_path, sep="\t", dtype=str)
        new_db = pd.read_csv(new_db_path, sep="\t", dtype=str)
        added_t, removed_t = diff_taxonomy(old_db, new_db)
        added_t.to_csv(args.out / "taxa_added.tsv", sep="\t", index=False)
        removed_t.to_csv(args.out / "taxa_removed.tsv", sep="\t", index=False)

        # Fetch params now so we can use new_params for coverage classification
        # and for reference-staleness checks.
        logger.info("Diffing index-params.json.")
        old_params_path = fetch(args.old, "output/input/index-params.json", td / "old")
        new_params_path = fetch(args.new, "output/input/index-params.json", td / "new")
        old_params = json.loads(old_params_path.read_text())
        new_params = json.loads(new_params_path.read_text())
        (args.out / "params_diff.txt").write_text(diff_params(old_params, new_params))

        # Coverage data: hard-excluded taxids come from the new index's params;
        # hard-included taxids come from the repo's overrides file (if --repo-root
        # was given). Without --repo-root we skip the annotation entirely.
        coverage_available = args.repo_root is not None
        if coverage_available:
            included_taxids = load_existing_overrides(args.repo_root)
            excluded_taxids = set(
                new_params.get("viral_taxids_exclude_hard", "").split()
            )
            parent_map = build_parent_map(new_db)
        else:
            included_taxids = {}
            excluded_taxids = set()
            parent_map = {}

        species_lost["covered_by_hard_exclude"] = (
            species_lost["species_taxid"]
            .astype(str)
            .apply(lambda taxid: _ancestor_in(taxid, parent_map, excluded_taxids))
            if coverage_available
            else ""
        )
        species_lost.to_csv(
            args.out / "species_lost_all_genomes.tsv", sep="\t", index=False
        )

        # Content metrics: stream the virus FASTA from S3/local (records + bp) and
        # use the already-staged metadata + DB files for row counts. Compressed
        # file sizes are misleading for gzipped FASTAs/TSVs because gzip ratio
        # varies with content — content metrics let reviewers see whether the
        # underlying content actually grew or shrank.
        logger.info("Computing FASTA / TSV content metrics.")
        old_fasta_path = fetch(
            args.old, "output/results/virus-genomes-masked.fasta.gz", td / "old"
        )
        new_fasta_path = fetch(
            args.new, "output/results/virus-genomes-masked.fasta.gz", td / "new"
        )
        old_fasta_stats = fasta_content_stats(old_fasta_path)
        new_fasta_stats = fasta_content_stats(new_fasta_path)
        content_rows: list[dict[str, object]] = [
            {
                "name": "virus-genomes-masked.fasta.gz",
                "old": old_fasta_stats,
                "new": new_fasta_stats,
                "metrics": ["records", "total_bp", "n_bp"],
            },
            {
                "name": "virus-genome-metadata-gid.tsv.gz",
                "old": {"rows": tsv_row_count(old_meta_path)},
                "new": {"rows": tsv_row_count(new_meta_path)},
                "metrics": ["rows"],
            },
            {
                "name": "total-virus-db-annotated.tsv.gz",
                "old": {"rows": tsv_row_count(old_db_path)},
                "new": {"rows": tsv_row_count(new_db_path)},
                "metrics": ["rows"],
            },
        ]

        per_host_changes: dict[str, pd.DataFrame] = {}
        host_cols = sorted(
            set(infection_status_columns(old_db))
            & set(infection_status_columns(new_db))
        )
        all_transitions = []
        for col in host_cols:
            host = col.removeprefix("infection_status_")
            trans = infection_status_transitions(old_db, new_db, col)
            if not trans.empty:
                trans.insert(0, "host", host)
                all_transitions.append(trans)
            changes = infection_status_changes(old_db, new_db, col)
            if coverage_available:
                changes = annotate_changes_with_coverage(
                    changes, host, parent_map, excluded_taxids, included_taxids
                )
            per_host_changes[host] = changes
            # Per-host TSVs are written once below, after cross-host annotation.
        if all_transitions:
            pd.concat(all_transitions, ignore_index=True).to_csv(
                args.out / "infection_status_transitions.tsv", sep="\t", index=False
            )
        else:
            (args.out / "infection_status_transitions.tsv").write_text(
                "host\told\tnew\tcount\n"
            )

    # Reference-DB staleness — outside the tempdir block; uses only new_params.
    logger.info("Checking reference-DB staleness (Kraken2, SILVA).")
    staleness_rows = check_ref_staleness(new_params)

    params_changes = summarise_params_changes(old_params, new_params)

    for host, df in per_host_changes.items():
        df.to_csv(
            args.out / f"infection_status_changes_{host}.tsv", sep="\t", index=False
        )
        species_changes = df[df["rank"] == "species"]
        species_changes.to_csv(
            args.out / f"species_transitions_{host}.tsv", sep="\t", index=False
        )

    # Per-genome-id categorization for §3.1 (lost / gained gids by reason). Both
    # sides key on the genome's assigned (leaf) taxon and the new index's
    # pre-filter raw metadata (exact, no no-drift assumption, no live NCBI).
    # The raw table is required (fetched above); gains additionally need its
    # release_date column and the old build date.
    if "release_date" not in new_raw_meta.columns:
        raise ValueError(
            "Target index's virus-genome-metadata-raw.tsv.gz lacks a release_date"
            " column, which is required for gained-genome categorization. Rebuild"
            " the index with a pipeline version that emits it."
        )
    # Old index build date (YYYY-MM-DD) from its params' trace timestamp, used to
    # tell genomes deposited since the old build from pre-existing ones.
    old_build_date = str(old_params.get("trace_timestamp", ""))[:10]
    logger.info("Categorizing lost / gained genome IDs.")
    screened_hosts = new_params.get("host_taxa_screen", "").split()
    lost_categorized = categorize_lost_genomes_raw(
        removed_g,
        new_raw_meta,
        new_db,
        parent_map,
        excluded_taxids,
        screened_hosts,
    )
    gained_categorized = categorize_gained_genomes_raw(
        added_g,
        new_raw_meta,
        old_db,
        parent_map,
        included_taxids,
        screened_hosts,
        old_build_date,
    )
    lost_categorized.to_csv(
        args.out / "genomes_lost_categorized.tsv", sep="\t", index=False
    )
    gained_categorized.to_csv(
        args.out / "genomes_gained_categorized.tsv", sep="\t", index=False
    )

    # Species that went from 0 → nonzero genomes (the gains counterpart to
    # species_lost_all_genomes). Used in §3.3 and the gained-species inventory appendix.
    species_gained = (
        by_species[(by_species["old_count"] == 0) & (by_species["new_count"] > 0)]
        .sort_values("new_count", ascending=False)
        .reset_index(drop=True)
    )
    species_gained.to_csv(
        args.out / "species_gained_all_genomes.tsv", sep="\t", index=False
    )

    # Compact facts for the review skill; detailed evidence stays in TSVs.
    write_facts_json(
        args.out,
        args.old,
        args.new,
        sizes,
        content_rows,
        metadata_schema_diff,
        staleness_rows,
        lost_categorized,
        gained_categorized,
        species_lost,
        species_gained,
        added_t,
        removed_t,
        per_host_changes,
        coverage_available,
        params_changes,
        reassigned_flows,
        n_kept,
    )
    logger.info(f"Done. Outputs in {args.out.resolve()}")


if __name__ == "__main__":
    main()
