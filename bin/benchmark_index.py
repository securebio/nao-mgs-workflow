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
from typing import cast

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
    common_cols = ["genome_id", "taxid", "species_taxid", "organism_name"]
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


def annotate_lost_genomes(
    lost: pd.DataFrame,
    old_meta: pd.DataFrame,
    new_meta: pd.DataFrame,
    new_db: pd.DataFrame,
    parent_map: dict[str, str] | None = None,
    excluded_taxids: set[str] | None = None,
) -> pd.DataFrame:
    """Annotate each row in the lost-all-genomes table with redistribution and
    hard-exclude coverage info.

    Redistribution is computed at the *genome_id* level: for each lost species,
    look up its old genome_ids in the new metadata. Genome_ids still present
    under a *different* `species_taxid` were redistributed (the genomes didn't
    vanish, NCBI / ICTV taxonomy reorganization moved them to a different
    species concept — common with the recent binomial-nomenclature push).
    Genome_ids absent from new metadata are truly lost.

    Hard-exclude coverage walks each species_taxid's lineage against the new
    index's `viral_taxids_exclude_hard` list. A covered loss has no downstream
    effect because the workflow forces the taxon to status `0` regardless.

    Adds columns:
    - `new_taxonomy_name`: name of `species_taxid` in the new taxonomy DB ("")
    - `redistributed_to_species_taxid`: most common new species_taxid that the
      old genome_ids now sit under (empty if none redistributed)
    - `redistributed_to_name`: organism name for that destination
    - `redistributed_genome_count`: number of old genome_ids found in new
      metadata (under any species_taxid)
    - `truly_lost_count`: number of old genome_ids absent from new metadata
    - `likely_rename`: "yes" if >=50% of old genome_ids were redistributed
      (semantic: the species concept moved); "no" otherwise.
    - `covered_by_hard_exclude`: rule taxid in `viral_taxids_exclude_hard` that
      covers this species via ancestry (empty if none).
    """
    out = lost.copy()
    if out.empty:
        for col in (
            "new_taxonomy_name",
            "redistributed_to_species_taxid",
            "redistributed_to_name",
            "redistributed_genome_count",
            "truly_lost_count",
            "likely_rename",
            "covered_by_hard_exclude",
        ):
            out[col] = pd.Series(dtype=str)
        return out
    if {"taxid", "name"}.issubset(new_db.columns):
        new_name_lookup = dict(zip(new_db["taxid"], new_db["name"], strict=False))
        out["new_taxonomy_name"] = out["species_taxid"].map(new_name_lookup).fillna("")
    else:
        out["new_taxonomy_name"] = ""

    # genome_id -> new species_taxid (None if not in new metadata)
    has_new_meta = {"genome_id", "species_taxid", "organism_name"}.issubset(
        new_meta.columns
    )
    has_old_meta = {"genome_id", "species_taxid"}.issubset(old_meta.columns)
    if has_new_meta:
        new_gid_to_species = dict(
            zip(new_meta["genome_id"], new_meta["species_taxid"], strict=False)
        )
        new_species_to_name = dict(
            zip(new_meta["species_taxid"], new_meta["organism_name"], strict=False)
        )
    else:
        new_gid_to_species = {}
        new_species_to_name = {}
    # species_taxid -> set of old genome_ids
    old_species_to_gids: dict[str, list[str]] = defaultdict(list)
    if has_old_meta:
        for sp, gid in zip(
            old_meta["species_taxid"], old_meta["genome_id"], strict=False
        ):
            old_species_to_gids[sp].append(gid)

    redist_taxid: list[str] = []
    redist_name: list[str] = []
    redist_count: list[int] = []
    truly_lost: list[int] = []
    for sp in out["species_taxid"]:
        old_gids = old_species_to_gids.get(sp, [])
        if not old_gids:
            redist_taxid.append("")
            redist_name.append("")
            redist_count.append(0)
            truly_lost.append(0)
            continue
        destinations: Counter[str] = Counter()
        absent = 0
        for gid in old_gids:
            dest = new_gid_to_species.get(gid)
            if dest is None:
                absent += 1
            else:
                destinations[dest] += 1
        if destinations:
            top_dest, top_count = destinations.most_common(1)[0]
            redist_taxid.append(top_dest)
            redist_name.append(new_species_to_name.get(top_dest, ""))
            redist_count.append(sum(destinations.values()))
        else:
            redist_taxid.append("")
            redist_name.append("")
            redist_count.append(0)
        truly_lost.append(absent)
    out["redistributed_to_species_taxid"] = redist_taxid
    out["redistributed_to_name"] = redist_name
    out["redistributed_genome_count"] = redist_count
    out["truly_lost_count"] = truly_lost
    # "likely_rename" now means: most of the species' genome_ids are still present
    # in the new metadata but under a different species_taxid. The old name-based
    # heuristic missed cases like Jingmen tick virus, where the same taxid is now
    # rank=isolate under a new species_taxid; that's redistribution, not a loss.
    out["likely_rename"] = out.apply(
        lambda r: (
            "yes"
            if r["redistributed_genome_count"] >= (r["old_count"] + 1) // 2
            else "no"
        ),
        axis=1,
    )

    # Hard-exclude coverage via lineage walk.
    if parent_map and excluded_taxids:
        cov: list[str] = []
        for sp in out["species_taxid"]:
            cur: str | None = sp
            match = ""
            while cur:
                if cur in excluded_taxids:
                    match = cur
                    break
                parent = parent_map.get(cur)
                if parent is None or parent == cur:
                    break
                cur = parent
            cov.append(match)
        out["covered_by_hard_exclude"] = cov
    else:
        out["covered_by_hard_exclude"] = ""
    return out


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


def categorize_lost_genomes(
    removed: pd.DataFrame,
    new_db: pd.DataFrame,
    parent_map: dict[str, str],
    excluded_taxids: set[str],
    old_infection_human: dict[str, str],
    new_infection_human: dict[str, str],
) -> pd.DataFrame:
    """Classify each removed `genome_id` (gone from new metadata at the gid
    level) by the most likely reason it was dropped. Categories, in priority
    order:

    - `hard_excluded`: gid's old species_taxid (or an ancestor) is now in
      `viral_taxids_exclude_hard` in the new build — the new exclude rule
      explains the loss.
    - `infection_status_demotion`: gid's old species had
      `infection_status_human = 1` and now has `0` in the new build (the
      species lost its human-infecting annotation, often the proximate cause
      for the gid being dropped from a surveillance-focused index).
    - `species_retired`: old species_taxid is no longer in the new taxonomy DB
      — the species concept was reorganized away and its gids weren't
      reassigned.
    - `other`: no rule applies; a genuine upstream drop.

    Returns the input DataFrame with `reason` and `reason_taxid` columns
    appended. `reason_taxid` is the lineage taxid that matched the rule
    (the excluded ancestor for `hard_excluded`, the species_taxid for the
    other categories, or "" for `other`)."""
    out = removed.copy()
    if out.empty:
        out["reason"] = pd.Series(dtype=str)
        out["reason_taxid"] = pd.Series(dtype=str)
        return out
    new_taxids: set[str] = set(new_db["taxid"]) if "taxid" in new_db.columns else set()
    reasons: list[str] = []
    reason_taxids: list[str] = []
    for _, row in out.iterrows():
        sp = str(row["species_taxid"])
        hard = _ancestor_in(sp, parent_map, excluded_taxids)
        if hard:
            reasons.append("hard_excluded")
            reason_taxids.append(hard)
            continue
        if (
            old_infection_human.get(sp, "") == "1"
            and new_infection_human.get(sp, "") == "0"
        ):
            reasons.append("infection_status_demotion")
            reason_taxids.append(sp)
            continue
        if sp not in new_taxids:
            reasons.append("species_retired")
            reason_taxids.append(sp)
            continue
        reasons.append("other")
        reason_taxids.append("")
    out["reason"] = reasons
    out["reason_taxid"] = reason_taxids
    return out


def categorize_gained_genomes(
    added: pd.DataFrame,
    parent_map: dict[str, str],
    included_taxids: dict[str, set[str]],
    old_infection_human: dict[str, str],
    new_infection_human: dict[str, str],
    old_taxids: set[str],
) -> pd.DataFrame:
    """Classify each newly-added `genome_id` by the most likely reason it was
    added. Categories, in priority order:

    - `hard_included`: gid's new species_taxid (or an ancestor) is in
      `ref/host-infection-overrides.json` for any host — the include rule
      explains why this gid lands in the surveillance set.
    - `infection_status_promotion`: gid's new species had
      `infection_status_human = 0` in old and now has `1` — a species that
      was previously not surveilled is now flagged human-infecting.
    - `newly_deposited_existing`: gid's new species already had
      `infection_status_human = 1` in the old index (and still does). New
      data for a known human-infecting species.
    - `species_new`: gid's new species_taxid did not exist in the old
      taxonomy DB — a brand-new species concept.
    - `other`: gid belongs to a known non-human-infecting species; the new
      record adds reference data without affecting the surveillance set.

    Returns the input DataFrame with `reason` and `reason_taxid` columns
    appended."""
    out = added.copy()
    if out.empty:
        out["reason"] = pd.Series(dtype=str)
        out["reason_taxid"] = pd.Series(dtype=str)
        return out
    all_included: set[str] = set()
    for taxids in included_taxids.values():
        all_included.update(taxids)
    reasons: list[str] = []
    reason_taxids: list[str] = []
    for _, row in out.iterrows():
        sp = str(row["species_taxid"])
        hard = _ancestor_in(sp, parent_map, all_included)
        if hard:
            reasons.append("hard_included")
            reason_taxids.append(hard)
            continue
        old_h = old_infection_human.get(sp, "")
        new_h = new_infection_human.get(sp, "")
        if old_h == "0" and new_h == "1":
            reasons.append("infection_status_promotion")
            reason_taxids.append(sp)
            continue
        if old_h == "1":
            reasons.append("newly_deposited_existing")
            reason_taxids.append(sp)
            continue
        if sp not in old_taxids:
            reasons.append("species_new")
            reason_taxids.append(sp)
            continue
        reasons.append("other")
        reason_taxids.append("")
    out["reason"] = reasons
    out["reason_taxid"] = reason_taxids
    return out


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


def annotate_cross_host_actionables(
    per_host_changes: dict[str, pd.DataFrame],
    species_lost_taxids: set[str],
) -> dict[str, pd.DataFrame]:
    """For each per-host changes DataFrame, add two columns to species-rank
    actionable rows:

    - `cross_host_actionable_on`: comma-joined list of *other* hosts where the
      same taxid is actionable in the same direction. Mirrors the existing
      `included_for_other_hosts` policy-gap column but for the actionable-rather
      -than-policy side. Lets the report group "promoted on human, primate,
      mammal, vertebrate" as one item instead of writing it up four times.
    - `driven_by_genome_loss` (demotions only): "yes" if the demoted taxid is
      also in the lost-all-genomes table. Surfaces the §3.1 → §5.x mechanical
      link so reviewers see "demotion is a consequence of the genome loss, not
      a VHDB drift" without having to cross-reference manually.
    """
    # First pass: build {(taxid, direction): set of hosts}
    actionable_hosts: dict[tuple[str, str], set[str]] = defaultdict(set)
    for host, df in per_host_changes.items():
        if df.empty or "covered_by" not in df.columns:
            continue
        sp = df[(df["rank"] == "species") & (df["covered_by"] == "")]
        for _, row in sp.iterrows():
            old_s, new_s = str(row["old_status"]), str(row["new_status"])
            if (old_s, new_s) in (("1", "0"), ("0", "1")):
                actionable_hosts[(row["taxid"], f"{old_s}->{new_s}")].add(host)
    # Second pass: annotate each DataFrame.
    out: dict[str, pd.DataFrame] = {}
    for host, df in per_host_changes.items():
        if df.empty or "covered_by" not in df.columns:
            out[host] = df
            continue
        df2 = df.copy()

        def _cross_host(row: pd.Series, current_host: str = host) -> str:
            old_s, new_s = str(row["old_status"]), str(row["new_status"])
            if (old_s, new_s) not in (("1", "0"), ("0", "1")) or row[
                "covered_by"
            ] != "":
                return ""
            others = actionable_hosts.get(
                (row["taxid"], f"{old_s}->{new_s}"), set()
            ) - {current_host}
            return ",".join(sorted(others))

        def _gloss(row: pd.Series) -> str:
            old_s, new_s = str(row["old_status"]), str(row["new_status"])
            if (old_s, new_s) != ("1", "0") or row["covered_by"] != "":
                return ""
            return "yes" if row["taxid"] in species_lost_taxids else ""

        df2["cross_host_actionable_on"] = df2.apply(_cross_host, axis=1)
        df2["driven_by_genome_loss"] = df2.apply(_gloss, axis=1)
        out[host] = df2
    return out


def detect_bidirectional_flips(
    per_host_changes: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Identify taxids whose species-rank actionable transitions go in *both*
    directions (1→0 on some hosts, 0→1 on others). This is the upstream-VHDB
    -taxonomy-churn fingerprint — a single `viral_taxids_exclude_hard` entry
    would demote the legitimate hosts along with the incorrect ones, so these
    cases need a distinct narrative.

    Returns one row per affected taxid with columns: taxid, name, hosts_up
    (comma-joined hosts promoted 0→1), hosts_down (comma-joined hosts demoted
    1→0). Empty DataFrame if no taxid satisfies both directions."""
    up_hosts: dict[str, set[str]] = defaultdict(set)
    down_hosts: dict[str, set[str]] = defaultdict(set)
    names: dict[str, str] = {}
    for host, df in per_host_changes.items():
        if df.empty or "covered_by" not in df.columns:
            continue
        actionable = df[(df["rank"] == "species") & (df["covered_by"] == "")]
        for _, row in actionable.iterrows():
            old_s, new_s = str(row["old_status"]), str(row["new_status"])
            if old_s == "0" and new_s == "1":
                up_hosts[row["taxid"]].add(host)
                names[row["taxid"]] = row["name"]
            elif old_s == "1" and new_s == "0":
                down_hosts[row["taxid"]].add(host)
                names[row["taxid"]] = row["name"]
    overlap = set(up_hosts) & set(down_hosts)
    rows = [
        {
            "taxid": t,
            "name": names[t],
            "hosts_up": ",".join(sorted(up_hosts[t])),
            "hosts_down": ",".join(sorted(down_hosts[t])),
        }
        for t in sorted(overlap)
    ]
    return pd.DataFrame(rows, columns=["taxid", "name", "hosts_up", "hosts_down"])


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
    return pd.DataFrame(rows, columns=["key", "kind", "old", "new"])


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


def tsv_header(path: Path) -> list[str]:
    """Return the header columns of a (optionally gzipped) TSV."""
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt") as f:  # type: ignore[arg-type]
        return f.readline().rstrip("\n").split("\t")


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

    for key in ("ssu_url", "lsu_url"):
        url = new_params.get(key, "")
        if not url:
            continue
        cur_rel = parse_silva_url_release(url)
        latest_rel = latest_silva_release()
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
                    "latest": "",
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


################
# REPORT WRITER #
################


def write_summary_md(  # noqa: C901, PLR0912, PLR0915 - long but linear report writer
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
    transitions: dict[str, pd.DataFrame],
    per_host_changes: dict[str, pd.DataFrame],
    coverage_available: bool,
    params_changes: pd.DataFrame,
    bidirectional_flips: pd.DataFrame,
) -> None:
    """Write a self-contained `summary.md` following the structure of
    `.claude/skills/benchmark-index/review-template.md`. Data-only; the
    `Summary`, per-section `Findings:` bullets, `§3.2/§3.3` discussion, and
    `Recommendations` sections carry script-generated factual bullets that
    the reviewer expands into prose in `REVIEW.md`."""
    # ---------- Header
    lines: list[str] = [
        "# `mgs-workflow` index benchmark report",
        "",
        f"- **Target index:** `{new}`",
        f"- **Reference index:** `{old}`",
        f"- **Report timestamp:** {datetime.now(UTC).strftime('%Y-%m-%d %H:%M')} UTC",
        "",
        "---",
        "",
        "## Summary",
        "",
        "_To be filled in by the reviewer with a bullet-list of top-level findings"
        " (drawing on the Findings sections below) and a **Recommendations** sub-list_"
        " _restating the concrete actions from the Recommendations section. Don't"
        " link out to other files or directory paths; the report should stand alone._",
        "",
        "---",
        "",
        "## Findings",
        "",
    ]

    # ---------- §1 Staleness
    lines += ["### 1. Staleness", ""]
    if staleness_rows:
        lines += [
            "| Reference | Version in target index | Latest available | Status |",
            "|---|---|---|---|",
        ]
        for r in staleness_rows:
            cur_str = (
                f"`{Path(r['current']).name}` ({r['current_date']})"
                if r["current_date"]
                else f"`{r['current']}`"
            )
            if r["status"] == "stale":
                latest_str = (
                    f"**`{r['latest'] or r['latest_date']}` ({r['latest_date']})**"
                )
                status_str = "**stale**"
            elif r["status"] == "current":
                latest_str = f"`{r['latest'] or r['latest_date']}` ({r['latest_date']})"
                status_str = "current"
            elif r["status"] == "unknown":
                latest_str = "_manual check required_"
                status_str = "—"
            else:
                latest_str = "_check failed_"
                status_str = "error"
            lines.append(f"| `{r['ref']}` | {cur_str} | {latest_str} | {status_str} |")
    else:
        lines.append("_No reference URLs detected in `index-params.json`._")
    lines += ["", "**Findings:**", ""]
    stale_refs = [r["ref"] for r in staleness_rows if r["status"] == "stale"]
    if stale_refs:
        lines.append(
            f"- {len(stale_refs)} reference{'s' if len(stale_refs) != 1 else ''} stale: "
            f"{', '.join(f'`{n}`' for n in stale_refs)}. Staleness applies to the URL pinned"
            " for the *next* build, not this one — never a blocker for promoting an already-built"
            " index. Recommend bumping in the next build (see Recommendations)."
        )
    else:
        lines.append("- All actively-checked references are current.")
    unknown_refs = [r["ref"] for r in staleness_rows if r["status"] == "unknown"]
    if unknown_refs:
        lines.append(
            f"- Passive (no automated check): {', '.join(f'`{n}`' for n in unknown_refs)}."
            " `taxonomy_url` and `virus_host_db_url` always fetch the latest by design."
        )

    # ---------- §2 Database size
    shrunk = sizes[sizes["delta_bytes"] < 0]
    grown = sizes[sizes["delta_bytes"] > 0]
    same = sizes[sizes["delta_bytes"] == 0]
    lines += [
        "",
        "### 2. Database size",
        "",
        "| DB | Size in reference index | Size in target index | Δ |",
        "|---|---:|---:|---:|",
    ]
    for _, row in sizes[sizes["delta_bytes"] != 0].iterrows():
        pct = "" if pd.isna(row["pct_change"]) else f" ({row['pct_change']:+.1f}%)"
        lines.append(
            f"| `{row['name']}` | {_fmt_bytes(row['old_bytes'])} "
            f"| {_fmt_bytes(row['new_bytes'])} "
            f"| {_fmt_bytes(row['delta_bytes'], signed=True)}{pct} |"
        )
    lines += ["", "**Findings:**", ""]
    lines.append(
        f"- {len(shrunk)} entries shrank, {len(grown)} grew, {len(same)} unchanged."
    )
    # Content-vs-compressed callouts so reviewers don't mistake gzip artifacts for content loss.
    if content_rows:
        for content in content_rows:
            old_dict = cast(dict[str, int], content.get("old", {}))
            new_dict = cast(dict[str, int], content.get("new", {}))
            metrics_list = cast(list[tuple[str, object]], content.get("metrics", []))
            for metric, _fmt in metrics_list:
                old_v = old_dict.get(metric, 0) or 0
                new_v = new_dict.get(metric, 0) or 0
                delta = new_v - old_v
                if delta == 0:
                    continue
                name = content.get("name", "")
                if metric == "records":
                    lines.append(
                        f"- `{name}` records: {old_v:,} → {new_v:,} ({delta:+,})"
                    )
                elif metric == "total_bp":
                    lines.append(
                        f"- `{name}` total bp: {_fmt_bp(old_v)} → {_fmt_bp(new_v)} "
                        f"({_fmt_bp(delta, signed=True)})"
                    )
                elif metric == "rows":
                    lines.append(
                        f"- `{name}` row count: {old_v:,} → {new_v:,} ({delta:+,})"
                    )
    removed_cols, added_cols = metadata_schema_diff
    if removed_cols or added_cols:
        lines.append(
            f"- `virus-genome-metadata-gid.tsv.gz` schema changed: "
            f"{len(removed_cols)} columns removed, {len(added_cols)} added "
            f"(drives most of that file's compressed-bytes change independent of row count)."
        )

    # ---------- §3 Virus genomes
    lines += ["", "### 3. Virus genomes", "", "#### 3.1. Total", ""]

    # Loss breakdown (template categories: hard_excluded, infection_status_demotion,
    # species_retired, other) and gain breakdown (hard_included,
    # infection_status_promotion, newly_deposited_existing, species_new, other).
    loss_n = len(lost_categorized)
    gain_n = len(gained_categorized)
    loss_counts: dict[str, int] = (
        {str(k): int(v) for k, v in lost_categorized["reason"].value_counts().items()}
        if loss_n
        else {}
    )
    gain_counts: dict[str, int] = (
        {str(k): int(v) for k, v in gained_categorized["reason"].value_counts().items()}
        if gain_n
        else {}
    )
    loss_labels = [
        ("hard_excluded", "Hard-excluded", "A.1"),
        ("infection_status_demotion", "Change in infection status (1→0 human)", "A.2"),
        ("species_retired", "Change in assigned taxid (old species retired)", "A.3"),
        ("other", "Other", "A.4"),
    ]
    gain_labels = [
        ("hard_included", "Hard-included", "A.5"),
        (
            "newly_deposited_existing",
            "Newly deposited for existing included taxa",
            "A.6",
        ),
        ("infection_status_promotion", "Change in infection status (0→1 human)", "A.7"),
        ("species_new", "Change in assigned taxid (new species concept)", "A.8"),
        ("other", "Other (non-human-infecting reference data)", "A.9"),
    ]
    lines.append(f"- **Genome IDs lost: {loss_n:,}**")
    for key, label, appendix in loss_labels:
        n = loss_counts.get(key, 0)
        lines.append(f"    - {label}: {n:,} (see Appendix {appendix})")
    lines.append(f"- **Genome IDs gained: {gain_n:,}**")
    for key, label, appendix in gain_labels:
        n = gain_counts.get(key, 0)
        lines.append(f"    - {label}: {n:,} (see Appendix {appendix})")

    # §3.2 Losses discussion (data + script-generated bullet starters; agent expands).
    lines += ["", "#### 3.2. Losses", ""]
    if loss_n == 0:
        lines.append("- No genome IDs lost.")
    else:
        for key, label, _ in loss_labels:
            n = loss_counts.get(key, 0)
            if n == 0:
                continue
            lines.append(f"- **{label} ({n:,} gids)**")
            sub = lost_categorized[lost_categorized["reason"] == key]
            top_species = (
                sub.groupby(["species_taxid", "organism_name"], dropna=False)
                .size()
                .sort_values(ascending=False)
                .head(5)
            )
            # MultiIndex key from groupby on two cols is a 2-tuple; mypy
            # types items() keys as bare Hashable, so iterate via the
            # underlying records. "count" shadows namedtuple.count(), so
            # the count column is renamed "n_gids".
            for record in (
                top_species.to_frame("n_gids").reset_index().itertuples(index=False)
            ):
                lines.append(
                    f"    - `{record.species_taxid}` *{record.organism_name}*:"
                    f" {int(cast(int, record.n_gids)):,} gids"
                )
        # Species not hard-excluded that nonetheless went to zero (the §3.2
        # "discuss species that drop to zero but aren't hard-excluded" ask).
        if not species_lost.empty and "covered_by_hard_exclude" in species_lost.columns:
            uncovered_zeros = species_lost[
                species_lost["covered_by_hard_exclude"] == ""
            ]
            if not uncovered_zeros.empty:
                lines.append("")
                lines.append(
                    f"- {len(uncovered_zeros)} species dropped to zero genomes without"
                    " being covered by a hard-exclude rule. Of these:"
                )
                if "likely_rename" in uncovered_zeros.columns:
                    redistributed = uncovered_zeros[
                        uncovered_zeros["likely_rename"] == "yes"
                    ]
                    true_losses = uncovered_zeros[
                        uncovered_zeros["likely_rename"] == "no"
                    ]
                    lines.append(
                        f"    - {len(redistributed)} redistributed (genome_ids moved to a"
                        " different species_taxid via NCBI/ICTV taxonomy restructuring; the"
                        " sequences remain in the index — see Appendix A.10)."
                    )
                    lines.append(
                        f"    - {len(true_losses)} true losses (genome_ids absent from the new"
                        " metadata entirely — see Appendix A.10)."
                    )

    # §3.3 Gains discussion.
    lines += ["", "#### 3.3. Gains", ""]
    if gain_n == 0:
        lines.append("- No genome IDs gained.")
    else:
        for key, label, _ in gain_labels:
            n = gain_counts.get(key, 0)
            if n == 0:
                continue
            lines.append(f"- **{label} ({n:,} gids)**")
            sub = gained_categorized[gained_categorized["reason"] == key]
            top_species = (
                sub.groupby(["species_taxid", "organism_name"], dropna=False)
                .size()
                .sort_values(ascending=False)
                .head(5)
            )
            # MultiIndex key from groupby on two cols is a 2-tuple; mypy
            # types items() keys as bare Hashable, so iterate via the
            # underlying records. "count" shadows namedtuple.count(), so
            # the count column is renamed "n_gids".
            for record in (
                top_species.to_frame("n_gids").reset_index().itertuples(index=False)
            ):
                lines.append(
                    f"    - `{record.species_taxid}` *{record.organism_name}*:"
                    f" {int(cast(int, record.n_gids)):,} gids"
                )
        if species_gained is not None and not species_gained.empty:
            lines.append("")
            lines.append(
                f"- {len(species_gained)} species went from 0 → nonzero genomes between builds"
                " — see Appendix A.11."
            )

    # ---------- §4 Infection status
    lines += [
        "",
        "### 4. Infection status",
        "",
        "Gains or losses of viral species assigned to each host category, ignoring"
        " hard inclusions and exclusions (i.e. only the **actionable** subset that"
        " a reviewer should look at):",
        "",
        "| Host | Promotions (0→1, actionable) | Demotions (1→0, actionable) |",
        "|---|---:|---:|",
    ]
    actionable_per_host: dict[str, dict[str, pd.DataFrame]] = {}
    for host in transitions:
        changes_df = per_host_changes.get(host, pd.DataFrame())
        species_demotions = changes_df[
            (changes_df["rank"] == "species")
            & (changes_df["old_status"].astype(str) == "1")
            & (changes_df["new_status"].astype(str) == "0")
        ]
        species_promotions = changes_df[
            (changes_df["rank"] == "species")
            & (changes_df["old_status"].astype(str) == "0")
            & (changes_df["new_status"].astype(str) == "1")
        ]
        if coverage_available and not changes_df.empty:
            dem_actionable = species_demotions[species_demotions["covered_by"] == ""]
            pro_actionable = species_promotions[species_promotions["covered_by"] == ""]
            dem_policy = dem_actionable[
                dem_actionable["included_for_other_hosts"] != ""
            ]
            actionable_per_host[host] = {
                "demotions": dem_actionable,
                "promotions": pro_actionable,
                "policy_gaps": dem_policy,
            }
            lines.append(
                f"| `{host}` | {len(pro_actionable)} | {len(dem_actionable)} |"
            )
        else:
            lines.append(
                f"| `{host}` | {len(species_promotions)} | {len(species_demotions)} |"
            )
    lines += ["", "**Findings:**", ""]
    if not coverage_available:
        lines.append(
            "- Coverage annotation unavailable (re-run with `--repo-root <mgs-workflow>`"
            " to identify which transitions are absorbed by existing rules)."
        )
    else:
        # Build de-duplicated actionable taxid lists for the findings + appendix.
        all_actionable_pro: dict[tuple[str, str], set[str]] = defaultdict(set)
        all_actionable_dem: dict[tuple[str, str], set[str]] = defaultdict(set)
        for host, buckets in actionable_per_host.items():
            for _, row in buckets["promotions"].iterrows():
                all_actionable_pro[(row["taxid"], row["name"])].add(host)
            for _, row in buckets["demotions"].iterrows():
                all_actionable_dem[(row["taxid"], row["name"])].add(host)
        if all_actionable_pro:
            lines.append(
                f"- **{len(all_actionable_pro)} unique actionable promotion taxid(s)**"
                f" across {sum(len(h) for h in all_actionable_pro.values())} host-rows."
                " See Appendix A.12 for each taxid with its affected hosts."
            )
            for (taxid, name), hosts in sorted(
                all_actionable_pro.items(), key=lambda x: (-len(x[1]), x[0])
            ):
                lines.append(
                    f"    - `{taxid}` *{name}* — promoted on {','.join(sorted(hosts))}"
                )
        else:
            lines.append("- No actionable promotions.")
        if all_actionable_dem:
            policy_gap_rows = [
                (host, row)
                for host, buckets in actionable_per_host.items()
                for _, row in buckets["policy_gaps"].iterrows()
            ]
            genome_loss_count = sum(
                1
                for _host, buckets in actionable_per_host.items()
                for _, row in buckets["demotions"].iterrows()
                if str(row.get("driven_by_genome_loss", "")) == "yes"
            )
            lines.append(
                f"- **{len(all_actionable_dem)} unique actionable demotion taxid(s)**"
                f" across {sum(len(h) for h in all_actionable_dem.values())} host-rows."
                f" {genome_loss_count} of those host-rows are mechanically driven by §3.2"
                " genome losses (no genomes → no ancestor-propagation evidence → demotion),"
                " not by upstream VHDB drift; those need no override."
            )
            if policy_gap_rows:
                lines.append(
                    f"- **{len(policy_gap_rows)} override policy gap(s)** — demotion(s)"
                    " whose species_taxid IS in `ref/host-infection-overrides.json` but"
                    " only for *other* hosts. Widen the override entry's `hosts` list or"
                    " accept the drift:"
                )
                for host, row in policy_gap_rows:
                    lines.append(
                        f"    - `{row['taxid']}` *{row['name']}* on `{host}`"
                        f" (override covers: {row['included_for_other_hosts']})"
                    )
        else:
            lines.append("- No actionable demotions.")
        if not bidirectional_flips.empty:
            lines.append(
                f"- **{len(bidirectional_flips)} bidirectional flip(s)** — same taxid"
                " actionable in both directions across different hosts (upstream VHDB"
                " taxonomy churn fingerprint; a `viral_taxids_exclude_hard` entry would"
                " demote on every host including the legitimate ones):"
            )
            for _, row in bidirectional_flips.iterrows():
                lines.append(
                    f"    - `{row['taxid']}` *{row['name']}*: promoted on"
                    f" {row['hosts_up']}, demoted on {row['hosts_down']}"
                )

    # ---------- §5 Other notable changes
    lines += ["", "### 5. Other notable changes", ""]
    if params_changes.empty:
        lines.append("- No top-level `index-params.json` key changes.")
    else:
        added = params_changes[params_changes["kind"] == "added"]
        removed = params_changes[params_changes["kind"] == "removed"]
        changed = params_changes[params_changes["kind"] == "changed"]
        lines.append(
            f"- **`index-params.json`**: {len(added)} keys added, {len(removed)} removed,"
            f" {len(changed)} value-changed. See Appendix A.13 for the key-by-key table"
            " and Appendix A.14 for the verbatim diff."
        )
        # Single-line callouts for high-signal params.
        for _, row in changed.iterrows():
            key = row["key"]
            if key in {"trace_timestamp", "base_dir"}:
                continue  # noise
            if key.endswith("_url") or key == "kraken_db":
                lines.append(
                    f"    - `{key}` changed: `{Path(row['old']).name}` → "
                    f"`{Path(row['new']).name}`"
                )
            elif key == "viral_taxids_exclude_hard":
                old_tx = set(row["old"].split())
                new_tx = set(row["new"].split())
                add_tx = new_tx - old_tx
                rm_tx = old_tx - new_tx
                if add_tx:
                    lines.append(
                        f"    - `viral_taxids_exclude_hard` added: "
                        f"{', '.join(f'`{t}`' for t in sorted(add_tx))}"
                    )
                if rm_tx:
                    lines.append(
                        f"    - `viral_taxids_exclude_hard` removed: "
                        f"{', '.join(f'`{t}`' for t in sorted(rm_tx))}"
                    )
    lines.append(
        f"- **Virus taxonomy DB**: {len(added_taxa):,} taxa added, {len(removed_taxa):,}"
        " removed (routine NCBI/ICTV churn at this magnitude; not itemised)."
    )

    # ---------- Recommendations placeholder
    lines += [
        "",
        "---",
        "",
        "## Recommendations",
        "",
        "_To be filled in by the reviewer based on findings above. Use the template's"
        " ordered list with each entry's confidence level (high / scientist judgement /"
        " policy / next-build hygiene / coordination)._",
    ]

    # ---------- Appendix
    lines += ["", "---", "", "## Appendix", ""]

    # A.1–A.4: lost gid categories. A.5–A.9: gained gid categories. A.10:
    # full lost-species inventory. A.11: full gained-species inventory.
    # A.12: per-host actionable transitions. A.13: params changes table.
    # A.14: verbatim params diff.
    def _gid_appendix(
        label: str, reason_key: str, df: pd.DataFrame, head_n: int = 50
    ) -> list[str]:
        subset = df[df["reason"] == reason_key] if not df.empty else df
        body: list[str] = [
            f"### {label}",
            "",
            f"{len(subset):,} genome_ids.",
            "",
        ]
        if len(subset) == 0:
            return body
        body += [
            "| genome_id | species_taxid | Organism | reason_taxid |",
            "|---|---|---|---|",
        ]
        for _, row in subset.head(head_n).iterrows():
            body.append(
                f"| `{row['genome_id']}` | `{row['species_taxid']}` "
                f"| *{row['organism_name']}* | `{row.get('reason_taxid', '') or ''}` |"
            )
        if len(subset) > head_n:
            body.append(
                f"_…and {len(subset) - head_n:,} more; full list in `genomes_lost_categorized.tsv` /"
                " `genomes_gained_categorized.tsv` in the output directory._"
            )
        return body

    for label, key in [
        ("A.1. Lost gids — hard-excluded", "hard_excluded"),
        ("A.2. Lost gids — infection-status demotion", "infection_status_demotion"),
        ("A.3. Lost gids — species retired", "species_retired"),
        ("A.4. Lost gids — other", "other"),
    ]:
        lines += _gid_appendix(label, key, lost_categorized)
        lines.append("")

    for label, key in [
        ("A.5. Gained gids — hard-included", "hard_included"),
        (
            "A.6. Gained gids — newly deposited for existing included taxa",
            "newly_deposited_existing",
        ),
        ("A.7. Gained gids — infection-status promotion", "infection_status_promotion"),
        ("A.8. Gained gids — new species concept", "species_new"),
        ("A.9. Gained gids — other (non-human-infecting reference data)", "other"),
    ]:
        lines += _gid_appendix(label, key, gained_categorized)
        lines.append("")

    # A.10 — full lost-species inventory
    if len(species_lost) > 0:
        lines += [
            "### A.10. Full lost-species inventory",
            "",
            f"All {len(species_lost)} species with new_count = 0 (sorted by old_count desc):",
            "",
            "| species_taxid | Organism | Old | Redist gids | Truly lost"
            " | → Dest taxid | Dest name | Covered_by_hard_exclude |",
            "|---|---|---:|---:|---:|---|---|---|",
        ]
        for _, row in species_lost.iterrows():
            lines.append(
                f"| `{row['species_taxid']}` | *{row['organism_name']}* | {row['old_count']} "
                f"| {row.get('redistributed_genome_count', 0)} | {row.get('truly_lost_count', '—')} "
                f"| {row.get('redistributed_to_species_taxid', '')} "
                f"| {row.get('redistributed_to_name', '')} "
                f"| {row.get('covered_by_hard_exclude', '')} |"
            )
        lines.append("")

    # A.11 — full gained-species inventory (species that went 0 → nonzero)
    if species_gained is not None and not species_gained.empty:
        lines += [
            "### A.11. Full gained-species inventory",
            "",
            f"All {len(species_gained)} species with old_count = 0 and new_count > 0"
            " (sorted by new_count desc):",
            "",
            "| species_taxid | Organism | New genome count |",
            "|---|---|---:|",
        ]
        head_n = 100
        for _, row in species_gained.head(head_n).iterrows():
            lines.append(
                f"| `{row['species_taxid']}` | *{row['organism_name']}* | {row['new_count']} |"
            )
        if len(species_gained) > head_n:
            lines.append(
                f"_…and {len(species_gained) - head_n:,} more; see"
                " `genomes_by_species.tsv` in the output directory._"
            )
        lines.append("")

    # A.12 — per-host actionable transitions
    if coverage_available and actionable_per_host:
        lines += [
            "### A.12. Per-host actionable transitions",
            "",
        ]
        for host, buckets in actionable_per_host.items():
            dem = buckets["demotions"]
            pro = buckets["promotions"]
            if dem.empty and pro.empty:
                continue
            lines += ["", f"**`{host}`**", ""]
            if not pro.empty:
                lines += [
                    "Promotions (0→1):",
                    "",
                    "| taxid | Name | Also actionable on |",
                    "|---|---|---|",
                ]
                for _, row in pro.iterrows():
                    cross = row.get("cross_host_actionable_on", "") or "—"
                    lines.append(f"| `{row['taxid']}` | *{row['name']}* | {cross} |")
                lines.append("")
            if not dem.empty:
                lines += [
                    "Demotions (1→0):",
                    "",
                    "| taxid | Name | Override scope (other hosts) | Genome loss |",
                    "|---|---|---|---|",
                ]
                for _, row in dem.iterrows():
                    other = row.get("included_for_other_hosts", "") or "—"
                    gloss = row.get("driven_by_genome_loss", "") or "—"
                    lines.append(
                        f"| `{row['taxid']}` | *{row['name']}* | {other} | {gloss} |"
                    )

    # A.13 — params changes table
    if not params_changes.empty:
        lines += [
            "",
            "### A.13. `index-params.json` key-by-key changes",
            "",
            "| Key | Kind | Old | New |",
            "|---|---|---|---|",
        ]
        for _, row in params_changes.iterrows():
            old_s = row["old"] or "—"
            new_s = row["new"] or "—"
            lines.append(f"| `{row['key']}` | {row['kind']} | {old_s} | {new_s} |")

    # A.14 — verbatim params diff
    diff_path = out_dir / "params_diff.txt"
    if diff_path.exists():
        diff_text = diff_path.read_text()
        lines += [
            "",
            "### A.14. Verbatim `index-params.json` diff",
            "",
            "```diff",
            diff_text.rstrip("\n"),
            "```",
            "",
        ]

    (out_dir / "summary.md").write_text("\n".join(lines))


def _fmt_bp(n: int | float, signed: bool = False) -> str:
    """Format a base-pair count human-readably (Gbp / Mbp / Kbp / bp)."""
    if pd.isna(n):
        return "—"
    n = int(n)
    sign = "+" if signed and n > 0 else ("-" if signed and n < 0 else "")
    n = abs(n)
    for unit, threshold in [("Gbp", 1_000_000_000), ("Mbp", 1_000_000), ("Kbp", 1_000)]:
        if n >= threshold:
            return f"{sign}{n / threshold:.2f} {unit}"
    return f"{sign}{n:,} bp"


def _fmt_bytes(n: int | float, signed: bool = False) -> str:
    """Format bytes as a human-readable string. 1234567890 → '1.15 GB'."""
    if pd.isna(n):
        return "—"
    n = int(n)
    sign = "+" if signed and n > 0 else ("-" if signed and n < 0 else "")
    n = abs(n)
    units = [
        ("TB", 1 << 40),
        ("GB", 1 << 30),
        ("MB", 1 << 20),
        ("KB", 1 << 10),
        ("B", 1),
    ]
    for unit, threshold in units:
        if n >= threshold or unit == "B":
            return (
                f"{sign}{n / threshold:.2f} {unit}"
                if unit != "B"
                else f"{sign}{n} {unit}"
            )
    return f"{sign}{n} B"


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
        help="Output directory for TSVs and summary.md.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Path to a mgs-workflow checkout. When given, the script reads "
        "ref/host-infection-overrides.json and uses the new index's "
        "viral_taxids_exclude_hard to annotate per-species transitions with "
        "which existing rule (if any) covers them, surfacing only the "
        "uncovered actionable ones in summary.md.",
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
        # Schema diff (column-set change is a major driver of compressed-bytes
        # change independent of row count).
        old_meta_cols = list(old_meta.columns)
        new_meta_cols = list(new_meta.columns)
        metadata_schema_diff = (
            [c for c in old_meta_cols if c not in set(new_meta_cols)],
            [c for c in new_meta_cols if c not in set(old_meta_cols)],
        )
        added_g, removed_g, by_species = diff_genome_metadata(old_meta, new_meta)
        added_g.to_csv(args.out / "genomes_added.tsv", sep="\t", index=False)
        removed_g.to_csv(args.out / "genomes_removed.tsv", sep="\t", index=False)
        by_species.to_csv(args.out / "genomes_by_species.tsv", sep="\t", index=False)
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

        # Lost-genomes triage: genome_id-level redistribution check + hard-exclude
        # coverage. Replaces the old name-based likely_rename heuristic which missed
        # cases like Jingmen tick virus (same taxid, demoted from species to isolate
        # rank under a new species_taxid, all genome_ids still present).
        species_lost = annotate_lost_genomes(
            species_lost,
            old_meta,
            new_meta,
            new_db,
            parent_map=parent_map,
            excluded_taxids=excluded_taxids,
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
                "metrics": [
                    (
                        "records",
                        lambda n, signed=False: (
                            f"{'+' if signed and n > 0 else ''}{n:,}"
                        ),
                    ),
                    ("total_bp", _fmt_bp),
                    ("n_bp", _fmt_bp),
                ],
            },
            {
                "name": "virus-genome-metadata-gid.tsv.gz",
                "old": {"rows": tsv_row_count(old_meta_path)},
                "new": {"rows": tsv_row_count(new_meta_path)},
                "metrics": [
                    (
                        "rows",
                        lambda n, signed=False: (
                            f"{'+' if signed and n > 0 else ''}{n:,}"
                        ),
                    )
                ],
            },
            {
                "name": "total-virus-db-annotated.tsv.gz",
                "old": {"rows": tsv_row_count(old_db_path)},
                "new": {"rows": tsv_row_count(new_db_path)},
                "metrics": [
                    (
                        "rows",
                        lambda n, signed=False: (
                            f"{'+' if signed and n > 0 else ''}{n:,}"
                        ),
                    )
                ],
            },
        ]

        transitions: dict[str, pd.DataFrame] = {}
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
            transitions[host] = trans
            changes = infection_status_changes(old_db, new_db, col)
            if coverage_available:
                changes = annotate_changes_with_coverage(
                    changes, host, parent_map, excluded_taxids, included_taxids
                )
            per_host_changes[host] = changes
            changes.to_csv(
                args.out / f"infection_status_changes_{host}.tsv", sep="\t", index=False
            )
            # Species-rank-only file for the actionable subset
            species_changes = changes[changes["rank"] == "species"]
            species_changes.to_csv(
                args.out / f"species_transitions_{host}.tsv", sep="\t", index=False
            )
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

    # Params changes + bidirectional flips for the summary.
    params_changes = summarise_params_changes(old_params, new_params)
    bidirectional_flips = detect_bidirectional_flips(per_host_changes)

    # Cross-host actionable annotation + genome-loss-driven demotion flag.
    # These add two columns to each per-host DataFrame for the inline
    # actionable tables (Appendix A.12) and re-persist the per-host TSVs.
    species_lost_taxids = set(species_lost["species_taxid"].astype(str))
    per_host_changes = annotate_cross_host_actionables(
        per_host_changes, species_lost_taxids
    )
    for host, df in per_host_changes.items():
        df.to_csv(
            args.out / f"infection_status_changes_{host}.tsv", sep="\t", index=False
        )
        species_changes = df[df["rank"] == "species"]
        species_changes.to_csv(
            args.out / f"species_transitions_{host}.tsv", sep="\t", index=False
        )

    # Per-genome-id categorization for §3.1 (lost / gained gids by reason).
    # The infection_status_human lookups are built from old_db / new_db
    # taxonomy rows; missing entries (taxid absent from a db) read as "".
    logger.info("Categorizing lost / gained genome IDs.")
    old_infection_human: dict[str, str] = {}
    new_infection_human: dict[str, str] = {}
    if "infection_status_human" in old_db.columns:
        old_infection_human = dict(
            zip(
                old_db["taxid"].astype(str),
                old_db["infection_status_human"].astype(str),
                strict=False,
            )
        )
    if "infection_status_human" in new_db.columns:
        new_infection_human = dict(
            zip(
                new_db["taxid"].astype(str),
                new_db["infection_status_human"].astype(str),
                strict=False,
            )
        )
    old_taxids = (
        set(old_db["taxid"].astype(str)) if "taxid" in old_db.columns else set()
    )
    lost_categorized = categorize_lost_genomes(
        removed_g,
        new_db,
        parent_map,
        excluded_taxids,
        old_infection_human,
        new_infection_human,
    )
    gained_categorized = categorize_gained_genomes(
        added_g,
        parent_map,
        included_taxids,
        old_infection_human,
        new_infection_human,
        old_taxids,
    )
    lost_categorized.to_csv(
        args.out / "genomes_lost_categorized.tsv", sep="\t", index=False
    )
    gained_categorized.to_csv(
        args.out / "genomes_gained_categorized.tsv", sep="\t", index=False
    )

    # Species that went from 0 → nonzero genomes (the gains counterpart to
    # species_lost_all_genomes). Used in §3.3 and Appendix A.11.
    species_gained = (
        by_species[(by_species["old_count"] == 0) & (by_species["new_count"] > 0)]
        .sort_values("new_count", ascending=False)
        .reset_index(drop=True)
    )
    species_gained.to_csv(
        args.out / "species_gained_all_genomes.tsv", sep="\t", index=False
    )

    # Summary
    write_summary_md(
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
        transitions,
        per_host_changes,
        coverage_available,
        params_changes,
        bidirectional_flips,
    )
    logger.info(f"Done. Outputs in {args.out.resolve()}")


if __name__ == "__main__":
    main()
