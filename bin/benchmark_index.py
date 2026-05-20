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
import json
import logging
import shutil
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

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


def annotate_lost_genomes_with_renames(
    lost: pd.DataFrame, new_db: pd.DataFrame
) -> pd.DataFrame:
    """For each species in the lost-all-genomes table, check whether its
    species_taxid still exists in the new annotated virus DB and was renamed.
    Mass NCBI / ICTV reorgs (e.g. binomial nomenclature for circoviruses,
    cycloviruses, hantaviruses) re-key species concepts under different taxids,
    leaving the OLD species_taxid pointing at a different species — the genomes
    haven't been culled, they just moved.

    Adds two columns:
    - `new_taxonomy_name`: the name of this taxid in the new taxonomy DB (or "")
    - `likely_rename`: "yes" if the new name differs from the old organism_name
      (i.e. taxid was repurposed); "no" if the name is unchanged or the taxid
      is gone from the new taxonomy DB.
    """
    out = lost.copy()
    if out.empty:
        out["new_taxonomy_name"] = pd.Series(dtype=str)
        out["likely_rename"] = pd.Series(dtype=str)
        return out
    new_name_lookup = dict(zip(new_db["taxid"], new_db["name"], strict=False))
    out["new_taxonomy_name"] = out["species_taxid"].map(new_name_lookup).fillna("")
    out["likely_rename"] = (
        (out["new_taxonomy_name"] != "")
        & (out["new_taxonomy_name"] != out["organism_name"])
    ).map({True: "yes", False: "no"})
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


################
# REPORT WRITER #
################


def write_summary_md(
    out_dir: Path,
    old: str,
    new: str,
    sizes: pd.DataFrame,
    added_genomes: pd.DataFrame,
    removed_genomes: pd.DataFrame,
    species_lost: pd.DataFrame,
    added_taxa: pd.DataFrame,
    removed_taxa: pd.DataFrame,
    transitions: dict[str, pd.DataFrame],
    per_host_changes: dict[str, pd.DataFrame],
    coverage_available: bool,
) -> None:
    """Write a human-readable summary referencing the TSVs. Designed so the
    skill's REVIEW.md can quote it directly without re-doing the arithmetic."""
    lines = [
        "# Index benchmark report",
        "",
        f"- **old**: `{old}`",
        f"- **new**: `{new}`",
        f"- generated: {datetime.now(UTC).isoformat(timespec='seconds')}",
        "",
        "## Per-DB size",
        "",
    ]
    shrunk = sizes[sizes["delta_bytes"] < 0]
    grown = sizes[sizes["delta_bytes"] > 0]
    same = sizes[sizes["delta_bytes"] == 0]
    lines += [
        f"{len(shrunk)} entries shrunk, {len(grown)} grew, {len(same)} unchanged. Full table in `sizes.tsv`; entries that changed by ≥1 byte:",
        "",
        "| DB | Old | New | Δ |",
        "|---|---:|---:|---:|",
    ]
    for _, row in sizes[sizes["delta_bytes"] != 0].iterrows():
        pct = "" if pd.isna(row["pct_change"]) else f" ({row['pct_change']:+.1f}%)"
        lines.append(
            f"| `{row['name']}` | {_fmt_bytes(row['old_bytes'])} | {_fmt_bytes(row['new_bytes'])} | {_fmt_bytes(row['delta_bytes'], signed=True)}{pct} |"
        )
    if len(shrunk):
        lines += [
            "",
            f"**Shrunk DBs (flag for review)**: {', '.join(f'`{n}`' for n in shrunk['name'])}.",
        ]
    # Lost-genomes triage. Renames vs true losses computed by
    # annotate_lost_genomes_with_renames upstream.
    if "likely_rename" in species_lost.columns:
        true_losses = species_lost[species_lost["likely_rename"] == "no"]
        renames = species_lost[species_lost["likely_rename"] == "yes"]
    else:
        true_losses = species_lost
        renames = species_lost.iloc[0:0]
    lines += [
        "",
        "## Virus genomes",
        "",
        f"- **{len(added_genomes):,} genomes added, {len(removed_genomes):,} removed** (by NCBI `genome_id`). See `genomes_added.tsv`, `genomes_removed.tsv`.",
        f"- **{len(species_lost)} species had their genome count drop to zero in the new index.** {len(renames)} of those are likely NCBI/ICTV taxonomy renames (the same `species_taxid` still exists in the new taxonomy DB but now points to a different species concept — the genomes likely moved with the new species). {len(true_losses)} appear to be true losses (taxid kept the same name, or taxid disappeared from the new taxonomy DB).",
        "",
        "See `species_lost_all_genomes.tsv` (full list, with `likely_rename` and `new_taxonomy_name` columns to triage each row). Top true losses by genome count:",
        "",
        "| `species_taxid` | Organism | Genomes before | Genomes after |",
        "|---|---|---:|---:|",
    ]
    for _, row in true_losses.head(10).iterrows():
        lines.append(
            f"| `{row['species_taxid']}` | *{row['organism_name']}* | {row['old_count']} | 0 |"
        )
    if len(true_losses) > 10:
        lines.append(
            f"_…and {len(true_losses) - 10} more true losses; see `species_lost_all_genomes.tsv`._"
        )
    lines += [
        "",
        "## Virus taxonomy DB",
        "",
        f"- {len(added_taxa):,} taxa added, {len(removed_taxa):,} removed. Driven by NCBI taxonomy churn. See `taxa_added.tsv`, `taxa_removed.tsv`.",
        "",
        "## Infection-status annotation changes (shared taxa)",
        "",
    ]
    if coverage_available:
        lines += [
            "Each viral taxon carries an `infection_status_<host>` column per host group (`human`, `primate`, `mammal`, `vertebrate`, `bird`); `1` = infecting, `0` = not, `2` = unknown, `3` = likely. The table below summarises how those annotations changed between indexes, after filtering to species-rank transitions only (which is where the actionable signal lives — strain-level transitions are mostly downstream propagation noise).",
            "",
            "**Already-handled** transitions are explained by an existing rule in `viral_taxids_exclude_hard` (forces the taxon and its descendants to `0`) or `ref/host-infection-overrides.json` (forces the listed taxa to `1`).  **Needs review** transitions aren't — those are the rows worth investigating.",
            "",
        ]
    else:
        lines += [
            "Each viral taxon carries an `infection_status_<host>` column per host group; the table below summarises how those changed between indexes at species rank. Run with `--repo-root <mgs-workflow>` to annotate which transitions are already covered by existing exclude/override rules.",
            "",
        ]
    if coverage_available:
        lines += [
            "| Host | Total transitions (all ranks) | Species 1→0 demotions (needs review) | Species 0→1 promotions (needs review) | Override policy gaps* |",
            "|---|---:|---:|---:|---:|",
        ]
    else:
        lines += [
            "| Host | Total transitions (all ranks) | Species 1→0 demotions | Species 0→1 promotions |",
            "|---|---:|---:|---:|",
        ]
    for host, df in transitions.items():
        n_changes = int(df["count"].sum()) if not df.empty else 0
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
            lines.append(
                f"| `{host}` | {n_changes:,} | **{len(dem_actionable)}** ({len(species_demotions)} total) | **{len(pro_actionable)}** ({len(species_promotions)} total) | {len(dem_policy)} |"
            )
        else:
            lines.append(
                f"| `{host}` | {n_changes:,} | {len(species_demotions)} | {len(species_promotions)} |"
            )
    if coverage_available:
        lines += [
            "",
            "_*Override policy gap = an uncovered demotion whose `species_taxid` IS in `ref/host-infection-overrides.json`, but only for other hosts. The override entry's `hosts` list excludes the host that just demoted; either the entry needs widening or the demotion is acceptable for that host._",
            "",
            "Per-host actionable detail in `species_transitions_<host>.tsv` (species rank, with `covered_by` and `included_for_other_hosts` columns). For all-rank detail see `infection_status_changes_<host>.tsv`.",
        ]
    else:
        lines.append(
            "Per-host detail in `species_transitions_<host>.tsv` and `infection_status_changes_<host>.tsv`."
        )
    lines += [
        "",
        "## Params diff",
        "",
        "Diff of `output/input/index-params.json` between the two indexes is in `params_diff.txt`. Look for `kraken_db` URL changes (Kraken DB version bumps), new/removed `params.*` keys (workflow surface changes), and pipeline-version bumps.",
        "",
        "---",
        "",
        "_Generated by `bin/benchmark_index.py`. To turn the per-host counts into a written review, walk the `.claude/skills/benchmark-index/` skill against this directory._",
        "",
    ]
    (out_dir / "summary.md").write_text("\n".join(lines))


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

        # Lost-genomes triage: distinguish true losses from NCBI/ICTV renames
        # (taxid kept, name swapped → genomes moved to a different species concept)
        species_lost = annotate_lost_genomes_with_renames(species_lost, new_db)
        species_lost.to_csv(
            args.out / "species_lost_all_genomes.tsv", sep="\t", index=False
        )

        # Fetch params now so we can use new_params for coverage classification
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

    # Summary
    write_summary_md(
        args.out,
        args.old,
        args.new,
        sizes,
        added_g,
        removed_g,
        species_lost,
        added_t,
        removed_t,
        transitions,
        per_host_changes,
        coverage_available,
    )
    logger.info(f"Done. Outputs in {args.out.resolve()}")


if __name__ == "__main__":
    main()
