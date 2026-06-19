#!/usr/bin/env python3
DESC = """Compare two mgs-workflow index releases before promotion.

Accepts s3:// URIs or local directories for --old and --new, each pointing at
the root of an index release (the parent of `output/`).
"""

# Imports

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
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

# Logging


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


# Staging


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


def _write_json(path: Path, obj: object) -> None:
    """Write `obj` as pretty, key-sorted JSON."""
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


# 1. Reference Staleness


def latest_kraken_release() -> tuple[str, str] | None:
    """(date, filename) of the newest k2_standard_*.tar.gz in the public Kraken2
    bucket, or None on failure."""
    try:
        out = subprocess.run(["aws", "s3", "ls", "s3://genome-idx/kraken/", "--no-sign-request"], check=True, capture_output=True, text=True, timeout=15).stdout
    except (subprocess.SubprocessError, OSError):
        return None
    bundles = re.findall(r"\b(k2_standard_(\d{8})\.tar\.gz)\b", out)
    if not bundles:
        return None
    filename, date = max(bundles, key=lambda bundle: bundle[1])
    return date, filename


def latest_silva_release() -> str | None:
    """Highest release_NN[.M] directory in the SILVA FTP root, or None on failure."""
    try:
        with urllib.request.urlopen("https://ftp.arb-silva.de/", timeout=15) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError, TimeoutError):
        return None
    releases = {
        (int(m.group(1)), int(m.group(2) or 0))
        for m in re.finditer(r"release_(\d+)(?:[._](\d+))?", body)
    }
    if not releases:
        return None
    major, minor = max(releases)
    return f"{major}.{minor}" if minor else str(major)


def _staleness_row(
    ref: str,
    current: str,
    current_date: str = "",
    latest: str = "",
    latest_date: str = "",
    status: str = "error",
) -> dict[str, str]:
    """Build one reference-staleness output row."""
    return {"ref": ref, "current": current, "current_date": current_date, "latest": latest, "latest_date": latest_date, "status": status}


def check_kraken_staleness(new_params: dict) -> list[dict[str, str]]:
    """Compare the index's Kraken2 DB against the latest available release."""
    url = new_params.get("kraken_db", "")
    if not url:
        return []
    m = re.search(r"k2_standard_(\d{8})\.tar\.gz", url)
    current_date = m.group(1) if m else ""
    latest = latest_kraken_release()
    if latest is None:
        return [_staleness_row("kraken_db", url, current_date)]
    latest_date, latest_name = latest
    status = "current" if current_date == latest_date else "stale"
    return [
        _staleness_row("kraken_db", url, current_date, latest_name, latest_date, status)
    ]


def check_silva_staleness(new_params: dict) -> list[dict[str, str]]:
    """Compare the index's SILVA SSU/LSU refs against the latest release."""
    keys = [key for key in ("ssu_url", "lsu_url") if new_params.get(key)]
    if not keys:
        return []
    latest_rel = latest_silva_release()
    rows: list[dict[str, str]] = []
    for key in keys:
        url = new_params[key]
        m = re.search(r"release_(\d+(?:[._]\d+)?)", url)
        current_release = m.group(1).replace("_", ".") if m else ""
        if latest_rel is None:
            rows.append(_staleness_row(key, url, current_release))
            continue
        status = "current" if current_release == latest_rel else "stale"
        rows.append(_staleness_row(key, url, current_release, f"release_{latest_rel}", latest_rel, status))
    return rows


def write_staleness_table(new_params: dict, out_path: Path) -> None:
    """Check Kraken2/SILVA freshness for the new index and write staleness.tsv."""
    logger.info("Checking reference-DB staleness (Kraken2, SILVA).")
    rows = [*check_kraken_staleness(new_params), *check_silva_staleness(new_params)]
    pd.DataFrame(rows, columns=list(_staleness_row("", ""))).to_csv(out_path, sep="\t", index=False)


# 2. Size And Content Comparisons


def list_recursive_sizes(prefix: str) -> dict[str, int]:
    """Map each top-level entry under `prefix/output/results/` to its total bytes
    (directories summed; files keyed by basename). Accepts s3:// or local."""
    base = f"{prefix.rstrip('/')}/output/results/"
    sizes: Counter[str] = Counter()
    if prefix.startswith("s3://"):
        out = subprocess.run(["aws", "s3", "ls", "--recursive", base], check=True, capture_output=True, text=True).stdout
        prefix_key = base.split("/", 3)[-1]  # strip "s3://bucket/"
        for line in out.splitlines():
            parts = line.split()
            if len(parts) < 4 or parts[2] == "0":
                continue
            rel = parts[3].removeprefix(prefix_key)
            sizes[rel.split("/", 1)[0] or rel] += int(parts[2])
    else:
        base_path = Path(base)
        for f in base_path.rglob("*"):
            if f.is_file():
                sizes[f.relative_to(base_path).parts[0]] += f.stat().st_size
    return dict(sizes)


# Suffixes that get content metrics beyond byte size; gzip ratio varies with
# content, so compressed bytes alone can mislead.
_FASTA_SUFFIXES = (".fasta.gz", ".fasta", ".fa.gz", ".fa")
_TSV_SUFFIXES = (".tsv.gz", ".tsv")


def _content_stats(path: Path) -> dict[str, int] | None:
    """FASTA (records/bp/masked-bp) or TSV (data rows) stats for an optionally
    gzipped file; None for non-content suffixes."""
    name = path.name
    if not name.endswith(_FASTA_SUFFIXES + _TSV_SUFFIXES):
        return None
    with gzip.open(path, "rt") if name.endswith(".gz") else open(path) as f:
        if name.endswith(_TSV_SUFFIXES):
            return {"rows": max(sum(1 for _ in f) - 1, 0)}
        records = total_bp = n_bp = 0
        for line in f:
            if line.startswith(">"):
                records += 1
            else:
                seq = line.rstrip("\n")
                total_bp += len(seq)
                n_bp += seq.count("N") + seq.count("n")
        return {"records": records, "total_bp": total_bp, "n_bp": n_bp}


def collect_content_stats(
    old_prefix: str, new_prefix: str, names: list[str]
) -> dict[str, tuple[dict[str, int], dict[str, int]]]:
    """Fetch each named file from both indexes (into a temp dir) and return its
    old/new content stats, keyed by filename."""
    stats: dict[str, tuple[dict[str, int], dict[str, int]]] = {}
    with tempfile.TemporaryDirectory() as td:
        old_dir = Path(td) / "old"
        new_dir = Path(td) / "new"
        old_dir.mkdir()
        new_dir.mkdir()
        for name in names:
            subpath = f"output/results/{name}"
            old_stat = _content_stats(fetch(old_prefix, subpath, old_dir))
            new_stat = _content_stats(fetch(new_prefix, subpath, new_dir))
            if old_stat is not None and new_stat is not None:
                stats[name] = (old_stat, new_stat)
    return stats


def _metric_row(name: str, metric: str, old: int, new: int) -> dict[str, object]:
    """One long-format comparison row: old/new values, delta, and pct_change."""
    delta = new - old
    return {
        "name": name,
        "metric": metric,
        "old": old,
        "new": new,
        "delta": delta,
        "pct_change": round(delta / old * 100, 2) if old else float("nan"),
    }


def compare_metrics(
    old_sizes: dict[str, int],
    new_sizes: dict[str, int],
    content_stats: dict[str, tuple[dict[str, int], dict[str, int]]],
) -> pd.DataFrame:
    """Long-format comparison (columns: name, metric, old, new, delta, pct_change):
    a "bytes" row per output entry, ordered by absolute byte delta, with each
    file's content-metric rows following its byte row."""
    names = sorted(
        set(old_sizes) | set(new_sizes),
        key=lambda name: abs(new_sizes.get(name, 0) - old_sizes.get(name, 0)),
        reverse=True,
    )
    rows: list[dict[str, object]] = []
    for name in names:
        rows.append(_metric_row(name, "bytes", old_sizes.get(name, 0), new_sizes.get(name, 0)))
        old_stat, new_stat = content_stats.get(name, ({}, {}))
        for metric in old_stat:
            rows.append(_metric_row(name, metric, old_stat[metric], new_stat[metric]))
    return pd.DataFrame(rows)


def write_metrics_table(old_prefix: str, new_prefix: str, out_dir: Path) -> None:
    """Write the long-format size + content table (sizes.tsv; content files
    discovered as FASTA/TSV entries in both indexes) plus a sizes_summary.json
    count of shrunk / grown / unchanged entries."""
    logger.info("Listing per-DB sizes and content metrics.")
    old_sizes = list_recursive_sizes(old_prefix)
    new_sizes = list_recursive_sizes(new_prefix)
    content_files = sorted(name for name in set(old_sizes) & set(new_sizes) if name.endswith(_FASTA_SUFFIXES + _TSV_SUFFIXES))
    content_stats = collect_content_stats(old_prefix, new_prefix, content_files)
    metrics = compare_metrics(old_sizes, new_sizes, content_stats)
    metrics.to_csv(out_dir / "sizes.tsv", sep="\t", index=False)
    byte_delta = metrics.loc[metrics["metric"] == "bytes", "delta"]
    _write_json(
        out_dir / "sizes_summary.json",
        {
            "shrunk": int((byte_delta < 0).sum()),
            "grown": int((byte_delta > 0).sum()),
            "unchanged": int((byte_delta == 0).sum()),
        },
    )


# 3. Genome And Taxonomy Delta


GENOME_META_COLS = [
    "assembly_accession",
    "genome_id",
    "taxid",
    "species_taxid",
    "organism_name",
]


@dataclass
class Coverage:
    """Rule context used to explain genome and infection-status changes."""

    available: bool
    parent_map: dict[str, str]
    excluded_taxids: set[str]
    included_taxids: dict[str, set[str]]


def _lineage(taxid: str, parent_map: dict[str, str]) -> Iterator[str]:
    """Yield taxid and ancestors until the root, a missing parent, or a self-loop."""
    while taxid:
        yield taxid
        parent = parent_map.get(taxid)
        if parent is None or parent == taxid:
            break
        taxid = parent


def _ancestor_in(taxid: str, parent_map: dict[str, str], target: set[str]) -> str:
    """Return the first taxid or ancestor found in target, or an empty string."""
    return next((cur for cur in _lineage(taxid, parent_map) if cur in target), "")


def surveilled_taxids(db: pd.DataFrame, screened_hosts: list[str]) -> set[str]:
    """Return taxids passing any screened host, including species-rollup matches."""
    cols = [
        f"infection_status_{h}"
        for h in screened_hosts
        if f"infection_status_{h}" in db.columns
    ]
    if not cols or "taxid" not in db.columns:
        return set()
    positive = set(db.loc[(db[cols] == "1").any(axis=1), "taxid"].astype(str))
    if "taxid_species" in db.columns:
        positive |= set(
            db.loc[db["taxid_species"].astype(str).isin(positive), "taxid"].astype(str)
        )
    return positive


def genome_deltas(
    old_meta: pd.DataFrame, new_meta: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, set[str]]:
    """Return lost genomes, gained genomes, and genome IDs shared by both builds."""
    for df, label in [(old_meta, "old"), (new_meta, "new")]:
        missing = set(GENOME_META_COLS) - set(df.columns)
        if missing:
            raise ValueError(f"{label} metadata missing required columns: {missing}")

    old_ids = set(old_meta["genome_id"])
    new_ids = set(new_meta["genome_id"])
    lost = old_meta.loc[~old_meta["genome_id"].isin(new_ids), GENOME_META_COLS]
    gained = new_meta.loc[~new_meta["genome_id"].isin(old_ids), GENOME_META_COLS]
    return (
        lost.sort_values("organism_name").reset_index(drop=True),
        gained.sort_values("organism_name").reset_index(drop=True),
        old_ids & new_ids,
    )


def species_zero_crossings(
    old_meta: pd.DataFrame, new_meta: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return species that lost all genomes, and species that gained any genomes."""
    species_counts = (
        pd.concat(
            {
                "old_count": old_meta["species_taxid"].value_counts(),
                "new_count": new_meta["species_taxid"].value_counts(),
            },
            axis=1,
        )
        .fillna(0)
        .astype(int)
        .assign(delta=lambda df: df["new_count"] - df["old_count"])
        .rename_axis("species_taxid")
        .reset_index()
    )
    species_names = (
        pd.concat([old_meta, new_meta])
        .drop_duplicates("species_taxid", keep="last")
        .set_index("species_taxid")["organism_name"]
    )
    species_counts.insert(
        1, "organism_name", species_counts["species_taxid"].map(species_names)
    )
    lost = (
        species_counts.query("new_count == 0 and old_count > 0")
        .sort_values("old_count", ascending=False)
        .reset_index(drop=True)
    )
    gained = (
        species_counts.query("old_count == 0 and new_count > 0")
        .sort_values("new_count", ascending=False)
        .reset_index(drop=True)
    )
    return lost, gained


def reassignment_flows(old_meta: pd.DataFrame, new_meta: pd.DataFrame) -> pd.DataFrame:
    """Return old->new species assignment flows for genomes present in both builds."""
    reassigned = old_meta[["genome_id", "species_taxid"]].merge(
        new_meta[["genome_id", "species_taxid", "organism_name"]],
        on="genome_id",
        suffixes=("_old", "_new"),
    )
    reassigned = reassigned[
        reassigned["species_taxid_old"] != reassigned["species_taxid_new"]
    ]
    return (
        reassigned.groupby(["species_taxid_old", "species_taxid_new"], dropna=False)
        .agg(n_genomes=("genome_id", "size"), organism_name=("organism_name", "first"))
        .reset_index()
        .rename(
            columns={
                "species_taxid_old": "old_species_taxid",
                "species_taxid_new": "new_species_taxid",
            }
        )
        .sort_values("n_genomes", ascending=False)
        .reset_index(drop=True)
        [["old_species_taxid", "new_species_taxid", "organism_name", "n_genomes"]]
    )


def set_reason(
    out: pd.DataFrame, mask: pd.Series, label: str, taxids: pd.Series | str = ""
) -> None:
    """Assign a categorization reason and reason_taxid to matching rows."""
    out.loc[mask, "reason"] = label
    out.loc[mask, "reason_taxid"] = (
        taxids.loc[mask] if isinstance(taxids, pd.Series) else taxids
    )


def categorize_lost_genomes_raw(
    removed: pd.DataFrame,
    raw_meta: pd.DataFrame,
    new_db: pd.DataFrame,
    coverage: Coverage,
    screened_hosts: list[str],
) -> pd.DataFrame:
    """Categorize genomes lost from the filtered metadata using target raw metadata."""
    raw = raw_meta[["assembly_accession", "taxid", "assembly_status"]].rename(
        columns={"taxid": "_new_leaf", "assembly_status": "_new_status"}
    )
    out = removed.merge(raw, on="assembly_accession", how="left", sort=False)
    old_leaf = out["taxid"].astype(str)
    new_leaf = out["_new_leaf"].fillna("").astype(str)
    raw_present = out["_new_leaf"].notna()
    current = out["_new_status"] == "current"
    present_current = raw_present & current
    surveilled = new_leaf.isin(surveilled_taxids(new_db, screened_hosts))
    hard_exclude = new_leaf.map(
        lambda t: _ancestor_in(t, coverage.parent_map, coverage.excluded_taxids) if t else ""
    )
    unsurveilled = present_current & ~surveilled

    out["reason"] = "other"
    out["reason_taxid"] = new_leaf
    set_reason(
        out, unsurveilled & (new_leaf == old_leaf), "infection_status_demotion", new_leaf
    )
    set_reason(
        out, unsurveilled & (new_leaf != old_leaf), "reassigned_to_excluded", new_leaf
    )
    set_reason(out, present_current & (hard_exclude != ""), "hard_excluded", hard_exclude)
    set_reason(out, raw_present & ~current, "non_current_genome_version")
    set_reason(out, ~raw_present, "absent_from_ncbi")
    return out.drop(columns=["_new_leaf", "_new_status"])


def categorize_gained_genomes_raw(
    added: pd.DataFrame,
    raw_meta: pd.DataFrame,
    old_db: pd.DataFrame,
    coverage: Coverage,
    screened_hosts: list[str],
    old_build_date: str,
) -> pd.DataFrame:
    """Categorize genomes gained in the filtered metadata using target raw metadata."""
    raw = raw_meta[["assembly_accession", "release_date", "source_database"]].rename(
        columns={"release_date": "_release_date"}
    )
    out = added.merge(raw, on="assembly_accession", how="left", sort=False)
    new_leaf = out["taxid"].fillna("").astype(str)
    release = out["_release_date"].fillna("").astype(str)
    all_included = set().union(*coverage.included_taxids.values()) if coverage.included_taxids else set()
    hard_include = new_leaf.map(
        lambda t: _ancestor_in(t, coverage.parent_map, all_included) if t else ""
    )
    old_db_taxids = (
        set(old_db["taxid"].astype(str)) if "taxid" in old_db.columns else set()
    )
    old_surveilled = new_leaf.isin(surveilled_taxids(old_db, screened_hosts))

    out["reason"] = "no_release_date"
    out["reason_taxid"] = new_leaf
    set_reason(out, release != "", "pre_existing_reincluded", new_leaf)
    set_reason(out, ~old_surveilled, "infection_status_promotion", new_leaf)
    set_reason(out, ~new_leaf.isin(old_db_taxids), "new_taxon_in_taxonomy", new_leaf)
    set_reason(out, hard_include != "", "hard_included", hard_include)
    set_reason(
        out, (release != "") & (release > old_build_date), "newly_deposited", new_leaf
    )
    out["source_database"] = out["source_database"].fillna("").astype(str)
    return out.drop(columns=["_release_date"])


def _reason_counts(df: pd.DataFrame) -> dict[str, int]:
    """Return reason value counts as a JSON-serializable dict."""
    return {} if df.empty else {str(k): int(v) for k, v in df["reason"].value_counts().items()}


def write_genome_taxonomy_tables(
    out_dir: Path,
    old_meta: pd.DataFrame,
    new_meta: pd.DataFrame,
    new_raw_meta: pd.DataFrame,
    old_db: pd.DataFrame,
    new_db: pd.DataFrame,
    coverage: Coverage,
    screened_hosts: list[str],
    old_build_date: str,
) -> None:
    """Write genome lost/gained categorization tables and summary counts."""
    logger.info("Diffing genome metadata and taxonomy; categorizing genome IDs.")
    if "release_date" not in new_raw_meta.columns:
        raise ValueError(
            "Target index raw metadata lacks release_date, required for gained-genome"
            " categorization."
        )

    lost_g, gained_g, shared_genomes = genome_deltas(old_meta, new_meta)
    species_lost, species_gained = species_zero_crossings(old_meta, new_meta)
    species_lost["covered_by_hard_exclude"] = (
        species_lost["species_taxid"]
        .astype(str)
        .apply(lambda t: _ancestor_in(t, coverage.parent_map, coverage.excluded_taxids))
        if coverage.available
        else ""
    )
    species_lost.to_csv(out_dir / "species_lost_all_genomes.tsv", sep="\t", index=False)
    species_gained.to_csv(
        out_dir / "species_gained_all_genomes.tsv", sep="\t", index=False
    )

    reassigned = reassignment_flows(old_meta, new_meta)
    reassigned.to_csv(out_dir / "genomes_reassigned.tsv", sep="\t", index=False)

    old_taxids = set(old_db["taxid"].astype(str))
    new_taxids = set(new_db["taxid"].astype(str))

    lost = categorize_lost_genomes_raw(
        lost_g, new_raw_meta, new_db, coverage, screened_hosts
    )
    gained = categorize_gained_genomes_raw(
        gained_g, new_raw_meta, old_db, coverage, screened_hosts, old_build_date
    )
    lost.to_csv(out_dir / "genomes_lost_categorized.tsv", sep="\t", index=False)
    gained.to_csv(out_dir / "genomes_gained_categorized.tsv", sep="\t", index=False)

    _write_json(
        out_dir / "genomes_summary.json",
        {
            "lost_total": len(lost),
            "gained_total": len(gained),
            "lost_by_reason": _reason_counts(lost),
            "gained_by_reason": _reason_counts(gained),
            "species_lost_all_genomes": len(species_lost),
            "species_gained_all_genomes": len(species_gained),
            "reassigned_genomes": int(reassigned["n_genomes"].sum())
            if not reassigned.empty
            else 0,
            "kept_genomes": len(shared_genomes),
            "taxa_added": len(new_taxids - old_taxids),
            "taxa_removed": len(old_taxids - new_taxids),
        },
    )


# 4. Infection Status Changes


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


def infection_status_columns(db: pd.DataFrame) -> list[str]:
    """Return infection-status annotation columns in a taxonomy DB."""
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
    for cur in _lineage(taxid, parent_map):
        if cur in excluded_taxids:
            return "excluded", cur
        if cur in host_includes:
            return "included", cur
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
    return sorted(
        h
        for h, taxids in included_taxids.items()
        if h != host and any(cur in taxids for cur in _lineage(taxid, parent_map))
    )


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


def _zero_species_transition_counts() -> dict[str, int]:
    """Return an all-zero species transition summary."""
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
    """Summarize species-level promotion, demotion, and coverage counts by host."""
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


def write_infection_status_tables(
    out_dir: Path,
    old_db: pd.DataFrame,
    new_db: pd.DataFrame,
    coverage: Coverage,
) -> None:
    """Write per-host infection-status transitions/changes tables plus
    infection_status_summary.json."""
    logger.info("Diffing infection-status annotations.")
    host_cols = sorted(
        set(infection_status_columns(old_db)) & set(infection_status_columns(new_db))
    )
    per_host_changes: dict[str, pd.DataFrame] = {}
    transitions = []
    for col in host_cols:
        host = col.removeprefix("infection_status_")
        trans = infection_status_transitions(old_db, new_db, col)
        if not trans.empty:
            trans.insert(0, "host", host)
            transitions.append(trans)
        changes = infection_status_changes(old_db, new_db, col)
        if coverage.available:
            changes = annotate_changes_with_coverage(
                changes,
                host,
                coverage.parent_map,
                coverage.excluded_taxids,
                coverage.included_taxids,
            )
        per_host_changes[host] = changes
    if transitions:
        pd.concat(transitions, ignore_index=True).to_csv(
            out_dir / "infection_status_transitions.tsv", sep="\t", index=False
        )
    else:
        (out_dir / "infection_status_transitions.tsv").write_text(
            "host\told\tnew\tcount\n"
        )
    for host, df in per_host_changes.items():
        df.to_csv(
            out_dir / f"infection_status_changes_{host}.tsv", sep="\t", index=False
        )
        df[df["rank"] == "species"].to_csv(
            out_dir / f"species_transitions_{host}.tsv", sep="\t", index=False
        )
    _write_json(
        out_dir / "infection_status_summary.json",
        {
            "coverage_available": coverage.available,
            "hosts": _species_transition_counts(per_host_changes, coverage.available),
        },
    )


# 5. Params Diff


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


# Main


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=DESC, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--old", required=True, help="Old index root, parent of output/.")
    parser.add_argument("--new", required=True, help="New index root, parent of output/.")
    parser.add_argument("--out", type=Path, required=True, help="Output directory.")
    parser.add_argument("--repo-root", type=Path, default=None, help="mgs-workflow checkout for coverage annotations.")
    return parser.parse_args()


def main() -> None:
    """Run the benchmark comparison and write output tables."""
    args = parse_arguments()
    args.out.mkdir(parents=True, exist_ok=True)
    logger.info(f"Benchmarking {args.old} -> {args.new}")

    write_metrics_table(args.old, args.new, args.out)

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "old").mkdir()
        (td / "new").mkdir()

        def stage(prefix: str, subpath: str, side: str) -> Path:
            """Fetch one index artifact into the temporary side directory."""
            return fetch(prefix, subpath, td / side)

        # Stage genome metadata, the pre-filter raw metadata (required by the
        # categorizers), the taxonomy DB, and params.
        logger.info("Staging genome metadata, taxonomy DB, and params.")
        gid = "output/results/virus-genome-metadata-gid.tsv.gz"
        db = "output/results/total-virus-db-annotated.tsv.gz"
        params = "output/input/index-params.json"
        old_meta = pd.read_csv(stage(args.old, gid, "old"), sep="\t", dtype=str)
        new_meta = pd.read_csv(stage(args.new, gid, "new"), sep="\t", dtype=str)
        try:
            raw_path = stage(
                args.new, "output/results/virus-genome-metadata-raw.tsv.gz", "new"
            )
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            raise ValueError(
                "Target index has no output/results/virus-genome-metadata-raw.tsv.gz,"
                " which is required for genome-ID categorization. Rebuild the index"
                " with a pipeline version that publishes the pre-filter assembly"
                " metadata."
            ) from exc
        new_raw_meta = pd.read_csv(raw_path, sep="\t", dtype=str)
        old_db = pd.read_csv(stage(args.old, db, "old"), sep="\t", dtype=str)
        new_db = pd.read_csv(stage(args.new, db, "new"), sep="\t", dtype=str)
        old_params = json.loads(stage(args.old, params, "old").read_text())
        new_params = json.loads(stage(args.new, params, "new").read_text())
        (args.out / "params_diff.txt").write_text(diff_params(old_params, new_params))
        summarise_params_changes(old_params, new_params).to_csv(
            args.out / "params_changes.tsv", sep="\t", index=False
        )

        # Coverage rule data: hard-exclude from the new params, hard-include from
        # the repo overrides. Skipped (and empty) without --repo-root.
        coverage = Coverage(
            available=args.repo_root is not None,
            parent_map=dict(zip(new_db["taxid"], new_db["parent_taxid"], strict=False)),
            excluded_taxids=set(new_params.get("viral_taxids_exclude_hard", "").split())
            if args.repo_root is not None
            else set(),
            included_taxids=load_existing_overrides(args.repo_root)
            if args.repo_root is not None
            else {},
        )

        write_genome_taxonomy_tables(
            args.out,
            old_meta,
            new_meta,
            new_raw_meta,
            old_db,
            new_db,
            coverage,
            new_params.get("host_taxa_screen", "").split(),
            str(old_params.get("trace_timestamp", ""))[:10],
        )
        write_infection_status_tables(args.out, old_db, new_db, coverage)
        write_staleness_table(new_params, args.out / "staleness.tsv")

    logger.info(f"Done. Outputs in {args.out.resolve()}")


if __name__ == "__main__":
    main()
