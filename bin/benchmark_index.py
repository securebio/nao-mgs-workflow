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
from dataclasses import dataclass
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


def fetch(prefix: str, subpath: str, local_dir: Path) -> Path:
    """Stage `prefix/subpath` to `local_dir/<basename>` and return the local path."""
    src = f"{prefix.rstrip('/')}/{subpath}"
    dst = local_dir / Path(subpath).name
    local_dir.mkdir(parents=True, exist_ok=True)
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


##########################
# 1. REFERENCE STALENESS #
##########################


def latest_kraken_release() -> tuple[str, str] | None:
    """(date, filename) of the newest k2_standard_*.tar.gz in the public Kraken2
    bucket, or None on failure."""
    try:
        out = subprocess.run(
            ["aws", "s3", "ls", "s3://genome-idx/kraken/", "--no-sign-request"],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout
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


STALENESS_COLS = "ref", "current", "current_date", "latest", "latest_date", "status"


def _stale(
    ref: str,
    current: str,
    current_date: str = "",
    latest: str = "",
    latest_date: str = "",
    status: str = "error",
) -> dict[str, str]:
    """Return one staleness.tsv row with stable column names."""
    values = ref, current, current_date, latest, latest_date, status
    return dict(zip(STALENESS_COLS, values, strict=False))


def check_kraken_staleness(new_params: dict) -> list[dict[str, str]]:
    """Compare the index's Kraken2 DB against the latest available release."""
    url = new_params.get("kraken_db", "")
    if not url:
        return []
    m = re.search(r"k2_standard_(\d{8})\.tar\.gz", url)
    current_date = m.group(1) if m else ""
    latest = latest_kraken_release()
    if latest is None:
        return [_stale("kraken_db", url, current_date)]
    latest_date, latest_name = latest
    status = "current" if current_date == latest_date else "stale"
    return [_stale("kraken_db", url, current_date, latest_name, latest_date, status)]


def check_silva_staleness(new_params: dict) -> list[dict[str, str]]:
    """Compare the index's SILVA SSU/LSU refs against the latest release."""
    keys = [key for key in ("ssu_url", "lsu_url") if new_params.get(key)]
    if not keys:
        return []
    latest = latest_silva_release()
    rows: list[dict[str, str]] = []
    for key in keys:
        url = new_params[key]
        m = re.search(r"release_(\d+(?:[._]\d+)?)", url)
        current = m.group(1).replace("_", ".") if m else ""
        if latest is None:
            rows.append(_stale(key, url, current))
            continue
        status = "current" if current == latest else "stale"
        rows.append(_stale(key, url, current, f"release_{latest}", latest, status))
    return rows


def write_staleness_table(new_params: dict, out_path: Path) -> None:
    """Check Kraken2/SILVA freshness for the new index and write staleness.tsv."""
    logger.info("Checking reference-DB staleness (Kraken2, SILVA).")
    rows = [*check_kraken_staleness(new_params), *check_silva_staleness(new_params)]
    pd.DataFrame(rows, columns=STALENESS_COLS).to_csv(out_path, sep="\t", index=False)


###################################
# 2. SIZE AND CONTENT COMPARISONS #
###################################


def list_recursive_sizes(prefix: str) -> dict[str, int]:
    """Map each top-level entry under `prefix/output/results/` to its total bytes
    (directories summed; files keyed by basename). Accepts s3:// or local."""
    base = f"{prefix.rstrip('/')}/output/results/"
    sizes: Counter[str] = Counter()
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
        temp_dir = Path(td)
        for name in names:
            subpath = f"output/results/{name}"
            old_stat = _content_stats(fetch(old_prefix, subpath, temp_dir))
            new_stat = _content_stats(fetch(new_prefix, subpath, temp_dir))
            if old_stat is not None and new_stat is not None:
                stats[name] = (old_stat, new_stat)
    return stats


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
    rows: list[tuple[str, str, int, int]] = []
    for name in names:
        rows.append((name, "bytes", old_sizes.get(name, 0), new_sizes.get(name, 0)))
        old_stat, new_stat = content_stats.get(name, ({}, {}))
        for metric in old_stat:
            rows.append((name, metric, old_stat[metric], new_stat[metric]))
    out = pd.DataFrame(rows, columns=["name", "metric", "old", "new"])
    out["delta"] = out["new"] - out["old"]
    out["pct_change"] = (out["delta"] / out["old"] * 100).round(2)
    out.loc[out["old"].eq(0), "pct_change"] = float("nan")
    return out


def write_metrics_table(old_prefix: str, new_prefix: str, out_dir: Path) -> None:
    """Write the long-format size + content table (sizes.tsv; content files
    discovered as FASTA/TSV entries in both indexes) plus a sizes_summary.json
    count of shrunk / grown / unchanged entries."""
    logger.info("Listing per-DB sizes and content metrics.")
    old_sizes = list_recursive_sizes(old_prefix)
    new_sizes = list_recursive_sizes(new_prefix)
    content_files = sorted(
        name
        for name in set(old_sizes) & set(new_sizes)
        if name.endswith(_FASTA_SUFFIXES + _TSV_SUFFIXES)
    )
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


################################
# 3. GENOME AND TAXONOMY DELTA #
################################

META_COLS = "assembly_accession", "genome_id", "taxid", "species_taxid", "organism_name"
LOSS_REASONS = (
    "absent_from_ncbi",
    "non_current_genome_version",
    "hard_excluded",
    "reassigned_to_excluded",
    "infection_status_demotion",
    "other",
)
GAIN_REASONS = (
    "newly_deposited",
    "hard_included",
    "new_taxon_in_taxonomy",
    "infection_status_promotion",
    "pre_existing_reincluded",
    "no_release_date",
)


@dataclass
class Coverage:
    """Rule context used to explain genome and infection-status changes."""

    available: bool
    parent_map: dict[str, str]
    excluded_taxids: set[str]
    included_taxids: dict[str, set[str]]


def build_parent_map(new_db: pd.DataFrame) -> dict[str, str]:
    """Return taxid -> parent_taxid lookup from the annotated taxonomy DB."""
    return dict(zip(new_db["taxid"], new_db["parent_taxid"], strict=False))


def _ancestor_in(taxid: str, parent_map: dict[str, str], target: set[str]) -> str:
    """Return the first taxid or ancestor found in target, or an empty string."""
    while taxid:
        if taxid in target:
            return taxid
        parent = parent_map.get(taxid)
        if parent is None or parent == taxid:
            break
        taxid = parent
    return ""


def surveilled_taxids(db: pd.DataFrame, hosts: list[str]) -> set[str]:
    """Return taxids passing any screened host, including species-rollup matches."""
    cols = [f"infection_status_{h}" for h in hosts]
    cols = [c for c in cols if c in db]
    if not cols or "taxid" not in db.columns:
        return set()
    positive = set(db.loc[(db[cols] == "1").any(axis=1), "taxid"].astype(str))
    if "taxid_species" in db.columns:
        species = db["taxid_species"].astype(str)
        positive.update(db.loc[species.isin(positive), "taxid"].astype(str))
    return positive


def metadata_deltas(
    old_meta: pd.DataFrame, new_meta: pd.DataFrame
) -> tuple[
    pd.DataFrame, pd.DataFrame, set[str], pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """Return genome, species, and reassignment deltas from metadata tables."""
    for label, df in {"old": old_meta, "new": new_meta}.items():
        missing = set(META_COLS) - set(df.columns)
        if missing:
            raise ValueError(f"{label} metadata missing required columns: {missing}")
    old_ids, new_ids = set(old_meta["genome_id"]), set(new_meta["genome_id"])
    lost = old_meta.loc[~old_meta["genome_id"].isin(new_ids), META_COLS]
    gained = new_meta.loc[~new_meta["genome_id"].isin(old_ids), META_COLS]
    old_counts = old_meta["species_taxid"].value_counts()
    new_counts = new_meta["species_taxid"].value_counts()
    species = pd.concat([old_counts, new_counts], axis=1).fillna(0).astype(int)
    species.columns = ["old_count", "new_count"]
    species["delta"] = species["new_count"] - species["old_count"]
    species = species.rename_axis("species_taxid").reset_index()
    names = (
        pd.concat([old_meta, new_meta])
        .drop_duplicates("species_taxid", keep="last")
        .set_index("species_taxid")["organism_name"]
    )
    species.insert(1, "organism_name", species["species_taxid"].map(names))
    species_lost = species[species["new_count"].eq(0) & species["old_count"].gt(0)]
    species_lost = species_lost.sort_values("old_count", ascending=False)
    species_gained = species[species["old_count"].eq(0) & species["new_count"].gt(0)]
    species_gained = species_gained.sort_values("new_count", ascending=False)
    reassigned = old_meta[["genome_id", "species_taxid"]].merge(
        new_meta[["genome_id", "species_taxid", "organism_name"]],
        on="genome_id",
        suffixes=("_old", "_new"),
    )
    reassigned = reassigned[
        reassigned["species_taxid_old"] != reassigned["species_taxid_new"]
    ]
    reassigned = reassigned.rename(
        columns={
            "species_taxid_old": "old_species_taxid",
            "species_taxid_new": "new_species_taxid",
        }
    )
    reassigned = (
        reassigned.groupby(["old_species_taxid", "new_species_taxid"], dropna=False)
        .agg(n_genomes=("genome_id", "size"), organism_name=("organism_name", "first"))
        .reset_index()
        .sort_values("n_genomes", ascending=False)
    )
    reassigned_cols = [
        "old_species_taxid",
        "new_species_taxid",
        "organism_name",
        "n_genomes",
    ]
    lost = lost.sort_values("organism_name")
    gained = gained.sort_values("organism_name")
    return (
        lost,
        gained,
        old_ids & new_ids,
        species_lost,
        species_gained,
        reassigned[reassigned_cols],
    )


def set_reason(
    out: pd.DataFrame, mask: pd.Series, label: str, taxids: pd.Series | str = ""
) -> None:
    """Assign reason; later calls overwrite earlier lower-priority matches."""
    out.loc[mask, "reason"] = label
    out.loc[mask, "reason_taxid"] = (
        taxids.loc[mask] if isinstance(taxids, pd.Series) else taxids
    )


def reason_counts(out: pd.DataFrame, labels: tuple[str, ...]) -> dict[str, int]:
    """Return reason counts with explicit zeroes for absent categories."""
    counts = out["reason"].value_counts()
    return {label: int(counts.get(label, 0)) for label in labels}


def categorize_loss(
    removed: pd.DataFrame,
    raw_meta: pd.DataFrame,
    new_db: pd.DataFrame,
    cov: Coverage,
    hosts: list[str],
) -> pd.DataFrame:
    """Categorize genomes lost from the filtered metadata using target raw metadata."""
    raw = raw_meta[["assembly_accession", "taxid", "assembly_status"]].rename(
        columns={"taxid": "_new_leaf", "assembly_status": "_new_status"}
    )
    out = removed.merge(raw, on="assembly_accession", how="left", sort=False)
    old = out["taxid"].astype(str)
    new = out["_new_leaf"].fillna("").astype(str)
    in_raw = out["_new_leaf"].notna()
    current = out["_new_status"].eq("current")
    present = in_raw & current
    surveilled = new.isin(surveilled_taxids(new_db, hosts))
    excluded = new.map(lambda t: _ancestor_in(t, cov.parent_map, cov.excluded_taxids))
    unsurveilled = present & ~surveilled
    out["reason"], out["reason_taxid"] = "other", new
    set_reason(out, unsurveilled & new.eq(old), "infection_status_demotion", new)
    set_reason(out, unsurveilled & new.ne(old), "reassigned_to_excluded", new)
    set_reason(out, present & excluded.ne(""), "hard_excluded", excluded)
    set_reason(out, in_raw & ~current, "non_current_genome_version")
    set_reason(out, ~in_raw, "absent_from_ncbi")
    return out.drop(columns=["_new_leaf", "_new_status"])


def categorize_gain(
    added: pd.DataFrame,
    raw_meta: pd.DataFrame,
    old_db: pd.DataFrame,
    cov: Coverage,
    hosts: list[str],
    old_date: str,
) -> pd.DataFrame:
    """Categorize genomes gained in the filtered metadata using target raw metadata."""
    raw = raw_meta[["assembly_accession", "release_date", "source_database"]].rename(
        columns={"release_date": "_release_date"}
    )
    out = added.merge(raw, on="assembly_accession", how="left", sort=False)
    new = out["taxid"].fillna("").astype(str)
    release = out["_release_date"].fillna("").astype(str)
    included = set().union(*cov.included_taxids.values())
    hard_include = new.map(lambda t: _ancestor_in(t, cov.parent_map, included))
    old_taxids = set(old_db["taxid"].astype(str)) if "taxid" in old_db else set()
    old_surveilled = new.isin(surveilled_taxids(old_db, hosts))
    out["reason"], out["reason_taxid"] = "no_release_date", new
    set_reason(out, release != "", "pre_existing_reincluded", new)
    set_reason(out, ~old_surveilled, "infection_status_promotion", new)
    set_reason(out, ~new.isin(old_taxids), "new_taxon_in_taxonomy", new)
    set_reason(out, hard_include != "", "hard_included", hard_include)
    set_reason(out, release.ne("") & release.gt(old_date), "newly_deposited", new)
    out["source_database"] = out["source_database"].fillna("").astype(str)
    return out.drop(columns=["_release_date"])


def write_genome_taxonomy_tables(
    out_dir: Path,
    old: str,
    new: str,
    old_db: pd.DataFrame,
    new_db: pd.DataFrame,
    cov: Coverage,
    old_params: dict[str, Any],
    new_params: dict[str, Any],
    work_dir: Path,
) -> None:
    """Stage genome metadata and write genome/taxonomy delta outputs."""
    logger.info("Diffing genome metadata and taxonomy; categorizing genome IDs.")
    gid = "output/results/virus-genome-metadata-gid.tsv.gz"
    raw = "output/results/virus-genome-metadata-raw.tsv.gz"
    old_meta = pd.read_csv(fetch(old, gid, work_dir / "old"), sep="\t", dtype=str)
    new_meta = pd.read_csv(fetch(new, gid, work_dir / "new"), sep="\t", dtype=str)
    try:
        raw_path = fetch(new, raw, work_dir / "new")
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        raise ValueError(
            "Target index has no output/results/virus-genome-metadata-raw.tsv.gz,"
            " which is required for genome-ID categorization. Rebuild the index"
            " with a pipeline version that publishes the pre-filter assembly"
            " metadata."
        ) from exc
    new_raw_meta = pd.read_csv(raw_path, sep="\t", dtype=str)
    old_cols = set(old_meta.columns)
    new_cols = set(new_meta.columns)
    schema_rows = [
        ("removed", col) for col in old_meta.columns if col not in new_cols
    ] + [("added", col) for col in new_meta.columns if col not in old_cols]
    pd.DataFrame(schema_rows, columns=["change", "column"]).to_csv(
        out_dir / "metadata_schema_diff.tsv", sep="\t", index=False
    )
    schema_counts = Counter(change for change, _ in schema_rows)
    _write_json(
        out_dir / "metadata_schema_summary.json",
        {"added": schema_counts["added"], "removed": schema_counts["removed"]},
    )
    if "release_date" not in new_raw_meta.columns:
        raise ValueError(
            "Target index raw metadata lacks release_date, required for gained-genome categorization."
        )
    (
        lost_g,
        gained_g,
        shared_genomes,
        species_lost,
        species_gained,
        reassigned,
    ) = metadata_deltas(old_meta, new_meta)
    excluded = cov.excluded_taxids if cov.available else set()
    lost_species = species_lost["species_taxid"].astype(str)
    species_lost["covered_by_hard_exclude"] = lost_species.map(
        lambda t: _ancestor_in(t, cov.parent_map, excluded)
    )
    hosts = new_params.get("host_taxa_screen", "").split()
    old_date = str(old_params.get("trace_timestamp", ""))[:10]
    lost = categorize_loss(lost_g, new_raw_meta, new_db, cov, hosts)
    gained = categorize_gain(gained_g, new_raw_meta, old_db, cov, hosts, old_date)
    for filename, df in [
        ("species_lost_all_genomes.tsv", species_lost),
        ("species_gained_all_genomes.tsv", species_gained),
        ("genomes_reassigned.tsv", reassigned),
        ("genomes_lost_categorized.tsv", lost),
        ("genomes_gained_categorized.tsv", gained),
    ]:
        df.to_csv(out_dir / filename, sep="\t", index=False)
    old_taxids = set(old_db["taxid"].astype(str))
    new_taxids = set(new_db["taxid"].astype(str))
    reassigned_genomes = int(reassigned["n_genomes"].sum())
    kept_genomes = len(shared_genomes)
    _write_json(
        out_dir / "genomes_summary.json",
        {
            "lost_total": len(lost),
            "gained_total": len(gained),
            "net_genome_delta": len(gained) - len(lost),
            "lost_by_reason": reason_counts(lost, LOSS_REASONS),
            "gained_by_reason": reason_counts(gained, GAIN_REASONS),
            "species_lost_all_genomes": len(species_lost),
            "species_gained_all_genomes": len(species_gained),
            "reassigned_genomes": reassigned_genomes,
            "kept_genomes": kept_genomes,
            "reassigned_pct_of_kept": (
                round(reassigned_genomes / kept_genomes * 100, 2)
                if kept_genomes
                else 0.0
            ),
            "taxa_added": len(new_taxids - old_taxids),
            "taxa_removed": len(old_taxids - new_taxids),
        },
    )


###############################
# 4. INFECTION STATUS CHANGES #
###############################


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


def load_taxonomy_context(
    old_prefix: str,
    new_prefix: str,
    new_params: dict[str, Any],
    repo_root: Path | None,
    work_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, Coverage]:
    """Stage taxonomy DBs and build the coverage context."""
    db = "output/results/total-virus-db-annotated.tsv.gz"
    old_db = pd.read_csv(fetch(old_prefix, db, work_dir / "old"), sep="\t", dtype=str)
    new_db = pd.read_csv(fetch(new_prefix, db, work_dir / "new"), sep="\t", dtype=str)
    if repo_root is None:
        return old_db, new_db, Coverage(False, {}, set(), {})
    coverage = Coverage(
        available=True,
        parent_map=build_parent_map(new_db),
        excluded_taxids=set(new_params.get("viral_taxids_exclude_hard", "").split()),
        included_taxids=load_existing_overrides(repo_root),
    )
    return old_db, new_db, coverage


def infection_status_columns(db: pd.DataFrame) -> list[str]:
    """Return infection-status annotation columns from an annotated taxonomy DB."""
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
    rows = [(old, new, count) for (old, new), count in counts.most_common()]
    return pd.DataFrame(rows, columns=["old", "new", "count"])


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


COVERAGE_COLS = "covered_by", "covered_rule_taxid", "included_for_other_hosts"


def _coverage_match(taxid: str, host: str, cov: Coverage) -> tuple[str, str]:
    """Return (coverage kind, matched rule taxid) for one transition taxid."""
    included = cov.included_taxids.get(host, set())
    rule = _ancestor_in(taxid, cov.parent_map, cov.excluded_taxids | included)
    if not rule:
        return "", ""
    return ("excluded" if rule in cov.excluded_taxids else "included", rule)


def _included_for_other_hosts(taxid: str, host: str, cov: Coverage) -> str:
    """Return comma-separated other hosts whose include rules cover taxid."""
    hosts = (
        h
        for h, taxids in cov.included_taxids.items()
        if h != host and _ancestor_in(taxid, cov.parent_map, taxids)
    )
    return ",".join(sorted(hosts))


def annotate_changes_with_coverage(
    changes: pd.DataFrame, host: str, cov: Coverage
) -> pd.DataFrame:
    """Add coverage rule columns to per-taxon infection-status changes."""
    out = changes.copy()
    if out.empty:
        for col in COVERAGE_COLS:
            out[col] = pd.Series(dtype=str)
        return out
    matched = out["taxid"].map(lambda t: _coverage_match(t, host, cov))
    out[["covered_by", "covered_rule_taxid"]] = pd.DataFrame(
        matched.tolist(), index=out.index
    )
    out["included_for_other_hosts"] = out["taxid"].map(
        lambda t: _included_for_other_hosts(t, host, cov)
    )
    out.loc[out["covered_by"].eq("included"), "included_for_other_hosts"] = ""
    return out


def _species_transition_counts(
    per_host_changes: dict[str, pd.DataFrame],
) -> dict[str, dict[str, int]]:
    """Return per-host species promotion/demotion counts for summary JSON."""
    host_counts = {}
    for host, df in sorted(per_host_changes.items()):
        species = df[df["rank"] == "species"]
        old_status = species["old_status"].astype(str)
        new_status = species["new_status"].astype(str)
        promotions = species[old_status.eq("0") & new_status.eq("1")]
        demotions = species[old_status.eq("1") & new_status.eq("0")]
        uncovered_promotions = promotions
        uncovered_demotions = demotions
        if "covered_by" in species:
            uncovered_promotions = promotions[promotions["covered_by"] == ""]
            uncovered_demotions = demotions[demotions["covered_by"] == ""]
        policy_gaps = 0
        if "included_for_other_hosts" in uncovered_demotions:
            policy_gaps = int(
                uncovered_demotions["included_for_other_hosts"].ne("").sum()
            )
        host_counts[host] = {
            "species_promotions": len(promotions),
            "uncovered_species_promotions": len(uncovered_promotions),
            "species_demotions": len(demotions),
            "uncovered_species_demotions": len(uncovered_demotions),
            "override_scope_gaps": policy_gaps,
        }
    return host_counts


def write_infection_status_tables(
    out_dir: Path, old_db: pd.DataFrame, new_db: pd.DataFrame, cov: Coverage
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
        if cov.available:
            changes = annotate_changes_with_coverage(changes, host, cov)
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
            "coverage_available": cov.available,
            "hosts": _species_transition_counts(per_host_changes),
        },
    )


##################
# 5. PARAMS DIFF #
##################


def summarise_params_changes(old_params: dict, new_params: dict) -> pd.DataFrame:
    """Return changed top-level params as key/kind/old/new rows."""
    rows: list[tuple[str, str, str, str]] = []
    for k in sorted(set(old_params) | set(new_params)):
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
        rows.append((k, kind, old_v, new_v))
    return pd.DataFrame(rows, columns=["key", "kind", "old", "new"]).astype(str)


def _stringify_param(v: object, max_len: int = 120) -> str:
    """Compact one-line stringification of a param value for table display."""
    if v is None:
        return ""
    s = json.dumps(v, sort_keys=True) if isinstance(v, dict | list) else str(v)
    return s if len(s) <= max_len else s[: max_len - 1] + "…"


def diff_params(old_params: dict, new_params: dict) -> str:
    """Return a unified diff between two pretty-printed params dicts."""
    old_lines = json.dumps(old_params, indent=2, sort_keys=True).splitlines(True)
    new_lines = json.dumps(new_params, indent=2, sort_keys=True).splitlines(True)
    diff = difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile="old/index-params.json",
        tofile="new/index-params.json",
    )
    return "".join(diff)


def write_params_tables(
    out_dir: Path, old_prefix: str, new_prefix: str, work_dir: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Stage params JSON, write params outputs, and return parsed params."""
    params = "output/input/index-params.json"
    old_params = json.loads(fetch(old_prefix, params, work_dir / "old").read_text())
    new_params = json.loads(fetch(new_prefix, params, work_dir / "new").read_text())
    (out_dir / "params_diff.txt").write_text(diff_params(old_params, new_params))
    summarise_params_changes(old_params, new_params).to_csv(
        out_dir / "params_changes.tsv", sep="\t", index=False
    )
    return old_params, new_params


########
# MAIN #
########


def parse_arguments() -> argparse.Namespace:
    """Parse benchmark script CLI arguments."""
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
        help="Output directory for benchmark tables and summaries.",
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


def main() -> None:
    """Run the index benchmark report pipeline."""
    args = parse_arguments()
    args.out.mkdir(parents=True, exist_ok=True)
    logger.info(f"Benchmarking {args.old} -> {args.new}")
    write_metrics_table(args.old, args.new, args.out)
    with tempfile.TemporaryDirectory() as td_str:
        work_dir = Path(td_str)
        old_params, new_params = write_params_tables(
            args.out, args.old, args.new, work_dir
        )
        old_db, new_db, coverage = load_taxonomy_context(
            args.old, args.new, new_params, args.repo_root, work_dir
        )
        write_genome_taxonomy_tables(
            args.out,
            args.old,
            args.new,
            old_db,
            new_db,
            coverage,
            old_params,
            new_params,
            work_dir,
        )
        write_infection_status_tables(args.out, old_db, new_db, coverage)
        write_staleness_table(new_params, args.out / "staleness.tsv")
    logger.info(f"Done. Outputs in {args.out.resolve()}")


if __name__ == "__main__":
    main()
