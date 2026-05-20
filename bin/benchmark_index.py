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
from collections import Counter
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
    species_delta: pd.DataFrame,
    added_taxa: pd.DataFrame,
    removed_taxa: pd.DataFrame,
    transitions: dict[str, pd.DataFrame],
) -> None:
    """Write a human-readable summary referencing the TSVs."""
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
    lines.append(
        f"- {len(shrunk)} entries shrunk, {len(grown)} grew, {len(same)} unchanged. See `sizes.tsv`."
    )
    if len(shrunk):
        lines.append(
            f"- **Shrunk DBs** (flag for review): {', '.join(shrunk['name'])}."
        )
    lines += [
        "",
        "## Virus genomes",
        "",
        f"- {len(added_genomes)} added, {len(removed_genomes)} removed (by `genome_id`). See `genomes_added.tsv`, `genomes_removed.tsv`.",
        f"- Per-species deltas in `genomes_by_species.tsv` ({len(species_delta)} species with any change).",
        "",
        "## Virus taxonomy DB",
        "",
        f"- {len(added_taxa)} taxa added, {len(removed_taxa)} removed. See `taxa_added.tsv`, `taxa_removed.tsv`.",
        "",
        "## Infection-status changes (shared taxa)",
        "",
    ]
    for host, df in transitions.items():
        n_changes = int(df["count"].sum()) if not df.empty else 0
        lines.append(
            f"- `{host}`: {n_changes} taxa changed status. See `infection_status_transitions.tsv` and `infection_status_changes_{host}.tsv`."
        )
    lines += [
        "",
        "## Params diff",
        "",
        "See `params_diff.txt`.",
        "",
        "---",
        "",
        "_Generated by `bin/benchmark_index.py`. Reviewers: focus on shrunk DBs, removed taxa, and 1→0 transitions in the per-host infection-status change lists._",
        "",
    ]
    (out_dir / "summary.md").write_text("\n".join(lines))


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
    return parser.parse_args()


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

        transitions: dict[str, pd.DataFrame] = {}
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
            changes.to_csv(
                args.out / f"infection_status_changes_{host}.tsv", sep="\t", index=False
            )
        if all_transitions:
            pd.concat(all_transitions, ignore_index=True).to_csv(
                args.out / "infection_status_transitions.tsv", sep="\t", index=False
            )
        else:
            (args.out / "infection_status_transitions.tsv").write_text(
                "host\told\tnew\tcount\n"
            )

        # Params diff
        logger.info("Diffing index-params.json.")
        old_params_path = fetch(args.old, "output/input/index-params.json", td / "old")
        new_params_path = fetch(args.new, "output/input/index-params.json", td / "new")
        old_params = json.loads(old_params_path.read_text())
        new_params = json.loads(new_params_path.read_text())
        (args.out / "params_diff.txt").write_text(diff_params(old_params, new_params))

    # Summary
    write_summary_md(
        args.out,
        args.old,
        args.new,
        sizes,
        added_g,
        removed_g,
        by_species,
        added_t,
        removed_t,
        transitions,
    )
    logger.info(f"Done. Outputs in {args.out.resolve()}")


if __name__ == "__main__":
    main()
