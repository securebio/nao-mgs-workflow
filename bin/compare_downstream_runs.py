#!/usr/bin/env python3
DESC = """
Compare the DOWNSTREAM output of two pipeline runs (typically `main` vs `dev`)
before promoting dev to a release, and flag large differences for human review.

Because main and dev usually differ in code AND reference index AND QC params at
once, this is a *holistic* release diff: it surfaces and flags differences but
makes no causal attribution and renders no good/bad verdict (there is no ground
truth). Differences are flagged for a human to adjudicate.

This script does the munging and orchestration: it stages each run's
`results_downstream/` tree, discovers per-group output files, loads the NCBI
taxonomy and annotated viral DB from the (dev) index, and writes comparison
tables. All numeric calculations live in `downstream_metrics.py` so they can be
reviewed and tested apart from this I/O code.

Accepts s3:// URIs or local directories for --main / --dev (each the DOWNSTREAM
output root, i.e. the parent of `results_downstream/`) and for --index.

Usage:
    python bin/compare_downstream_runs.py \\
        --main s3://.../main/downstream/output \\
        --dev  s3://.../dev/downstream/output \\
        --index s3://nao-mgs-index/20260615 \\
        --out ./downstream-bench/
"""

###########
# IMPORTS #
###########

import argparse
import csv
import gzip
import json
import logging
import subprocess
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, cast

import downstream_metrics as dm
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

# File-type keys emitted only by short-read (Illumina/Aviti) DOWNSTREAM runs,
# never by ONT. A group is classified ONT only when it has NONE of these, so a
# short-read group that is merely *missing* one of them (e.g. a dropped
# clade_counts) is still treated as short-read and the gap is surfaced as a
# missing expected output rather than silently relabelled ONT.
SHORTREAD_ONLY_TYPES = ("clade_counts", "duplicate_stats", "fastp")


###########
# STAGING #
###########


def open_by_suffix(path: Path) -> IO[str]:
    """Open a text file, transparently decompressing .gz."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return open(path)


def stage_results(root: str, local_dir: Path) -> Path:
    """Stage a run's `results_downstream/` tree locally and return its path.

    Args:
        root: DOWNSTREAM output root (s3:// URI or local path), the parent of
            `results_downstream/`.
        local_dir: Local directory to sync into.

    Returns:
        Path to the local `results_downstream/` directory.
    """
    src = f"{root.rstrip('/')}/results_downstream"
    local_dir.mkdir(parents=True, exist_ok=True)
    if src.startswith("s3://"):
        logger.info(f"Syncing {src} -> {local_dir}")
        # --delete mirrors the source, so a re-run into the same out dir cannot
        # retain files that were removed from the source between runs.
        subprocess.run(
            ["aws", "s3", "sync", src, str(local_dir), "--no-progress", "--delete"],
            check=True,
        )
        return local_dir
    logger.info(f"Using local results dir {src}")
    return Path(src)


#############
# DISCOVERY #
#############


def _strip_group_prefix(filename: str, groups: list[str]) -> tuple[str, str] | None:
    """Split `filename` into (group, file_type) using the known group list.

    Group names may contain underscores, so we match against the known set
    (longest first) rather than splitting naively. The file_type is the
    remainder with its extension (.tsv.gz/.tsv/.json) removed.

    Returns:
        (group, file_type), or None if no known group prefixes the filename.
    """
    for group in sorted(groups, key=len, reverse=True):
        prefix = f"{group}_"
        if filename.startswith(prefix):
            rest = filename[len(prefix) :]
            for ext in (".tsv.gz", ".tsv", ".json"):
                if rest.endswith(ext):
                    return group, rest[: -len(ext)]
            return group, rest
    return None


def _read_table_meta(path: Path) -> tuple[int, list[str]]:
    """Return (data_row_count, column_names) for a (optionally gzipped) TSV."""
    with open_by_suffix(path) as fh:
        header = fh.readline().rstrip("\n")
        columns = header.split("\t") if header else []
        n_rows = sum(1 for _ in fh)
    return n_rows, columns


def discover_side(results_dir: Path) -> dm.SideManifest:
    """Build a manifest of per-group output files for one run.

    Groups are enumerated from `*_validation_hits.tsv.gz` (one per group), then
    every file is attributed to a group and file type. Platform is inferred per
    group from file presence: a group is ONT only if it has none of the
    short-read-only output types (SHORTREAD_ONLY_TYPES), so a short-read group
    merely missing one of them is not mislabelled ONT. Row counts and columns are
    read for TSVs; JSON files are recorded as present only.

    Args:
        results_dir: Local `results_downstream/` directory.

    Returns:
        Manifest mapping group name -> GroupManifest.
    """
    all_files = sorted(p.name for p in results_dir.iterdir() if p.is_file())
    groups = sorted(
        f[: -len("_validation_hits.tsv.gz")]
        for f in all_files
        if f.endswith("_validation_hits.tsv.gz")
    )
    if not groups:
        raise ValueError(f"No *_validation_hits.tsv.gz files found in {results_dir}")

    manifest: dm.SideManifest = {
        g: dm.GroupManifest(platform="illumina") for g in groups
    }
    for fname in all_files:
        split = _strip_group_prefix(fname, groups)
        if split is None:
            logger.warning(f"Skipping file with no known group prefix: {fname}")
            continue
        group, file_type = split
        path = results_dir / fname
        if fname.endswith((".tsv.gz", ".tsv")):
            n_rows, columns = _read_table_meta(path)
            entry = dm.FileEntry(present=True, n_rows=n_rows, columns=columns)
        else:  # JSON or other non-tabular output: record presence only.
            entry = dm.FileEntry(present=True, n_rows=None, columns=None)
        manifest[group].files[file_type] = entry

    # Infer platform from file presence: a group is ONT only if it has NONE of
    # the short-read-only output types (so a short-read group missing just one of
    # them is still short-read, and the missing file is surfaced in Focus 4).
    for gm in manifest.values():
        has_shortread_only = any(t in gm.files for t in SHORTREAD_ONLY_TYPES)
        gm.platform = "illumina" if has_shortread_only else "ont"
    return manifest


def load_schema_columns(schema_dir: Path) -> dict[str, list[str]]:
    """Map file-type key -> ordered schema field names from schemas/*.schema.json.

    The file-type key is the schema filename without the `.schema.json` suffix,
    which matches the per-group output suffix (e.g. `validation_hits`,
    `qc_basic_stats_raw`). This keeps Focus 4 schema-driven: adding or changing
    an output's schema is picked up automatically.
    """
    out: dict[str, list[str]] = {}
    for schema_path in sorted(schema_dir.glob("*.schema.json")):
        file_type = schema_path.name[: -len(".schema.json")]
        data = json.loads(schema_path.read_text())
        fields = data.get("fields", [])
        out[file_type] = [f["name"] for f in fields]
    return out


def expected_downstream_types(pyproject_path: Path) -> dict[str, set[str]]:
    """Expected per-group file types per platform, from pyproject expected-outputs.

    Returns:
        {'illumina': {...}, 'ont': {...}} of file-type keys (results_downstream
        entries only), derived from `expected-outputs-downstream` and
        `expected-outputs-downstream-ont`.
    """
    data = tomllib.loads(pyproject_path.read_text())
    tool = data.get("tool", {}).get("mgs-workflow", data)

    def types_from(key: str) -> set[str]:
        types: set[str] = set()
        for entry in tool.get(key, []):
            if not entry.startswith("results_downstream/"):
                continue
            name = entry.split("/", 1)[1].replace("{GROUP}_", "", 1)
            for ext in (".tsv.gz", ".tsv", ".json"):
                if name.endswith(ext):
                    name = name[: -len(ext)]
                    break
            types.add(name)
        return types

    return {
        "illumina": types_from("expected-outputs-downstream"),
        "ont": types_from("expected-outputs-downstream-ont"),
    }


###########
# LOADERS #
###########


def read_tsv(path: Path, **kwargs: Any) -> pd.DataFrame:
    """Read a (optionally gzipped) TSV with CSV quoting disabled.

    Quoting is disabled to match the pipeline's own TSV readers, where fields
    can legitimately begin with a quote character (e.g. read sequences).
    """
    return cast(
        pd.DataFrame, pd.read_csv(path, sep="\t", quoting=csv.QUOTE_NONE, **kwargs)
    )


def _group_file(results_dir: Path, group: str, file_type: str) -> Path | None:
    """Locate a group's file of a given type regardless of compression suffix."""
    matches = sorted(results_dir.glob(f"{group}_{file_type}.tsv*"))
    return matches[0] if matches else None


def load_qc_basic_stats(results_dir: Path, manifest: dm.SideManifest) -> pd.DataFrame:
    """Load and concatenate qc_basic_stats (raw + cleaned) across all groups.

    Adds a `platform` column (from the manifest) to each row. NA strings (e.g.
    n_read_pairs for ONT) are read as missing values.

    Returns:
        Concatenated DataFrame, or an empty DataFrame if no QC files are found.
    """
    frames: list[pd.DataFrame] = []
    for group, gm in manifest.items():
        for file_type in ("qc_basic_stats_raw", "qc_basic_stats_cleaned"):
            if file_type not in gm.files:
                continue
            path = _group_file(results_dir, group, file_type)
            if path is None:
                continue
            df = read_tsv(path)
            df["platform"] = gm.platform
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_index_file(index_root: str, subpath: str, work_dir: Path) -> Path:
    """Stage a single file from an index `results/` tree, returning its path."""
    src = f"{index_root.rstrip('/')}/output/results/{subpath}"
    work_dir.mkdir(parents=True, exist_ok=True)
    dst = work_dir / Path(subpath).name
    if src.startswith("s3://"):
        logger.info(f"Downloading {src}")
        subprocess.run(["aws", "s3", "cp", src, str(dst), "--no-progress"], check=True)
        return dst
    return Path(src)


def parse_taxonomy_nodes(path: Path) -> tuple[dict[int, int], dict[int, str]]:
    """Parse taxonomy-nodes.dmp into (parent_map, rank_map).

    nodes.dmp rows are '\\t|\\t'-separated: field 0 = taxid, 1 = parent taxid,
    2 = rank.
    """
    parent: dict[int, int] = {}
    rank: dict[int, str] = {}
    with open_by_suffix(path) as fh:
        for line in fh:
            parts = line.split("\t|\t")
            taxid = int(parts[0])
            parent[taxid] = int(parts[1])
            rank[taxid] = parts[2]
    logger.info(f"Parsed taxonomy: {len(parent)} taxa from {path.name}")
    return parent, rank


def load_merged_taxids(index_root: str, work_dir: Path) -> dict[int, int]:
    """Load the dev index's merged.dmp into an {old_taxid: new_taxid} map.

    merged.dmp rows are '\\t|\\t'-separated: field 0 = old taxid, 1 = new taxid.
    Returns an empty map (with a warning) if the file is absent, so older index
    layouts without merged.dmp degrade gracefully.
    """
    try:
        path: Path | None = fetch_index_file(
            index_root, "taxonomy-merged.dmp", work_dir
        )
    except subprocess.CalledProcessError:
        path = None
    if path is None or not path.exists():
        logger.warning(
            "No taxonomy-merged.dmp in index; skipping taxid canonicalization "
            "(reassignments may include taxid-renumbering artifacts)."
        )
        return {}
    merged: dict[int, int] = {}
    with open_by_suffix(path) as fh:
        for line in fh:
            parts = line.split("\t|\t")
            if len(parts) >= 2:
                merged[int(parts[0])] = int(parts[1].split("\t|")[0])
    logger.info(f"Parsed {len(merged)} merged taxids from {path.name}")
    return merged


def load_annotated_db(index_root: str, work_dir: Path) -> pd.DataFrame:
    """Load total-virus-db-annotated.tsv.gz from an index root."""
    path = fetch_index_file(index_root, "total-virus-db-annotated.tsv.gz", work_dir)
    return read_tsv(path)


def load_validation_hits(
    results_dir: Path, manifest: dm.SideManifest, columns: list[str]
) -> pd.DataFrame:
    """Load and concatenate validation_hits across groups (selected columns).

    Args:
        results_dir: Local results_downstream dir.
        manifest: Side manifest (provides the group list).
        columns: Columns to read; missing optional columns are tolerated.

    Returns:
        Concatenated DataFrame, or empty if no files found.
    """
    wanted = set(columns)
    frames: list[pd.DataFrame] = []
    for group in manifest:
        path = _group_file(results_dir, group, "validation_hits")
        if path is None:
            continue
        df = read_tsv(path, usecols=lambda c: c in wanted)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_clade_counts(results_dir: Path, manifest: dm.SideManifest) -> pd.DataFrame:
    """Load and concatenate clade_counts across groups (short-read only)."""
    frames: list[pd.DataFrame] = []
    for group in manifest:
        path = _group_file(results_dir, group, "clade_counts")
        if path is None:
            continue
        frames.append(read_tsv(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_kraken(results_dir: Path, manifest: dm.SideManifest) -> pd.DataFrame:
    """Load and concatenate kraken reports across all groups.

    Reads only the columns needed for abundance comparison to keep memory low.

    Returns:
        Concatenated DataFrame with columns group, ribosomal, rank, taxid, name,
        n_reads_clade, or an empty DataFrame if no kraken files are found.
    """
    cols = ["group", "ribosomal", "rank", "taxid", "name", "n_reads_clade"]
    frames: list[pd.DataFrame] = []
    for group, gm in manifest.items():
        if "kraken" not in gm.files:
            continue
        path = _group_file(results_dir, group, "kraken")
        if path is None:
            continue
        df = read_tsv(path, usecols=cols)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


###############
# OUTPUT I/O  #
###############


def write_tsv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame as TSV (no index)."""
    df.to_csv(path, sep="\t", index=False)
    logger.info(f"Wrote {path} ({len(df)} rows)")


def write_json(path: Path, obj: object) -> None:
    """Write `obj` as pretty, key-sorted JSON."""
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
    logger.info(f"Wrote {path}")


######################
# CLI / ORCHESTRATION #
######################


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=DESC, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--main",
        required=True,
        help="Reference (main) DOWNSTREAM output root: parent of results_downstream/.",
    )
    parser.add_argument(
        "--dev",
        required=True,
        help="Candidate (dev) DOWNSTREAM output root: parent of results_downstream/.",
    )
    parser.add_argument(
        "--index",
        required=False,
        help="Dev index root (s3://... or local), for taxonomy + viral annotation.",
    )
    parser.add_argument(
        "--old-index",
        required=False,
        help="Main index root, used only for the vertebrate-status-flip table.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output directory for comparison tables and summaries.",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "schemas",
        help="Directory of *.schema.json files (default: repo schemas/).",
    )
    parser.add_argument(
        "--thresholds",
        type=str,
        default=None,
        help=(
            "JSON object overriding flag thresholds, e.g. "
            '\'{"bray_curtis": 0.2, "viral_pct_lost": 3}\'. Keys: '
            + ", ".join(dm.DEFAULT_THRESHOLDS)
            + "."
        ),
    )
    parser.add_argument(
        "--pyproject",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "pyproject.toml",
        help="pyproject.toml providing expected-outputs lists (default: repo root).",
    )
    return parser.parse_args()


def main() -> None:
    """Run the DOWNSTREAM comparison and write tables to the output directory."""
    args = parse_arguments()
    args.out.mkdir(parents=True, exist_ok=True)
    # Clear this script's own output tables from any prior run so a skipped focus
    # (e.g. no --index) cannot leave stale tables behind. REVIEW.md (the human's
    # artifact) is *.md and is left untouched.
    for stale in args.out.glob("*.tsv"):
        stale.unlink()
    stage_dir = args.out / "_staged"
    logger.info(f"Comparing DOWNSTREAM output: main={args.main} dev={args.dev}")

    main_results = stage_results(args.main, stage_dir / "main")
    dev_results = stage_results(args.dev, stage_dir / "dev")
    main_manifest = discover_side(main_results)
    dev_manifest = discover_side(dev_results)
    logger.info(
        f"Discovered {len(main_manifest)} main groups, {len(dev_manifest)} dev groups."
    )

    schema_columns = load_schema_columns(args.schema_dir)

    # Quantitative tables that feed the consolidated flags (Focus 1-3).
    outputs: dict[str, pd.DataFrame] = {}

    # Focus 4: schema-driven file/column inventory. Pass the platform-expected
    # output types so a file missing from BOTH runs still surfaces as a row.
    expected_types = expected_downstream_types(args.pyproject)
    inventory = dm.compare_file_inventory(main_manifest, dev_manifest, expected_types)
    write_tsv(inventory, args.out / "file_inventory.tsv")
    columns = dm.compare_columns_to_schema(main_manifest, dev_manifest, schema_columns)
    write_tsv(columns, args.out / "column_conformance.tsv")

    # Focus 3: quality metrics (qc_basic_stats).
    qc_main = load_qc_basic_stats(main_results, main_manifest)
    qc_dev = load_qc_basic_stats(dev_results, dev_manifest)
    if not qc_main.empty and not qc_dev.empty:
        qc_numeric = dm.compare_qc_numeric(qc_main, qc_dev)
        write_tsv(qc_numeric, args.out / "qc_numeric.tsv")
        outputs["qc_numeric"] = qc_numeric
        survival = dm.qc_read_survival(qc_main, qc_dev)
        write_tsv(survival, args.out / "qc_survival.tsv")
        outputs["qc_survival"] = survival
        flag_cols = [
            c
            for c in qc_main.columns
            if c not in (*dm.QC_NUMERIC_METRICS, *dm.QC_KEYS, "platform")
        ]
        qc_flags = dm.compare_qc_flags(qc_main, qc_dev, flag_cols)
        write_tsv(qc_flags, args.out / "qc_flag_changes.tsv")
    else:
        logger.warning("No qc_basic_stats files found; skipping Focus 3.")

    # Focus 2: kraken abundances.
    kraken_main = load_kraken(main_results, main_manifest)
    kraken_dev = load_kraken(dev_results, dev_manifest)
    if not kraken_main.empty and not kraken_dev.empty:
        bray = dm.kraken_bray_curtis(kraken_main, kraken_dev)
        write_tsv(bray, args.out / "kraken_bray_curtis.tsv")
        outputs["kraken_bray_curtis"] = bray
        movers = pd.concat(
            [
                dm.kraken_top_movers(kraken_main, kraken_dev, rank)
                for rank in dm.KRAKEN_RANKS
            ],
            ignore_index=True,
        )
        write_tsv(movers, args.out / "kraken_top_movers.tsv")
    else:
        logger.warning("No kraken files found; skipping Focus 2.")

    # Focus 1: viral assignments (requires the dev index for taxonomy + host
    # annotation). If --index is absent we cannot compute these; surface that.
    if args.index:
        work_dir = args.out / "_index"
        parent, rank = parse_taxonomy_nodes(
            fetch_index_file(args.index, "taxonomy-nodes.dmp", work_dir)
        )
        tax = dm.TaxonomyTree(parent, rank)
        merge_map = load_merged_taxids(args.index, work_dir)
        annotated = load_annotated_db(args.index, work_dir)
        vert = dm.vertebrate_taxids(annotated)
        logger.info(f"{len(vert)} vertebrate-infecting taxids (status 1, dev index).")

        # Load the main index annotation too (if given): used to resolve clade
        # rank/name from BOTH index versions (so a main-only family isn't dropped)
        # and for the vertebrate-status-flip side-table.
        old_annotated = (
            load_annotated_db(args.old_index, args.out / "_old_index")
            if args.old_index
            else None
        )
        clade_annotated = (
            pd.concat([old_annotated, annotated], ignore_index=True)
            if old_annotated is not None
            else annotated
        )

        vh_cols = [
            "group",
            "sample",
            "seq_id",
            "aligner_taxid_lca",
            "prim_align_dup_exemplar",
        ]
        vh_main = load_validation_hits(main_results, main_manifest, vh_cols)
        vh_dev = load_validation_hits(dev_results, dev_manifest, vh_cols)
        joined = dm.join_read_assignments(vh_main, vh_dev, merge_map)
        read_status = dm.summarize_read_status(joined, vert)
        write_tsv(read_status, args.out / "viral_read_status.tsv")
        outputs["viral_read_status"] = read_status
        reassign = dm.reassignment_distances(joined, tax, vert)
        write_tsv(reassign, args.out / "viral_reassignment_detail.tsv")
        write_tsv(
            dm.bucket_summary(reassign), args.out / "viral_reassignment_buckets.tsv"
        )
        write_tsv(
            dm.reassignment_concentration(reassign),
            args.out / "viral_reassignment_concentration.tsv",
        )

        # Duplicate-aware view: re-run the read-status comparison on alignment
        # exemplars only (short-read), so lost/gained/reassigned are not weighted
        # by PCR-duplicate counts (whose fraction can itself differ across runs).
        if (
            "prim_align_dup_exemplar" in vh_main.columns
            and "prim_align_dup_exemplar" in vh_dev.columns
        ):
            ex_main = vh_main[vh_main["seq_id"] == vh_main["prim_align_dup_exemplar"]]
            ex_dev = vh_dev[vh_dev["seq_id"] == vh_dev["prim_align_dup_exemplar"]]
            joined_dedup = dm.join_read_assignments(ex_main, ex_dev, merge_map)
            write_tsv(
                dm.summarize_read_status(joined_dedup, vert),
                args.out / "viral_read_status_dedup.tsv",
            )
        else:
            logger.warning(
                "No prim_align_dup_exemplar column; skipping dedup read-status view."
            )

        # Clade-count family/order breakdown (short-read only). Rank/name resolved
        # from both index versions so main-only families are not dropped.
        clade_main = load_clade_counts(main_results, main_manifest)
        clade_dev = load_clade_counts(dev_results, dev_manifest)
        if not clade_main.empty and not clade_dev.empty:
            clade = dm.clade_rank_shares(clade_main, clade_dev, clade_annotated)
            write_tsv(clade, args.out / "clade_rank_shares.tsv")
            outputs["clade_rank_shares"] = clade
        else:
            logger.warning("No clade_counts found; skipping clade breakdown.")

        # BLAST-validation agreement (secondary).
        val_cols = ["group", "validation_distance_aligner"]
        val_main = load_validation_hits(main_results, main_manifest, val_cols)
        val_dev = load_validation_hits(dev_results, dev_manifest, val_cols)
        validation = dm.validation_agreement(val_main).merge(
            dm.validation_agreement(val_dev),
            on="group",
            how="outer",
            suffixes=("_main", "_dev"),
        )
        write_tsv(validation, args.out / "viral_validation_agreement.tsv")
        outputs["viral_validation_agreement"] = validation

        # Vertebrate-status flips between the two index annotations.
        if old_annotated is not None:
            flips = dm.vertebrate_status_flips(old_annotated, annotated)
            write_tsv(flips, args.out / "vertebrate_status_flips.tsv")
        else:
            logger.warning(
                "No --old-index; skipping vertebrate-status-flip side-table."
            )
    else:
        logger.warning("No --index given; skipping Focus 1 (viral assignments).")

    # Consolidated flags across all focuses (fixed thresholds + cohort outliers).
    thresholds = json.loads(args.thresholds) if args.thresholds else None
    flags = dm.build_flags(outputs, thresholds=thresholds)
    write_tsv(flags, args.out / "flags.tsv")
    logger.info(f"{len(flags)} flags raised across all focuses.")

    logger.info(f"Done. Outputs in {args.out.resolve()}")


if __name__ == "__main__":
    main()
