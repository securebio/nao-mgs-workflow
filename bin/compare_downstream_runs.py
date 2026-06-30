#!/usr/bin/env python3
DESC = """
Compare candidate and reference DOWNSTREAM outputs and flag large differences
for human review. Inputs may be local paths or s3:// roots. Calculations live in
downstream_metrics.py; this script handles staging, discovery, and table I/O.
"""

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


def open_by_suffix(path: Path) -> IO[str]:
    """Open a text file, transparently decompressing .gz."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return open(path)


def stage_results(root: str, local_dir: Path) -> Path:
    """Stage `results_downstream/` locally and return its path."""
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


def _fetch_optional(src: str, dst: Path) -> Path | None:
    """Stage one file, returning None when it is absent."""
    if src.startswith("s3://"):
        dst.parent.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            ["aws", "s3", "cp", src, str(dst), "--no-progress"],
            capture_output=True,
        )
        return dst if result.returncode == 0 else None
    path = Path(src)
    return path if path.exists() else None


def _version_from_pyproject(path: Path) -> str | None:
    """Read the pipeline version from a pyproject.toml, or None if not found."""
    try:
        data = tomllib.loads(path.read_text())
    except (OSError, tomllib.TOMLDecodeError):
        return None
    project = data.get("project")
    if isinstance(project, dict) and project.get("version"):
        return str(project["version"])
    if data.get("version"):
        return str(data["version"])
    return None


def read_pipeline_version(
    root: str, work_dir: Path, override: str | None = None
) -> str | None:
    """Return an override or probe current/legacy logging version files."""
    if override:
        return override
    base = root.rstrip("/")
    for sub in ("logging_downstream/pyproject.toml", "logging/pyproject.toml"):
        staged = _fetch_optional(f"{base}/{sub}", work_dir / Path(sub).name)
        if staged is not None:
            version = _version_from_pyproject(staged)
            if version:
                return version
    for sub in (
        "logging_downstream/pipeline-version.txt",
        "logging/pipeline-version.txt",
    ):
        staged = _fetch_optional(f"{base}/{sub}", work_dir / Path(sub).name)
        if staged is not None:
            text = staged.read_text().strip()
            if text:
                return text
    return None


def _split_filename(filename: str, known_types: set[str]) -> tuple[str, str] | None:
    """Split on the longest recognized `_<file_type>` suffix."""
    for ext in (".tsv.gz", ".tsv", ".json"):
        if filename.endswith(ext):
            stem = filename[: -len(ext)]
            break
    else:
        return None
    for file_type in sorted(known_types, key=len, reverse=True):
        suffix = f"_{file_type}"
        if stem.endswith(suffix):
            group = stem[: -len(suffix)]
            if group:
                return group, file_type
    return None


def _read_table_meta(path: Path) -> tuple[int, list[str]]:
    """Return data-row count and columns for a (possibly gzipped) TSV."""
    with open_by_suffix(path) as fh:
        header = fh.readline().rstrip("\n")
        columns = header.split("\t") if header else []
        n_rows = sum(1 for _ in fh)
    return n_rows, columns


def discover_side(results_dir: Path, known_types: set[str]) -> dm.SideManifest:
    """Discover group files and infer platform from short-read-only outputs."""
    all_files = sorted(p.name for p in results_dir.iterdir() if p.is_file())
    manifest: dm.SideManifest = {}
    for fname in all_files:
        split = _split_filename(fname, known_types)
        if split is None:
            logger.warning(f"Skipping file with no known file type: {fname}")
            continue
        group, file_type = split
        if group not in manifest:
            manifest[group] = dm.GroupManifest(platform="illumina")
        path = results_dir / fname
        if fname.endswith((".tsv.gz", ".tsv")):
            n_rows, columns = _read_table_meta(path)
            entry = dm.FileEntry(n_rows=n_rows, columns=columns)
        else:  # JSON or other non-tabular output: record presence only.
            entry = dm.FileEntry()
        manifest[group].files[file_type] = entry

    if not manifest:
        raise ValueError(f"No recognized per-group output files in {results_dir}")

    # Infer platform from file presence: a group is ONT only if it has NONE of
    # the short-read-only output types (so a short-read group missing just one of
    # them is still short-read, and the missing file is surfaced in Focus 4).
    for gm in manifest.values():
        has_shortread_only = any(t in gm.files for t in SHORTREAD_ONLY_TYPES)
        gm.platform = "illumina" if has_shortread_only else "ont"
    return manifest


def load_schema_columns(schema_dir: Path) -> dict[str, list[str]]:
    """Map schema filename stems to ordered field names."""
    out: dict[str, list[str]] = {}
    for schema_path in sorted(schema_dir.glob("*.schema.json")):
        file_type = schema_path.name[: -len(".schema.json")]
        data = json.loads(schema_path.read_text())
        fields = data.get("fields", [])
        out[file_type] = [f["name"] for f in fields]
    return out


def expected_downstream_types(pyproject_path: Path) -> dict[str, set[str]]:
    """Load expected per-group result types for Illumina and ONT."""
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


def read_tsv(path: Path, **kwargs: Any) -> pd.DataFrame:
    """Read a TSV with quoting disabled, matching pipeline readers."""
    return cast(
        pd.DataFrame, pd.read_csv(path, sep="\t", quoting=csv.QUOTE_NONE, **kwargs)
    )


def _group_file(results_dir: Path, group: str, file_type: str) -> Path | None:
    """Locate a group's file of a given type regardless of compression suffix."""
    matches = sorted(results_dir.glob(f"{group}_{file_type}.tsv*"))
    return matches[0] if matches else None


def _both_sided_manifests(
    reference_manifest: dm.SideManifest,
    candidate_manifest: dm.SideManifest,
    file_type: str,
) -> tuple[dm.SideManifest, dm.SideManifest, list[dict[str, str]]]:
    """Keep groups with `file_type` on both sides and record one-sided groups."""
    reference_groups = {
        g for g, gm in reference_manifest.items() if file_type in gm.files
    }
    candidate_groups = {
        g for g, gm in candidate_manifest.items() if file_type in gm.files
    }
    common = reference_groups & candidate_groups
    skipped: list[dict[str, str]] = []
    for group in sorted(reference_groups ^ candidate_groups):
        side = "reference" if group in reference_groups else "candidate"
        skipped.append(
            {
                "metric": file_type,
                "group": group,
                "reason": f"present on {side} only",
            }
        )
    reference_filtered = {g: gm for g, gm in reference_manifest.items() if g in common}
    candidate_filtered = {g: gm for g, gm in candidate_manifest.items() if g in common}
    return reference_filtered, candidate_filtered, skipped


def load_qc_basic_stats(results_dir: Path, manifest: dm.SideManifest) -> pd.DataFrame:
    """Load raw/cleaned QC tables and add the manifest platform."""
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
    """Parse taxonomy-nodes.dmp into parent and rank maps."""
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


def load_annotated_db(index_root: str, work_dir: Path) -> pd.DataFrame:
    """Load total-virus-db-annotated.tsv.gz from an index root."""
    path = fetch_index_file(index_root, "total-virus-db-annotated.tsv.gz", work_dir)
    return read_tsv(path)


def load_validation_hits(
    results_dir: Path, manifest: dm.SideManifest, columns: list[str]
) -> pd.DataFrame:
    """Load selected validation-hit columns across manifest groups."""
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
    """Load abundance columns from Kraken reports across groups."""
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


def write_tsv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame as TSV (no index)."""
    df.to_csv(path, sep="\t", index=False)
    logger.info(f"Wrote {path} ({len(df)} rows)")


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=DESC, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--reference",
        required=True,
        help="Reference DOWNSTREAM output root: parent of results_downstream/.",
    )
    parser.add_argument(
        "--candidate",
        required=True,
        help="Candidate DOWNSTREAM output root: parent of results_downstream/.",
    )
    parser.add_argument(
        "--candidate-index",
        required=False,
        help="Candidate index root (s3://... or local), for taxonomy + annotation.",
    )
    parser.add_argument(
        "--reference-index",
        required=False,
        help="Reference index root. Used for the vertebrate-status-flip side-table.",
    )
    parser.add_argument(
        "--candidate-version",
        required=False,
        default=None,
        help=(
            "Candidate pipeline version, used verbatim in run_identity.tsv. "
            "Overrides auto-detection; supply this when the DOWNSTREAM root has no "
            "logging pyproject.toml (read it from the RUN output's pyproject.toml)."
        ),
    )
    parser.add_argument(
        "--reference-version",
        required=False,
        default=None,
        help="Reference pipeline version, used verbatim in run_identity.tsv.",
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
    # (e.g. no --candidate-index) cannot leave stale tables behind. REVIEW.md (the human's
    # artifact) is *.md and is left untouched.
    for stale in args.out.glob("*.tsv"):
        stale.unlink()
    stage_dir = args.out / "_staged"
    logger.info(
        f"Comparing DOWNSTREAM output: "
        f"reference={args.reference} candidate={args.candidate}"
    )

    # Recognized per-group file types = schema names + expected-output suffixes.
    # Used to recover (group, file_type) from filenames without depending on a
    # single anchor file.
    schema_columns = load_schema_columns(args.schema_dir)
    expected_types = expected_downstream_types(args.pyproject)
    known_types = (
        set(schema_columns)
        | expected_types.get("illumina", set())
        | expected_types.get("ont", set())
    )

    reference_results = stage_results(args.reference, stage_dir / "reference")
    candidate_results = stage_results(args.candidate, stage_dir / "candidate")
    reference_manifest = discover_side(reference_results, known_types)
    candidate_manifest = discover_side(candidate_results, known_types)
    logger.info(
        f"Discovered {len(reference_manifest)} reference groups, "
        f"{len(candidate_manifest)} candidate groups."
    )
    # Platform is inferred from file presence; a severely truncated short-read
    # group missing ALL short-read-only outputs is indistinguishable from ONT
    # here. Log the ONT-inferred groups (both sides) so a reviewer can check.
    for side, manifest in (
        ("reference", reference_manifest),
        ("candidate", candidate_manifest),
    ):
        ont_groups = sorted(g for g, gm in manifest.items() if gm.platform == "ont")
        if ont_groups:
            logger.info(
                f"Inferred ONT on {side} (no short-read-only outputs): {ont_groups}"
            )

    # Run identity: each run's DOWNSTREAM root, index root, and pipeline version.
    # The version auto-detects from the run's logging dir, falling back to the
    # explicit --*-version override (DOWNSTREAM-only outputs carry no version).
    meta_dir = args.out / "_meta"
    run_identity = pd.DataFrame(
        [
            {
                "side": "reference",
                "downstream_root": args.reference,
                "index_root": args.reference_index or "",
                "pipeline_version": read_pipeline_version(
                    args.reference, meta_dir / "reference", args.reference_version
                )
                or "unknown",
            },
            {
                "side": "candidate",
                "downstream_root": args.candidate,
                "index_root": args.candidate_index or "",
                "pipeline_version": read_pipeline_version(
                    args.candidate, meta_dir / "candidate", args.candidate_version
                )
                or "unknown",
            },
        ]
    )
    write_tsv(run_identity, args.out / "run_identity.tsv")

    # Quantitative tables that feed the consolidated flags (Focus 1-3).
    outputs: dict[str, pd.DataFrame] = {}
    # taxid -> name, populated when a candidate index is given; used to name
    # manifest entities. Empty otherwise (findings then carry taxids only).
    name_map: dict[int, str] = {}
    # Groups dropped from a metric because the required input is present on only
    # one side (would otherwise fabricate a difference). Surfaced via log +
    # skipped_groups.tsv.
    skipped_groups: list[dict[str, str]] = []

    # Focus 4: schema-driven file/column inventory. Pass the platform-expected
    # output types so a file missing from BOTH runs still surfaces as a row.
    inventory = dm.compare_file_inventory(
        reference_manifest, candidate_manifest, expected_types
    )
    write_tsv(inventory, args.out / "file_inventory.tsv")
    columns = dm.compare_columns_to_schema(
        reference_manifest, candidate_manifest, schema_columns
    )
    write_tsv(columns, args.out / "column_conformance.tsv")

    # Focus 3: quality metrics (qc_basic_stats).
    qc_reference = load_qc_basic_stats(reference_results, reference_manifest)
    qc_candidate = load_qc_basic_stats(candidate_results, candidate_manifest)
    if not qc_reference.empty and not qc_candidate.empty:
        qc_numeric = dm.compare_qc_numeric(qc_reference, qc_candidate)
        write_tsv(qc_numeric, args.out / "qc_numeric.tsv")
        outputs["qc_numeric"] = qc_numeric
        survival = dm.qc_read_survival(qc_reference, qc_candidate)
        write_tsv(survival, args.out / "qc_survival.tsv")
        outputs["qc_survival"] = survival
        flag_cols = [
            c
            for c in qc_reference.columns
            if c not in (*dm.QC_NUMERIC_METRICS, *dm.QC_KEYS, "platform")
        ]
        qc_flags = dm.compare_qc_flags(qc_reference, qc_candidate, flag_cols)
        write_tsv(qc_flags, args.out / "qc_flag_changes.tsv")
        outputs["qc_flag_changes"] = qc_flags
    else:
        logger.warning("No qc_basic_stats files found; skipping Focus 3.")

    # Focus 2: kraken abundances. Restrict to groups whose kraken file is present
    # on both sides; a one-sided kraken file would otherwise yield Bray-Curtis 1.0.
    kraken_reference_mf, kraken_candidate_mf, kraken_skipped = _both_sided_manifests(
        reference_manifest, candidate_manifest, "kraken"
    )
    skipped_groups.extend(kraken_skipped)
    if kraken_skipped:
        logger.warning(
            "kraken present on only one side for groups (skipped from Focus 2): "
            f"{[r['group'] for r in kraken_skipped]}"
        )
    kraken_reference = load_kraken(reference_results, kraken_reference_mf)
    kraken_candidate = load_kraken(candidate_results, kraken_candidate_mf)
    if not kraken_reference.empty and not kraken_candidate.empty:
        bray = dm.kraken_bray_curtis(kraken_reference, kraken_candidate)
        write_tsv(bray, args.out / "kraken_bray_curtis.tsv")
        outputs["kraken_bray_curtis"] = bray
        movers = pd.concat(
            [
                dm.kraken_top_movers(kraken_reference, kraken_candidate, rank)
                for rank in dm.KRAKEN_RANKS
            ],
            ignore_index=True,
        )
        write_tsv(movers, args.out / "kraken_top_movers.tsv")
    else:
        logger.warning("No kraken files found; skipping Focus 2.")

    # Focus 1: viral assignments (requires the candidate index for taxonomy + host
    # annotation). If --candidate-index is absent we cannot compute these; surface
    # that.
    if args.candidate_index:
        work_dir = args.out / "_index"
        parent, rank = parse_taxonomy_nodes(
            fetch_index_file(args.candidate_index, "taxonomy-nodes.dmp", work_dir)
        )
        tax = dm.TaxonomyTree(parent, rank)
        annotated = load_annotated_db(args.candidate_index, work_dir)
        vert = dm.vertebrate_taxids(annotated)
        # taxid -> name from the candidate annotation, used to name reassignment
        # and read-status drivers without re-fetching taxonomy-names.dmp.
        name_map = dict(
            zip(annotated["taxid"].astype(int), annotated["name"], strict=True)
        )
        logger.info(
            f"{len(vert)} vertebrate-infecting taxids (status 1, candidate index)."
        )

        # Load the reference index annotation too (if given): used for the
        # vertebrate-status-flip side-table.
        old_annotated = (
            load_annotated_db(args.reference_index, args.out / "_reference_index")
            if args.reference_index
            else None
        )

        vh_cols = [
            "group",
            "sample",
            "seq_id",
            "aligner_taxid_lca",
            "validation_distance_aligner",
            "validation_staxid_lca",
        ]
        # Read-level join: restrict to groups whose validation_hits is present on
        # both sides. A one-sided file would misread every reference read for that
        # group as "lost" (and vice versa). Independent metrics below (clade
        # shares, validation agreement, vertebrate-status flips) keep the full
        # manifests.
        vh_reference_mf, vh_candidate_mf, vh_skipped = _both_sided_manifests(
            reference_manifest, candidate_manifest, "validation_hits"
        )
        skipped_groups.extend(vh_skipped)
        if vh_skipped:
            logger.warning(
                "validation_hits present on only one side for groups (skipped from "
                f"Focus 1 read-level comparison): {[r['group'] for r in vh_skipped]}"
            )
        vh_reference = load_validation_hits(reference_results, vh_reference_mf, vh_cols)
        vh_candidate = load_validation_hits(candidate_results, vh_candidate_mf, vh_cols)
        need = {"group", "seq_id", "aligner_taxid_lca"}
        if need.issubset(vh_reference.columns) and need.issubset(vh_candidate.columns):
            joined = dm.join_read_assignments(vh_reference, vh_candidate)
            read_status = dm.summarize_read_status(joined, vert, name_map)
            write_tsv(read_status, args.out / "viral_read_status.tsv")
            outputs["viral_read_status"] = read_status
            reassign = dm.reassignment_pair_counts(joined, tax, vert)
            outputs["viral_reassignment_pairs"] = reassign
            write_tsv(
                dm.bucket_summary(reassign), args.out / "viral_reassignment_buckets.tsv"
            )
            write_tsv(reassign, args.out / "viral_reassignment_pairs.tsv")
        else:
            logger.warning(
                "validation_hits missing on a side; skipping the read-level "
                "comparison (not computed). Clade shares and vertebrate-status "
                "flips below are independent and still run; BLAST agreement is "
                "evaluated separately and skipped if validation_hits is absent."
            )

        # Clade-count family/order breakdown (short-read only). Rank and name are
        # resolved from the candidate index (taxonomy nodes.dmp + annotation); a
        # taxid deleted from the candidate-index taxonomy simply drops from the
        # clade table, and a name absent from the candidate annotation falls back
        # to its taxid.
        clade_reference = load_clade_counts(reference_results, reference_manifest)
        clade_candidate = load_clade_counts(candidate_results, candidate_manifest)
        if not clade_reference.empty and not clade_candidate.empty:
            clade = dm.clade_rank_shares(
                clade_reference, clade_candidate, rank, name_map
            )
            write_tsv(clade, args.out / "clade_rank_shares.tsv")
            outputs["clade_rank_shares"] = clade
        else:
            logger.warning("No clade_counts found; skipping clade breakdown.")

        agree_need = {"group", "validation_distance_aligner"}
        if agree_need.issubset(vh_reference.columns) and agree_need.issubset(
            vh_candidate.columns
        ):
            validation = dm.validation_agreement(vh_reference).merge(
                dm.validation_agreement(vh_candidate),
                on="group",
                how="outer",
                suffixes=("_reference", "_candidate"),
            )
            write_tsv(validation, args.out / "viral_validation_agreement.tsv")
            outputs["viral_validation_agreement"] = validation

            agreement_by_taxon = dm.validation_agreement_by_taxon(vh_reference).merge(
                dm.validation_agreement_by_taxon(vh_candidate),
                on=["group", "taxid"],
                how="outer",
                suffixes=("_reference", "_candidate"),
            )
            agreement_by_taxon["delta_agreement"] = (
                agreement_by_taxon["agreement_rate_candidate"]
                - agreement_by_taxon["agreement_rate_reference"]
            )
            agreement_by_taxon = dm.mark_agreement_drivers(agreement_by_taxon)
            write_tsv(
                agreement_by_taxon,
                args.out / "viral_validation_agreement_by_taxon.tsv",
            )
            # Which-side-moved decomposition: split each group's agreement losses
            # by which taxid moved -- target-only (rename), aligner-only
            # (reassignment), both (ambiguous), or neither -- plus the one-sided
            # validated residual, so a BLAST-agreement cause is not guessed.
            decomposition = dm.validation_agreement_decomposition(
                vh_reference, vh_candidate, name_map
            )
            write_tsv(decomposition, args.out / "viral_validation_decomposition.tsv")
        else:
            logger.warning(
                "validation_hits missing on a side; skipping BLAST-validation "
                "agreement (not computed)."
            )

        # Vertebrate-status flips between the two index annotations.
        if old_annotated is not None:
            flips = dm.vertebrate_status_flips(old_annotated, annotated)
            write_tsv(flips, args.out / "vertebrate_status_flips.tsv")
        else:
            logger.warning(
                "No --reference-index; skipping vertebrate-status-flip side-table."
            )
    else:
        logger.warning(
            "No --candidate-index given; skipping Focus 1 (viral assignments)."
        )

    thresholds = json.loads(args.thresholds) if args.thresholds else None
    flags = dm.build_flags(outputs, thresholds=thresholds)
    write_tsv(flags, args.out / "flags.tsv")
    # Groups dropped from a metric due to one-sided input absence. Always written
    # (header-only when none) so the report can state "no groups skipped".
    skipped = pd.DataFrame(
        skipped_groups, columns=["metric", "group", "reason"]
    ).sort_values(["metric", "group"])
    write_tsv(skipped, args.out / "skipped_groups.tsv")

    # findings.tsv: the report's required-coverage manifest (threshold flags plus
    # the non-threshold triggers). bounding_numbers.tsv: a max-deviation per
    # checked metric for the "Checked, no action needed" section.
    findings = dm.build_findings(
        outputs,
        inventory=inventory,
        columns=columns,
        skipped=skipped,
        thresholds=thresholds,
        name_map=name_map,
    )
    write_tsv(findings, args.out / "findings.tsv")
    write_tsv(dm.summarize_findings(findings), args.out / "findings_summary.tsv")
    write_tsv(
        dm.bounding_numbers(outputs, thresholds=thresholds),
        args.out / "bounding_numbers.tsv",
    )
    logger.info(
        f"{len(flags)} flags raised; {len(findings)} required findings enumerated."
    )
    logger.info(f"Done. Outputs in {args.out.resolve()}")


if __name__ == "__main__":
    main()
