#!/usr/bin/env python3
DESC = """
Clean pipeline output files after a simulated paired-end Ultima run.

Removes PE artifact columns from virus_hits (validation_hits), fixes doubled
read counts in read_counts and qc_basic_stats files.

This script operates on local files. If results are on S3, sync them down first:
    aws s3 sync s3://bucket/results/ ./results/
Then run this script, and optionally sync cleaned results back:
    aws s3 sync ./results_clean/ s3://bucket/results_clean/
"""

###########
# IMPORTS #
###########

import argparse
import csv
import gzip
import io
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

###########
# HELPERS #
###########

# PE artifact columns to drop from validation_hits / virus_hits files
VIRUS_HITS_COLS_TO_DROP = {
    "query_len_rev",
    "query_seq_rev",
    "query_qual_rev",
    "prim_align_fragment_length",
    "prim_align_best_alignment_score_rev",
    "prim_align_edit_distance_rev",
    "prim_align_ref_start_rev",
    "prim_align_query_rc_rev",
    "prim_align_pair_status",
}

# Columns to drop from read_counts and qc_basic_stats (replaced by n_reads)
READ_COUNT_COLS_TO_REPLACE = {"n_reads_single", "n_read_pairs"}


def open_by_suffix(path: Path, mode: str = "rt"):
    """Open a file, using gzip if it ends in .gz.

    Args:
        path: File path to open.
        mode: File open mode (default "rt" for reading text).

    Returns:
        File handle (gzip-wrapped if .gz suffix).
    """
    if path.suffix == ".gz":
        return gzip.open(path, mode)
    return open(path, mode)


##########################
# VIRUS HITS CLEANING    #
##########################


def clean_virus_hits(input_path: Path, output_path: Path) -> None:
    """Drop PE artifact columns from a virus_hits / validation_hits TSV.

    Args:
        input_path: Path to input TSV (possibly gzipped).
        output_path: Path to write cleaned TSV (gzipped if input was).
    """
    with open_by_suffix(input_path, "rt") as fin:
        reader = csv.DictReader(fin, delimiter="\t")
        assert reader.fieldnames is not None
        keep_cols = [c for c in reader.fieldnames if c not in VIRUS_HITS_COLS_TO_DROP]
        dropped = [c for c in reader.fieldnames if c in VIRUS_HITS_COLS_TO_DROP]
        logger.info(
            "  Dropping %d columns: %s", len(dropped), ", ".join(dropped)
        )

        with open_by_suffix(output_path, "wt") as fout:
            writer = csv.DictWriter(
                fout, fieldnames=keep_cols, delimiter="\t", extrasaction="ignore"
            )
            writer.writeheader()
            for row in reader:
                writer.writerow(row)


##########################
# READ COUNTS CLEANING   #
##########################


def clean_read_counts(input_path: Path, output_path: Path) -> None:
    """Fix doubled read counts in read_counts TSV.

    With simulated PE, n_reads_single = 2x actual and n_read_pairs = actual.
    Replaces both with a single n_reads column equal to n_read_pairs.

    Args:
        input_path: Path to input TSV.
        output_path: Path to write cleaned TSV.
    """
    with open_by_suffix(input_path, "rt") as fin:
        reader = csv.DictReader(fin, delimiter="\t")
        assert reader.fieldnames is not None
        # Build new column list: replace n_reads_single/n_read_pairs with n_reads
        keep_cols = []
        n_reads_inserted = False
        for c in reader.fieldnames:
            if c in READ_COUNT_COLS_TO_REPLACE:
                if not n_reads_inserted:
                    keep_cols.append("n_reads")
                    n_reads_inserted = True
            else:
                keep_cols.append(c)

        logger.info(
            "  Replacing n_reads_single + n_read_pairs with n_reads (= n_read_pairs)"
        )

        with open_by_suffix(output_path, "wt") as fout:
            writer = csv.DictWriter(
                fout, fieldnames=keep_cols, delimiter="\t", extrasaction="ignore"
            )
            writer.writeheader()
            for row in reader:
                new_row = {k: row[k] for k in reader.fieldnames if k not in READ_COUNT_COLS_TO_REPLACE}
                new_row["n_reads"] = row["n_read_pairs"]
                writer.writerow(new_row)


##############################
# QC BASIC STATS CLEANING    #
##############################


def clean_qc_basic_stats(input_path: Path, output_path: Path) -> None:
    """Fix doubled read counts in qc_basic_stats TSV.

    Same fix as read_counts: replace n_reads_single and n_read_pairs with
    n_reads = n_read_pairs.

    Args:
        input_path: Path to input TSV (possibly gzipped).
        output_path: Path to write cleaned TSV (gzipped if input was).
    """
    with open_by_suffix(input_path, "rt") as fin:
        reader = csv.DictReader(fin, delimiter="\t")
        assert reader.fieldnames is not None
        keep_cols = []
        n_reads_inserted = False
        for c in reader.fieldnames:
            if c in READ_COUNT_COLS_TO_REPLACE:
                if not n_reads_inserted:
                    keep_cols.append("n_reads")
                    n_reads_inserted = True
            else:
                keep_cols.append(c)

        logger.info(
            "  Replacing n_reads_single + n_read_pairs with n_reads (= n_read_pairs)"
        )

        with open_by_suffix(output_path, "wt") as fout:
            writer = csv.DictWriter(
                fout, fieldnames=keep_cols, delimiter="\t", extrasaction="ignore"
            )
            writer.writeheader()
            for row in reader:
                new_row = {k: row[k] for k in reader.fieldnames if k not in READ_COUNT_COLS_TO_REPLACE}
                new_row["n_reads"] = row["n_read_pairs"]
                writer.writerow(new_row)


########################
# FILE DISCOVERY       #
########################


def find_and_clean(results_dir: Path, output_dir: Path, dry_run: bool) -> None:
    """Find output files in results directory and clean them.

    Args:
        results_dir: Pipeline results directory (local path).
        output_dir: Directory for cleaned output files.
        dry_run: If True, list files without writing.
    """
    if not results_dir.is_dir():
        raise FileNotFoundError(f"Results directory not found: {results_dir}")

    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    # Find virus_hits / validation_hits files
    virus_hits_files = sorted(
        list(results_dir.rglob("*virus_hits*.tsv.gz"))
        + list(results_dir.rglob("*validation_hits*.tsv.gz"))
    )
    # Find read_counts files
    read_counts_files = sorted(results_dir.rglob("*read_counts*.tsv*"))
    # Find qc_basic_stats files
    qc_basic_stats_files = sorted(results_dir.rglob("*qc_basic_stats*.tsv*"))

    all_files = {
        "virus_hits": (virus_hits_files, clean_virus_hits),
        "read_counts": (read_counts_files, clean_read_counts),
        "qc_basic_stats": (qc_basic_stats_files, clean_qc_basic_stats),
    }

    total = sum(len(files) for files, _ in all_files.values())
    if total == 0:
        logger.warning("No matching files found in %s", results_dir)
        return

    for category, (files, clean_fn) in all_files.items():
        if not files:
            continue
        logger.info("Found %d %s file(s):", len(files), category)
        for f in files:
            rel = f.relative_to(results_dir)
            out_path = output_dir / rel
            logger.info("  %s", rel)
            if dry_run:
                continue
            out_path.parent.mkdir(parents=True, exist_ok=True)
            clean_fn(f, out_path)

    if dry_run:
        logger.info("Dry run complete. %d files would be cleaned.", total)
    else:
        logger.info("Cleaned %d files. Output in %s", total, output_dir)


##########
# MAIN   #
##########


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        required=True,
        help="Path to pipeline results directory (local). Sync from S3 first if needed.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Path for cleaned output files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be cleaned without writing output.",
    )
    return parser.parse_args()


def main() -> None:
    """Clean simulated PE artifacts from Ultima pipeline results."""
    start_time = time.time()
    args = parse_arguments()
    logger.info("Starting Ultima results cleanup")
    logger.info("  Results dir: %s", args.results_dir)
    logger.info("  Output dir:  %s", args.output_dir)
    if args.dry_run:
        logger.info("  DRY RUN — no files will be written")
    find_and_clean(args.results_dir, args.output_dir, args.dry_run)
    elapsed = time.time() - start_time
    logger.info("Done in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
