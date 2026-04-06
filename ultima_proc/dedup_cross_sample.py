#!/usr/bin/env python3
"""Remove reads duplicated across paired barcoded/NA downsampled samples.

When two samples (barcoded and NA) are independently downsampled from the same
source Illumina data using different seeds, a small number of reads end up in
both outputs by chance (expected overlap ≈ fraction_A × fraction_B × total).
This script identifies those shared reads and removes them from the barcoded
(smaller) sample to produce zero cross-sample duplication.

Workflow:
  1. Parse the downsample samplesheet to identify barcoded/NA pairs.
  2. For each pair, stream both R1 files from S3, collect read IDs, find overlap.
  3. Rewrite the barcoded sample's R1 and R2 excluding overlapping reads.
  4. Upload the deduplicated files back to S3.
"""

import argparse
import csv
import gzip
import io
import logging
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

###########
# GLOBALS #
###########

S3_PREFIX = "s3://nao-katherine/ultima-analysis/downsampled-illumina/"

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    converter = time.gmtime


def setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(
        UTCFormatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%SZ")
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler])


####################
# PAIR DISCOVERY   #
####################


def extract_source_prefix(output_id: str) -> str:
    """Extract the source prefix from an output_id for pairing.

    Examples:
        44_20250211-ATTAACAAGG-TGTTGTTCGT_illumina_matched -> 44_20250211
        44_20250211-NA-TGTTGTTCGT_illumina_matched -> 44_20250211
        44_COMO_20250715-CCATCTCGCC-TTCTATGGTT_illumina_matched -> 44_COMO_20250715
        CARiverside_20241223-NA-CGGTTATTAG_illumina_matched -> CARiverside_20241223
    """
    # Remove _illumina_matched suffix
    name = output_id.replace("_illumina_matched", "")
    # Split on "-" and take parts until we hit a barcode-like segment
    parts = name.split("-")
    prefix_parts = []
    for part in parts:
        if re.match(r"^[ACGT]{10}$", part) or part == "NA":
            break
        prefix_parts.append(part)
    return "-".join(prefix_parts)


def is_barcoded(output_id: str) -> bool:
    """Check if an output_id is for a barcoded (not NA) sample."""
    return "-NA-" not in output_id


def find_pairs(
    samplesheet: Path,
) -> list[dict[str, str]]:
    """Parse samplesheet and return list of paired samples.

    Returns:
        List of dicts with keys: source_prefix, barcoded_id, na_id
    """
    rows_by_prefix: dict[str, dict[str, str]] = {}

    with open(samplesheet) as f:
        reader = csv.DictReader(f)
        for row in reader:
            output_id = row["output_id"]
            prefix = extract_source_prefix(output_id)

            if prefix not in rows_by_prefix:
                rows_by_prefix[prefix] = {}

            if is_barcoded(output_id):
                rows_by_prefix[prefix]["barcoded_id"] = output_id
            else:
                rows_by_prefix[prefix]["na_id"] = output_id

    pairs = []
    for prefix, ids in sorted(rows_by_prefix.items()):
        if "barcoded_id" in ids and "na_id" in ids:
            pairs.append(
                {
                    "source_prefix": prefix,
                    "barcoded_id": ids["barcoded_id"],
                    "na_id": ids["na_id"],
                }
            )
        else:
            logging.warning("Unpaired prefix %s: %s", prefix, ids)

    return pairs


######################
# READ ID EXTRACTION #
######################


def stream_read_ids(s3_uri: str) -> set[str]:
    """Stream a gzipped FASTQ from S3 and return the set of read IDs.

    Read IDs are extracted from header lines (lines starting with @) as the
    portion before the first space.
    """
    proc = subprocess.Popen(
        ["aws", "s3", "cp", s3_uri, "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None
    read_ids: set[str] = set()
    line_num = 0
    with gzip.open(proc.stdout, "rt") as f:
        for line in f:
            if line_num % 4 == 0:
                # Header line: @ID rest
                read_id = line.split(" ", 1)[0]
                read_ids.add(read_id)
            line_num += 1

    proc.wait()
    if proc.returncode != 0:
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        raise RuntimeError(f"Failed to stream {s3_uri}: {stderr}")

    return read_ids


##########################
# FASTQ FILTERING        #
##########################


def filter_fastq(s3_input: str, local_output: Path, exclude_ids: set[str]) -> int:
    """Stream a gzipped FASTQ from S3, exclude reads in exclude_ids, write locally.

    Args:
        s3_input: S3 URI of the input FASTQ.
        local_output: Local path for the filtered gzipped FASTQ.
        exclude_ids: Set of read IDs (with @) to exclude.

    Returns:
        Number of reads removed.
    """
    proc = subprocess.Popen(
        ["aws", "s3", "cp", s3_input, "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None

    removed = 0
    with gzip.open(proc.stdout, "rb") as fin, gzip.open(
        local_output, "wb", compresslevel=1
    ) as fout:
        while True:
            header = fin.readline()
            if not header:
                break
            seq = fin.readline()
            plus = fin.readline()
            qual = fin.readline()

            read_id = header.split(b" ", 1)[0].decode()
            if read_id in exclude_ids:
                removed += 1
            else:
                fout.write(header)
                fout.write(seq)
                fout.write(plus)
                fout.write(qual)

    proc.wait()
    if proc.returncode != 0:
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        raise RuntimeError(f"Failed to stream {s3_input}: {stderr}")

    return removed


def upload_to_s3(local_path: Path, s3_uri: str) -> None:
    """Upload a local file to S3."""
    subprocess.run(
        ["aws", "s3", "cp", str(local_path), s3_uri],
        check=True,
        capture_output=True,
    )


################
# MAIN LOGIC   #
################


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--samplesheet",
        type=Path,
        default=Path(__file__).parent / "downsample_samplesheet.csv",
        help="Downsample samplesheet CSV",
    )
    parser.add_argument(
        "--s3-prefix",
        default=S3_PREFIX,
        help="S3 prefix where downsampled FASTQs live (default: %(default)s)",
    )
    parser.add_argument(
        "--output-suffix",
        default="_dedup",
        help="Suffix to add before _R1/_R2 in output filenames (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report overlaps without writing files",
    )
    parser.add_argument(
        "--pairs",
        nargs="*",
        help="Only process these source prefixes (e.g. 44_20250211). Default: all.",
    )
    return parser.parse_args()


def process_pair(
    pair: dict[str, str],
    s3_prefix: str,
    output_suffix: str,
    dry_run: bool,
) -> dict[str, int]:
    """Process one barcoded/NA pair. Returns stats dict."""
    barcoded_id = pair["barcoded_id"]
    na_id = pair["na_id"]
    source = pair["source_prefix"]

    barcoded_r1_uri = f"{s3_prefix}{barcoded_id}_R1.fastq.gz"
    na_r1_uri = f"{s3_prefix}{na_id}_R1.fastq.gz"

    logging.info("[%s] Collecting read IDs from barcoded sample: %s", source, barcoded_id)
    barcoded_ids = stream_read_ids(barcoded_r1_uri)
    logging.info("[%s]   %d reads in barcoded sample", source, len(barcoded_ids))

    logging.info("[%s] Streaming NA sample to find overlaps: %s", source, na_id)
    overlap: set[str] = set()
    na_count = 0
    proc = subprocess.Popen(
        ["aws", "s3", "cp", na_r1_uri, "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None
    with gzip.open(proc.stdout, "rt") as f:
        for i, line in enumerate(f):
            if i % 4 == 0:
                read_id = line.split(" ", 1)[0]
                na_count += 1
                if read_id in barcoded_ids:
                    overlap.add(read_id)
    proc.wait()
    if proc.returncode != 0:
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        raise RuntimeError(f"Failed to stream {na_r1_uri}: {stderr}")
    logging.info("[%s]   %d reads in NA sample", source, na_count)

    logging.info(
        "[%s]   %d overlapping reads (%.4f%% of barcoded, %.4f%% of NA)",
        source,
        len(overlap),
        100 * len(overlap) / len(barcoded_ids) if barcoded_ids else 0,
        100 * len(overlap) / na_count if na_count else 0,
    )

    stats = {
        "source_prefix": source,
        "barcoded_id": barcoded_id,
        "na_id": na_id,
        "barcoded_reads": len(barcoded_ids),
        "na_reads": na_count,
        "overlap": len(overlap),
    }

    if dry_run or not overlap:
        return stats

    # Filter the barcoded sample (smaller), removing overlapping reads
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        for read_end in ["R1", "R2"]:
            input_uri = f"{s3_prefix}{barcoded_id}_{read_end}.fastq.gz"
            # Write deduped file with suffix, e.g. _dedup_R1.fastq.gz
            output_name = f"{barcoded_id}{output_suffix}_{read_end}.fastq.gz"
            local_output = tmpdir_path / output_name
            output_uri = f"{s3_prefix}{output_name}"

            logging.info("[%s] Filtering %s -> %s", source, read_end, output_name)
            removed = filter_fastq(input_uri, local_output, overlap)
            logging.info("[%s]   Removed %d reads from %s", source, removed, read_end)

            logging.info("[%s] Uploading %s", source, output_uri)
            upload_to_s3(local_output, output_uri)

    stats["removed_r1"] = len(overlap)
    stats["removed_r2"] = len(overlap)
    return stats


def main() -> None:
    setup_logging()
    start = time.time()
    args = parse_arguments()

    pairs = find_pairs(args.samplesheet)
    logging.info("Found %d pairs", len(pairs))

    if args.pairs:
        pairs = [p for p in pairs if p["source_prefix"] in args.pairs]
        logging.info("Filtered to %d pairs", len(pairs))

    all_stats = []
    for pair in pairs:
        stats = process_pair(pair, args.s3_prefix, args.output_suffix, args.dry_run)
        all_stats.append(stats)

    # Summary
    logging.info("=" * 60)
    logging.info("SUMMARY")
    logging.info("=" * 60)
    total_overlap = 0
    for s in all_stats:
        total_overlap += s["overlap"]
        logging.info(
            "  %s: %d overlap / %d barcoded / %d NA",
            s["source_prefix"],
            s["overlap"],
            s["barcoded_reads"],
            s["na_reads"],
        )
    logging.info("Total overlapping reads across all pairs: %d", total_overlap)

    elapsed = time.time() - start
    logging.info("Done in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
