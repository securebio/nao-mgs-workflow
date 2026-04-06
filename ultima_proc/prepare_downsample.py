#!/usr/bin/env python3
"""Prepare a downsampling samplesheet to match Illumina read counts to Ultima.

For each Ultima simulated paired-end log file, finds the corresponding Illumina
fastq files, computes the downsampling fraction needed to match the Ultima read
count, and writes a samplesheet CSV for the downsample.nf Nextflow pipeline.
"""

import argparse
import csv
import gzip
import io
import logging
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

###########
# GLOBALS #
###########

ULTIMA_S3_PREFIX = "s3://nao-katherine/ultima-analysis/simulated-pe/"
ILLUMINA_BUCKET = "nao-restricted"
STABLE_BUCKET = "nao-mgs-stable"
READ_COUNTS_PATH_TEMPLATE = "{delivery}/3.0.1/20250825/output/results/read_counts.tsv.gz"

SEED_BARCODED = 42
SEED_NA = 137

#############
# UTILITIES #
#############


class UTCFormatter(logging.Formatter):
    converter = time.gmtime


def setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(
        UTCFormatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%SZ")
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler])


def run_cmd(cmd: list[str], check: bool = True) -> str:
    """Run a shell command and return stdout."""
    result = subprocess.run(cmd, capture_output=True, text=True, check=check)
    return result.stdout


def s3_cp_bytes(s3_uri: str) -> bytes:
    """Download an S3 object and return its contents as bytes."""
    result = subprocess.run(
        ["aws", "s3", "cp", s3_uri, "-"],
        capture_output=True,
        check=True,
    )
    return result.stdout


#####################
# PARSE LOG FILES   #
#####################


def list_log_files() -> list[str]:
    """List all *_simulate_pe.log files in the Ultima S3 prefix."""
    output = run_cmd(["aws", "s3", "ls", ULTIMA_S3_PREFIX])
    log_files = []
    for line in output.strip().split("\n"):
        parts = line.split()
        if len(parts) >= 4 and parts[-1].endswith("_simulate_pe.log"):
            log_files.append(parts[-1])
    return sorted(log_files)


def parse_log(content: str) -> int:
    """Parse a simulate_pe.log file and return the total read count."""
    # Format: "<name>: <N> total reads, <M> kept, <D> dropped (empty)"
    match = re.search(r"(\d+) total reads", content)
    if not match:
        raise ValueError(f"Could not parse total reads from log: {content!r}")
    return int(match.group(1))


def extract_prefix(log_filename: str) -> str:
    """Extract the sample prefix from a log filename for matching.

    Examples:
        44_20250211-ATTAACAAGG-TGTTGTTCGT_simulate_pe.log -> 44_20250211
        44_COMO_20250715-CCATCTCGCC-TTCTATGGTT_simulate_pe.log -> 44_COMO_20250715
        CARiverside_20241223-NA-CGGTTATTAG_simulate_pe.log -> CARiverside_20241223
    """
    # Remove _simulate_pe.log suffix
    name = log_filename.replace("_simulate_pe.log", "")
    # The prefix is everything before the barcode section (first hyphen-separated
    # segment that looks like a barcode: all caps letters or "NA")
    # Strategy: split on "-" and take parts until we hit a barcode-like segment
    parts = name.split("-")
    prefix_parts = []
    for part in parts:
        if re.match(r"^[ACGT]{10}$", part) or part == "NA":
            break
        prefix_parts.append(part)
    return "-".join(prefix_parts)


def is_barcoded(log_filename: str) -> bool:
    """Check if a log file is for a barcoded (not NA) sample."""
    return "-NA-" not in log_filename


##############################
# PARSE RESTORE CONTROLS     #
##############################


def parse_restore_controls(
    restore_script: Path,
) -> dict[str, list[dict[str, str]]]:
    """Parse restore_controls.sh to build prefix -> illumina files mapping.

    Returns:
        Dict mapping sample prefix to list of dicts with keys:
        - delivery: e.g. "MJ-2025-03-01"
        - key: full S3 key
        - read: "1" or "2"
        - sample_lane: e.g. "MJ-2025-03-01-44_20250211_S2_L001"
    """
    prefix_to_files: dict[str, list[dict[str, str]]] = defaultdict(list)

    with open(restore_script) as f:
        for line in f:
            match = re.search(r'--key "([^"]+)"', line)
            if not match:
                continue
            key = match.group(1)
            # key format: <delivery>/raw/<delivery>-<sample>_S<N>_L<lane>_<read>.fastq.gz
            delivery = key.split("/")[0]
            filename = key.split("/")[-1]
            # Extract read number (1 or 2)
            read_match = re.search(r"_([12])\.fastq\.gz$", filename)
            if not read_match:
                continue
            read_num = read_match.group(1)
            # Extract sample_lane: everything before _1.fastq.gz or _2.fastq.gz
            sample_lane = filename.replace(f"_{read_num}.fastq.gz", "")

            # Find which prefix this matches
            # The filename contains the prefix after the delivery prefix
            # e.g. MJ-2025-03-01-44_20250211_S2_L001 contains 44_20250211
            file_info = {
                "delivery": delivery,
                "key": key,
                "read": read_num,
                "sample_lane": sample_lane,
                "s3_uri": f"s3://{ILLUMINA_BUCKET}/{key}",
            }

            # We'll do prefix matching later; store by delivery-sample for now
            prefix_to_files[filename].append(file_info)

    # Re-organize: extract the actual prefix from filenames and group
    result: dict[str, list[dict[str, str]]] = defaultdict(list)
    for _filename, file_infos in prefix_to_files.items():
        for info in file_infos:
            result[info["key"]].append(info)

    # Actually, let's just return a flat list and match later
    all_files: list[dict[str, str]] = []
    with open(restore_script) as f:
        for line in f:
            match = re.search(r'--key "([^"]+)"', line)
            if not match:
                continue
            key = match.group(1)
            delivery = key.split("/")[0]
            filename = key.split("/")[-1]
            read_match = re.search(r"_([12])\.fastq\.gz$", filename)
            if not read_match:
                continue
            read_num = read_match.group(1)
            sample_lane = filename.replace(f"_{read_num}.fastq.gz", "")
            all_files.append(
                {
                    "delivery": delivery,
                    "key": key,
                    "read": read_num,
                    "sample_lane": sample_lane,
                    "s3_uri": f"s3://{ILLUMINA_BUCKET}/{key}",
                }
            )

    # Group by prefix
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for info in all_files:
        grouped[info["sample_lane"]].append(info)

    return dict(grouped)


def match_illumina_files(
    prefix: str, all_files: dict[str, list[dict[str, str]]]
) -> tuple[list[str], list[str], list[str]]:
    """Find illumina R1 and R2 files matching a given prefix.

    Returns:
        Tuple of (r1_uris, r2_uris, sample_lane_names)
    """
    r1_uris = []
    r2_uris = []
    sample_lanes = []
    for sample_lane, files in all_files.items():
        if prefix in sample_lane:
            for f in files:
                if f["read"] == "1":
                    r1_uris.append(f["s3_uri"])
                else:
                    r2_uris.append(f["s3_uri"])
            sample_lanes.append(sample_lane)
    return sorted(r1_uris), sorted(r2_uris), sorted(set(sample_lanes))


#########################
# READ COUNTS FROM S3   #
#########################


def get_read_counts(delivery: str) -> dict[str, int]:
    """Download read_counts.tsv.gz for a delivery and return sample->n_read_pairs."""
    path = READ_COUNTS_PATH_TEMPLATE.format(delivery=delivery)
    s3_uri = f"s3://{STABLE_BUCKET}/{path}"
    logging.info("Downloading read counts: %s", s3_uri)
    data = s3_cp_bytes(s3_uri)
    counts = {}
    with gzip.open(io.BytesIO(data), "rt") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            counts[row["sample"]] = int(row["n_read_pairs"])
    return counts


################
# MAIN LOGIC   #
################


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--restore-script",
        type=Path,
        default=Path(__file__).parent / "restore_controls.sh",
        help="Path to restore_controls.sh",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "downsample_samplesheet.csv",
        help="Output samplesheet CSV path",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    start = time.time()
    args = parse_arguments()

    # Step 1: List and parse all log files
    logging.info("Listing log files from %s", ULTIMA_S3_PREFIX)
    log_files = list_log_files()
    logging.info("Found %d log files", len(log_files))

    log_data: dict[str, int] = {}
    for log_file in log_files:
        content = s3_cp_bytes(f"{ULTIMA_S3_PREFIX}{log_file}").decode()
        total_reads = parse_log(content)
        output_id = log_file.replace("_simulate_pe.log", "") + "_illumina_matched"
        log_data[log_file] = total_reads
        logging.info("  %s: %d total reads", output_id, total_reads)

    # Step 2: Parse restore_controls.sh
    logging.info("Parsing %s", args.restore_script)
    all_illumina_files = parse_restore_controls(args.restore_script)
    logging.info(
        "Found %d unique sample-lane combinations", len(all_illumina_files)
    )

    # Step 3: Get read counts for each delivery
    deliveries = set()
    for _sample_lane, files in all_illumina_files.items():
        for f in files:
            deliveries.add(f["delivery"])

    all_read_counts: dict[str, int] = {}
    for delivery in sorted(deliveries):
        counts = get_read_counts(delivery)
        all_read_counts.update(counts)
    logging.info(
        "Loaded read counts for %d sample-lanes across %d deliveries",
        len(all_read_counts),
        len(deliveries),
    )

    # Step 4: Build samplesheet
    rows = []
    for log_file in log_files:
        output_id = log_file.replace("_simulate_pe.log", "") + "_illumina_matched"
        prefix = extract_prefix(log_file)
        target_reads = log_data[log_file]
        seed = SEED_BARCODED if is_barcoded(log_file) else SEED_NA

        r1_uris, r2_uris, sample_lanes = match_illumina_files(
            prefix, all_illumina_files
        )

        if not r1_uris:
            logging.warning("No illumina files found for prefix %s", prefix)
            continue

        # Sum read pairs across all matching lanes
        total_illumina_pairs = 0
        matched_lanes = 0
        for sl in sample_lanes:
            if sl in all_read_counts:
                total_illumina_pairs += all_read_counts[sl]
                matched_lanes += 1
            else:
                logging.warning("No read count found for %s", sl)

        if total_illumina_pairs == 0:
            logging.warning(
                "No illumina read counts for prefix %s, skipping", prefix
            )
            continue

        fraction = target_reads / total_illumina_pairs

        if fraction > 1.0:
            logging.warning(
                "WARNING: %s needs %.2fx more reads than available "
                "(target=%d, available=%d)",
                output_id,
                fraction,
                target_reads,
                total_illumina_pairs,
            )

        rows.append(
            {
                "output_id": output_id,
                "illumina_r1": ";".join(r1_uris),
                "illumina_r2": ";".join(r2_uris),
                "target_reads": target_reads,
                "total_illumina_pairs": total_illumina_pairs,
                "fraction": f"{fraction:.10f}",
                "seed": seed,
            }
        )
        logging.info(
            "  %s: target=%d, available=%d, fraction=%.6f, seed=%d, lanes=%d",
            output_id,
            target_reads,
            total_illumina_pairs,
            fraction,
            seed,
            matched_lanes,
        )

    # Step 5: Write samplesheet
    fieldnames = [
        "output_id",
        "illumina_r1",
        "illumina_r2",
        "target_reads",
        "total_illumina_pairs",
        "fraction",
        "seed",
    ]
    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logging.info("Wrote %d rows to %s", len(rows), args.output)
    elapsed = time.time() - start
    logging.info("Done in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
