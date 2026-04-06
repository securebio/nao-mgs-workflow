#!/usr/bin/env python3
DESC = """
Summarize the read funnel through EXTRACT_VIRAL_READS_SHORT.

Reports per-sample read counts at each pipeline stage:
  1. Input reads (from read_counts.tsv)
  2. BBDuk viral kmer matches (from fastp.json before_filtering)
  3. FASTP QC pass (from fastp.json after_filtering)
  4. Bowtie2 viral alignment (from .command.err in work dir via trace)
  5. Bowtie2 human depletion (from .command.err in work dir via trace)
  6. Bowtie2 other depletion (from .command.err in work dir via trace)
  7. Final virus hits (from virus_hits.tsv.gz row count)

Bowtie2 counts require the Nextflow trace file and access to the work directory.
If the trace file is not provided, only stages 1-3 and 7 are shown.

Usage:
    # With trace file (full funnel including Bowtie2):
    python3 read_funnel.py --results-dir ./results/ --trace-file ./logging/trace_*.tsv

    # Without trace file (published files only):
    python3 read_funnel.py --results-dir ./results/

    # For S3 results, sync locally first:
    aws s3 sync s3://bucket/output/results/ ./results/
    aws s3 sync s3://bucket/output/logging/ ./logging/
    # Also need work dir access for Bowtie2 counts:
    # Either run from the machine where the pipeline ran, or skip --trace-file
"""

###########
# IMPORTS #
###########

import argparse
import csv
import gzip
import json
import logging
import re
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


def open_by_suffix(path: Path, mode: str = "rt"):
    """Open a file, using gzip if it ends in .gz.

    Args:
        path: File path to open.
        mode: File open mode.

    Returns:
        File handle.
    """
    if path.suffix == ".gz":
        return gzip.open(path, mode)
    return open(path, mode)


def extract_sample_name(filename: str, suffixes: list[str]) -> str | None:
    """Extract sample name by stripping known suffixes from a filename.

    Args:
        filename: The filename (without directory).
        suffixes: List of suffixes to try stripping (longest match wins).

    Returns:
        Sample name, or None if no suffix matched.
    """
    for suffix in sorted(suffixes, key=len, reverse=True):
        if filename.endswith(suffix):
            return filename[: -len(suffix)]
    return None


##########################
# PUBLISHED FILE PARSING #
##########################


def get_input_read_counts(results_dir: Path) -> dict[str, int]:
    """Extract input read pair counts from read_counts.tsv files.

    Args:
        results_dir: Pipeline results directory.

    Returns:
        Dict mapping sample name to read pair count.
    """
    counts: dict[str, int] = {}
    for f in sorted(results_dir.rglob("*read_counts*tsv*")):
        with open_by_suffix(f) as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                counts[row["sample"]] = int(row["n_read_pairs"])
    return counts


def get_fastp_counts(results_dir: Path) -> dict[str, dict[str, int]]:
    """Extract before/after filtering read counts from fastp.json files.

    FASTP receives BBDuk kmer-matched reads as input, so
    before_filtering = reads surviving BBDuk kmer screen.

    Args:
        results_dir: Pipeline results directory.

    Returns:
        Dict mapping sample name to {before_pairs, after_pairs}.
    """
    counts: dict[str, dict[str, int]] = {}
    for f in sorted(results_dir.rglob("*fastp.json")):
        sample = extract_sample_name(f.name, ["_fastp.json"])
        if sample is None:
            continue
        with open(f) as fh:
            data = json.load(fh)
        summary = data.get("summary", {})
        before = summary.get("before_filtering", {}).get("total_reads", 0)
        after = summary.get("after_filtering", {}).get("total_reads", 0)
        counts[sample] = {
            "before_pairs": before // 2,
            "after_pairs": after // 2,
        }
    return counts


def get_virus_hits_counts(results_dir: Path) -> dict[str, int]:
    """Count unique reads in virus_hits / validation_hits files.

    Args:
        results_dir: Pipeline results directory.

    Returns:
        Dict mapping sample name to virus hit read count.
    """
    counts: dict[str, int] = {}
    for f in sorted(
        list(results_dir.rglob("*virus_hits*tsv*"))
        + list(results_dir.rglob("*validation_hits*tsv*"))
    ):
        sample = extract_sample_name(
            f.name,
            [
                "_virus_hits.tsv.gz", "_virus_hits.tsv",
                "_validation_hits.tsv.gz", "_validation_hits.tsv",
            ],
        )
        if sample is None:
            continue
        with open_by_suffix(f) as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            seq_ids = {row["seq_id"] for row in reader}
        counts[sample] = len(seq_ids)
    return counts


##########################
# BOWTIE2 LOG PARSING    #
##########################


def parse_bowtie2_stderr(stderr_text: str) -> dict[str, int]:
    """Parse Bowtie2 alignment summary from stderr output.

    Args:
        stderr_text: Contents of .command.err from a Bowtie2 process.

    Returns:
        Dict with keys: total_pairs, aligned_concordant_1, aligned_concordant_gt1,
        aligned_discordant, overall_rate_pct. Returns empty dict if not parseable.
    """
    result: dict[str, int] = {}

    # Total reads/pairs
    m = re.search(r"(\d+) reads; of these:", stderr_text)
    if not m:
        return result
    result["total_pairs"] = int(m.group(1))

    # Concordant exactly 1 time
    m = re.search(r"(\d+) \([\d.]+%\) aligned concordantly exactly 1 time", stderr_text)
    if m:
        result["concordant_1"] = int(m.group(1))

    # Concordant >1 times
    m = re.search(r"(\d+) \([\d.]+%\) aligned concordantly >1 times", stderr_text)
    if m:
        result["concordant_gt1"] = int(m.group(1))

    # Overall alignment rate
    m = re.search(r"([\d.]+)% overall alignment rate", stderr_text)
    if m:
        result["overall_rate_pct"] = float(m.group(1))

    # Compute total aligned pairs (concordant + discordant + mixed)
    # Simplest: total_pairs - concordant_0
    m = re.search(r"(\d+) \([\d.]+%\) aligned concordantly 0 times", stderr_text)
    if m:
        concordant_0 = int(m.group(1))
        result["concordant_0"] = concordant_0
        # The mapped FASTQ includes any pair where at least one mate aligned
        # so "mapped pairs" ≈ total - concordant_0 + discordant + mixed
        # But the pipeline uses samtools -G 12 (at least one mate mapped)
        # which captures all non-fully-unmapped pairs.
        # From the overall rate we can derive total mapped read-ends.

    return result


def get_bowtie2_counts_from_trace(
    trace_file: Path,
) -> dict[str, dict[str, dict[str, int]]]:
    """Parse Bowtie2 alignment stats from work directory .command.err files.

    Uses the Nextflow trace file to locate work directories for each
    Bowtie2 process invocation.

    Args:
        trace_file: Path to Nextflow trace TSV file.

    Returns:
        Nested dict: {sample: {suffix: {stats}}} where suffix is
        "virus", "human", or "other".
    """
    results: dict[str, dict[str, dict[str, int]]] = {}

    with open(trace_file) as f:
        reader = csv.DictReader(f, delimiter="\t")
        # Strip whitespace from field names (trace files have spaces)
        reader.fieldnames = [fn.strip() for fn in reader.fieldnames] if reader.fieldnames else []
        for row in reader:
            row = {k.strip(): v.strip() for k, v in row.items()}
            name = row.get("name", "")
            status = row.get("status", "")
            workdir = row.get("workdir", "")

            if status != "COMPLETED":
                continue
            if "BOWTIE2_VIRUS" not in name and "BOWTIE2_HUMAN" not in name and "BOWTIE2_OTHER" not in name:
                continue

            # Extract sample name from task name like "BOWTIE2_VIRUS (sample_name)"
            m = re.match(r"BOWTIE2_(\w+)\s*\((.+)\)", name)
            if not m:
                continue
            suffix = m.group(1).lower()  # virus, human, or other
            sample = m.group(2).strip()

            # Read .command.err from work directory
            err_file = Path(workdir) / ".command.err"
            if not err_file.exists():
                logger.warning("Work dir not accessible: %s", err_file)
                continue

            stderr_text = err_file.read_text()
            stats = parse_bowtie2_stderr(stderr_text)
            if not stats:
                continue

            if sample not in results:
                results[sample] = {}
            results[sample][suffix] = stats

    return results


################
# MAIN OUTPUT  #
################


def print_funnel(
    results_dir: Path,
    trace_file: Path | None,
) -> None:
    """Print the read funnel summary for all samples.

    Args:
        results_dir: Pipeline results directory.
        trace_file: Optional path to Nextflow trace TSV.
    """
    input_counts = get_input_read_counts(results_dir)
    fastp_counts = get_fastp_counts(results_dir)
    virus_counts = get_virus_hits_counts(results_dir)

    bt2_counts: dict[str, dict[str, dict[str, int]]] = {}
    have_bt2 = False
    if trace_file:
        bt2_counts = get_bowtie2_counts_from_trace(trace_file)
        have_bt2 = bool(bt2_counts)
        if not have_bt2:
            logger.warning(
                "No Bowtie2 stats found from trace file. "
                "Work directories may not be accessible."
            )

    all_samples = sorted(
        set(input_counts) | set(fastp_counts) | set(virus_counts) | set(bt2_counts)
    )

    if not all_samples:
        logger.warning("No samples found in %s", results_dir)
        return

    def fmt(n: int | float | None) -> str:
        if n is None:
            return "—"
        if isinstance(n, float):
            return f"{n:.1f}%"
        return f"{n:,}"

    def pct(num: int | None, denom: int | None) -> str:
        if num is not None and denom is not None and denom > 0:
            return f"{100 * num / denom:.2f}%"
        return "—"

    # Build columns
    cols = ["Sample", "Input pairs", "BBDuk match", "FASTP pass"]
    if have_bt2:
        cols += ["BT2 viral in", "BT2 viral rate", "BT2 human rate", "BT2 other rate"]
    cols += ["Virus hits"]

    col_widths = [45, 14, 14, 14]
    if have_bt2:
        col_widths += [14, 13, 13, 13]
    col_widths += [12]

    def row_str(values: list[str]) -> str:
        parts = []
        for v, w in zip(values, col_widths):
            if parts:  # right-align all but first
                parts.append(f"{v:>{w}}")
            else:
                parts.append(f"{v:<{w}}")
        return " ".join(parts)

    # Header
    print(row_str(cols))
    print("-" * sum(col_widths + [len(col_widths) - 1]))

    # Accumulators for totals
    totals: dict[str, int] = {
        "input": 0, "bbduk": 0, "fastp": 0, "virus": 0,
        "bt2_viral_in": 0, "bt2_viral_mapped": 0,
        "bt2_human_in": 0, "bt2_human_mapped": 0,
        "bt2_other_in": 0, "bt2_other_mapped": 0,
    }

    for sample in all_samples:
        input_n = input_counts.get(sample)
        fastp = fastp_counts.get(sample, {})
        bbduk_n = fastp.get("before_pairs")
        fastp_n = fastp.get("after_pairs")
        virus_n = virus_counts.get(sample)
        bt2 = bt2_counts.get(sample, {})

        if input_n is not None:
            totals["input"] += input_n
        if bbduk_n is not None:
            totals["bbduk"] += bbduk_n
        if fastp_n is not None:
            totals["fastp"] += fastp_n
        if virus_n is not None:
            totals["virus"] += virus_n

        values = [sample, fmt(input_n), fmt(bbduk_n), fmt(fastp_n)]

        if have_bt2:
            viral = bt2.get("virus", {})
            human = bt2.get("human", {})
            other = bt2.get("other", {})

            viral_in = viral.get("total_pairs")
            viral_rate = viral.get("overall_rate_pct")
            human_rate = human.get("overall_rate_pct")
            other_rate = other.get("overall_rate_pct")

            if viral_in is not None:
                totals["bt2_viral_in"] += viral_in
            if viral.get("concordant_0") is not None and viral_in is not None:
                totals["bt2_viral_mapped"] += viral_in - viral.get("concordant_0", 0)
            if human.get("total_pairs") is not None:
                totals["bt2_human_in"] += human["total_pairs"]
                if human.get("concordant_0") is not None:
                    totals["bt2_human_mapped"] += human["total_pairs"] - human["concordant_0"]
            if other.get("total_pairs") is not None:
                totals["bt2_other_in"] += other["total_pairs"]
                if other.get("concordant_0") is not None:
                    totals["bt2_other_mapped"] += other["total_pairs"] - other["concordant_0"]

            values += [fmt(viral_in), fmt(viral_rate), fmt(human_rate), fmt(other_rate)]

        values += [fmt(virus_n)]
        print(row_str(values))

    # Totals
    print("-" * sum(col_widths + [len(col_widths) - 1]))
    total_values: list[str] = [
        "TOTAL",
        fmt(totals["input"]),
        fmt(totals["bbduk"]),
        fmt(totals["fastp"]),
    ]
    if have_bt2:
        total_values += [
            fmt(totals["bt2_viral_in"]),
            pct(totals["bt2_viral_mapped"], totals["bt2_viral_in"]),
            pct(totals["bt2_human_mapped"], totals["bt2_human_in"]),
            pct(totals["bt2_other_mapped"], totals["bt2_other_in"]),
        ]
    total_values += [fmt(totals["virus"])]
    print(row_str(total_values))


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
        help="Path to pipeline results directory (local).",
    )
    parser.add_argument(
        "--trace-file",
        type=Path,
        default=None,
        help=(
            "Path to Nextflow trace TSV (e.g. output/logging/trace_*.tsv). "
            "Required for Bowtie2 intermediate counts. The work directories "
            "referenced in the trace must be accessible."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Print read funnel summary from pipeline results."""
    start_time = time.time()
    args = parse_arguments()
    logger.info("Reading results from %s", args.results_dir)
    if not args.results_dir.is_dir():
        raise FileNotFoundError(f"Results directory not found: {args.results_dir}")
    if args.trace_file:
        logger.info("Using trace file: %s", args.trace_file)
    else:
        logger.info("No trace file provided — Bowtie2 intermediate counts will be omitted")
    print()
    print_funnel(args.results_dir, args.trace_file)
    print()
    elapsed = time.time() - start_time
    logger.info("Done in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
