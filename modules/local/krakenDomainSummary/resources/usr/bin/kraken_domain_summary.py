#!/usr/bin/env python3
DESC = """
Create a domain-level abundance table from a Kraken2 report.

Kraken2 clade counts already give the reads assigned at or below each domain.
Reads classified *above* the domain rows are redistributed down the taxonomy in
two levels that respect the tree, mirroring how Bracken behaves:

1. Reads sitting at "cellular organisms" (taxid 131567) can only belong to
   cellular life, so they are split among Bacteria/Archaea/Eukaryota in
   proportion to their clade counts and never assigned to Viruses.
2. The remaining root-level residual is split across all four domains in
   proportion to their clade counts.

Naively prorating the whole above-domain residual across all four domains would
systematically over-assign the rare Viruses domain, since most of that residual
is cellular-organisms reads that Bracken never routes to Viruses.

Domains are identified by stable NCBI taxid rather than Kraken rank code, so the
summary is immune to the taxonomy-version rank-code shifts (e.g. domains losing
rank code "D") that motivated replacing Bracken.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

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

#############
# CONSTANTS #
#############

VIRUS_TAXID = 10239
# Bacteria, Archaea, Eukaryota — the cellular superkingdoms, all nested under the
# stable "cellular organisms" node (taxid 131567) in the NCBI taxonomy.
CELLULAR_DOMAIN_TAXIDS = frozenset({2, 2157, 2759})
DOMAIN_TAXIDS = CELLULAR_DOMAIN_TAXIDS | {VIRUS_TAXID}
CELLULAR_ORGANISMS_TAXID = 131567
ROOT_TAXID = 1
KRAKEN_FIELD_COUNT = 8
OUTPUT_FIELDS = [
    "name",
    "taxid",
    "rank",
    "kraken2_assigned_reads",
    "added_reads",
    "new_est_reads",
    "fraction_total_reads",
]

###############
# DATA MODELS #
###############


@dataclass(frozen=True)
class KrakenRow:
    """The fields we use from one Kraken2 report row."""

    reads_clade: int
    rank: str
    taxid: int
    name: str


####################
# HELPER FUNCTIONS #
####################


def open_by_suffix(filename: str | Path, mode: str = "r") -> IO[str]:
    """
    Parse the suffix of a filename to determine the right open method to use.

    Args:
        filename: Path to file to open.
        mode: File open mode.

    Returns:
        File handle appropriate for the file compression type.
    """
    filename_str = str(filename)
    if filename_str.endswith(".gz"):
        return gzip.open(filename_str, mode + "t")  # type: ignore[return-value]
    return open(filename_str, mode)


def read_kraken_report(report_path: str | Path) -> list[KrakenRow]:
    """
    Read the rows we need from a plain or gzipped Kraken2 report.

    Args:
        report_path: Path to a plain or gzipped Kraken2 report.

    Returns:
        Parsed report rows, skipping blank and header lines.
    """
    rows: list[KrakenRow] = []
    with open_by_suffix(report_path) as report_file:
        for line in report_file:
            fields = line.rstrip("\n").split("\t")
            # Skip blank lines and a header row (which starts with "pc_reads_total").
            if len(fields) < KRAKEN_FIELD_COUNT or fields[0] == "pc_reads_total":
                continue
            rows.append(
                KrakenRow(int(fields[1]), fields[5], int(fields[6]), fields[7].strip())
            )
    return rows


def prorate(residual: int, weights: list[int]) -> list[int]:
    """
    Split `residual` reads across domains in proportion to their clade counts.

    Uses floor division, so a small remainder (at most one read per domain) may
    go unallocated; this is an abundance estimate, not an exact partition.

    Args:
        residual: Reads to distribute (a no-op when non-positive).
        weights: Domain clade counts to distribute in proportion to.

    Returns:
        Per-domain integer allocations, in the same order as `weights`.
    """
    total = sum(weights)
    if residual <= 0 or total <= 0:
        return [0] * len(weights)
    return [residual * weight // total for weight in weights]


def summarize_domains(rows: list[KrakenRow]) -> list[list[str]]:
    """
    Build the domain abundance table from Kraken2 report rows.

    See the module docstring for the two-level redistribution scheme.

    Args:
        rows: Parsed Kraken2 report rows.

    Returns:
        Output rows as stringified fields in OUTPUT_FIELDS order, or an empty
        list when the report has no root row or no recognized domain reads.
    """
    clade = {row.taxid: row.reads_clade for row in rows}
    if ROOT_TAXID not in clade:
        logger.warning("Kraken report has no root row; writing empty output.")
        return []
    # One row per domain taxid in report order (a well-formed report has no dupes).
    domain_rows = [row for row in rows if row.taxid in DOMAIN_TAXIDS]
    if sum(row.reads_clade for row in domain_rows) == 0:
        logger.warning(
            "Kraken report has no recognized domain reads; writing empty output."
        )
        return []

    added = {row.taxid: 0 for row in domain_rows}

    def add_prorated(target_rows: list[KrakenRow], residual: int) -> None:
        """Prorate `residual` across `target_rows` by clade count and bank it."""
        weights = [row.reads_clade for row in target_rows]
        for row, share in zip(target_rows, prorate(residual, weights), strict=True):
            added[row.taxid] += share

    # Level 1: residual inside "cellular organisms" -> cellular domains only.
    cellular_rows = [row for row in domain_rows if row.taxid in CELLULAR_DOMAIN_TAXIDS]
    cellular_clade = sum(row.reads_clade for row in cellular_rows)
    # Anchor on the "cellular organisms" clade when that node is present; otherwise
    # fall back to the cellular domains themselves so the report degrades to level 2.
    cellular_anchor = (
        clade.get(CELLULAR_ORGANISMS_TAXID, cellular_clade)
        if cellular_rows
        else cellular_clade
    )
    add_prorated(cellular_rows, cellular_anchor - cellular_clade)

    # Level 2: remaining root-level residual -> all domains.
    root_residual = clade[ROOT_TAXID] - cellular_anchor - clade.get(VIRUS_TAXID, 0)
    add_prorated(domain_rows, root_residual)

    total_reads = clade[ROOT_TAXID]
    summary = []
    for row in domain_rows:
        new_est_reads = row.reads_clade + added[row.taxid]
        summary.append(
            [
                row.name,
                str(row.taxid),
                row.rank,
                str(row.reads_clade),
                str(added[row.taxid]),
                str(new_est_reads),
                f"{new_est_reads / total_reads:.5f}",
            ]
        )
    return summary


def write_summary(rows: list[list[str]], output_path: str | Path) -> None:
    """
    Write the domain abundance table (header + rows), or an empty file if no rows.

    Args:
        rows: Output rows from summarize_domains.
        output_path: Path to the output TSV, optionally gzipped.
    """
    with open_by_suffix(output_path, "w") as output_file:
        if not rows:
            return
        output_file.write("\t".join(OUTPUT_FIELDS) + "\n")
        for row in rows:
            output_file.write("\t".join(row) + "\n")


def create_domain_summary(report_path: str | Path, output_path: str | Path) -> None:
    """
    Create a domain-level abundance summary from a Kraken2 report.

    Args:
        report_path: Path to a plain or gzipped Kraken2 report.
        output_path: Path to write the output TSV.
    """
    write_summary(summarize_domains(read_kraken_report(report_path)), output_path)


##########################
# COMMAND-LINE INTERFACE #
##########################


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument("report", help="Plain or gzipped Kraken2 report.")
    parser.add_argument("output", help="Output TSV path.")
    return parser.parse_args()


def main() -> None:
    """Run the command-line entry point."""
    args = parse_arguments()
    logger.info("Reading Kraken2 report: %s", args.report)
    create_domain_summary(args.report, args.output)
    logger.info("Wrote domain summary: %s", args.output)


if __name__ == "__main__":
    main()
