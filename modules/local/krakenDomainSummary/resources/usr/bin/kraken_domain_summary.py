#!/usr/bin/env python3
DESC = """
Create a domain-level abundance table from a Kraken2 report.
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

# Domains are identified by stable NCBI taxid rather than by Kraken rank code,
# so the summary is immune to the taxonomy-version rank-code shifts (e.g. domains
# losing rank code "D") that motivated replacing Bracken.
VIRUS_TAXID = 10239
# Bacteria, Archaea, Eukaryota — the cellular superkingdoms, all nested under the
# stable "cellular organisms" node (taxid 131567) in the NCBI taxonomy.
CELLULAR_DOMAIN_TAXIDS = frozenset({2, 2157, 2759})
DOMAIN_TAXIDS = CELLULAR_DOMAIN_TAXIDS | {VIRUS_TAXID}
CELLULAR_ORGANISMS_TAXID = 131567
OUTPUT_FIELDS = [
    "name",
    "taxid",
    "rank",
    "kraken2_assigned_reads",
    "added_reads",
    "new_est_reads",
    "fraction_total_reads",
]
KRAKEN_FIELD_COUNT = 8
ROOT_TAXID = 1

###############
# DATA MODELS #
###############


@dataclass(frozen=True)
class KrakenReportRow:
    """One row from a Kraken2 report."""

    reads_clade: int
    rank: str
    taxid: int
    name: str


@dataclass(frozen=True)
class DomainSummaryRow:
    """One row in the domain-level abundance table."""

    name: str
    taxid: int
    rank: str
    kraken2_assigned_reads: int
    added_reads: int
    new_est_reads: int
    fraction_total_reads: float


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


def parse_kraken_report_line(line: str) -> KrakenReportRow | None:
    """
    Parse a single Kraken2 report line.

    Args:
        line: One tab-separated Kraken2 report line.

    Returns:
        Parsed report row, or None for a header or blank line.
    """
    if not line.strip():
        return None
    fields = line.rstrip("\n").split("\t")
    if fields[0] == "pc_reads_total":
        return None
    if len(fields) < KRAKEN_FIELD_COUNT:
        raise ValueError(
            f"Expected at least {KRAKEN_FIELD_COUNT} Kraken report fields, got {len(fields)}: "
            f"{line.rstrip()}"
        )
    return KrakenReportRow(
        reads_clade=int(fields[1]),
        rank=fields[5],
        taxid=int(fields[6]),
        name=fields[7].strip(),
    )


def read_kraken_report(report_path: str | Path) -> list[KrakenReportRow]:
    """
    Read a Kraken2 report.

    Args:
        report_path: Path to a plain or gzipped Kraken2 report.

    Returns:
        Parsed report rows.
    """
    rows: list[KrakenReportRow] = []
    with open_by_suffix(report_path) as report_file:
        for line in report_file:
            if row := parse_kraken_report_line(line):
                rows.append(row)
    return rows


def allocate_reads_proportionally(
    reads_to_allocate: int, domain_reads: list[int]
) -> list[int]:
    """
    Allocate reads across domains in proportion to observed domain counts.

    Uses largest-remainder integer allocation so the output read counts remain
    integers and the allocations sum exactly to reads_to_allocate.

    Args:
        reads_to_allocate: Number of reads assigned above recognized domains.
        domain_reads: Kraken clade counts for recognized domains.

    Returns:
        Integer read allocations in the same order as domain_reads.
    """
    total_domain_reads = sum(domain_reads)
    if reads_to_allocate <= 0 or total_domain_reads <= 0:
        return [0 for _ in domain_reads]

    allocations = [
        reads_to_allocate * reads // total_domain_reads for reads in domain_reads
    ]
    remainders = [
        reads_to_allocate * reads % total_domain_reads for reads in domain_reads
    ]
    remaining_reads = reads_to_allocate - sum(allocations)
    for index in sorted(
        range(len(domain_reads)), key=lambda i: (-remainders[i], -domain_reads[i], i)
    )[:remaining_reads]:
        allocations[index] += 1
    return allocations


def summarize_domains(rows: list[KrakenReportRow]) -> list[DomainSummaryRow]:
    """
    Create Bracken-shaped domain abundance rows from a Kraken2 report.

    Reads classified above the domain rows are redistributed down the taxonomy in
    two levels that respect the tree, mirroring how Bracken's k-mer model
    behaves:

    1. Reads sitting at "cellular organisms" (taxid 131567), above the three
       cellular superkingdoms, can only belong to cellular life. They are split
       among Bacteria/Archaea/Eukaryota in proportion to their clade counts and
       are never assigned to Viruses.
    2. The remaining root-level residual (reads directly at the root, plus any
       other non-cellular, non-viral root children) is split across all four
       domains in proportion to their clade counts.

    Naively prorating the whole above-domain residual across all four domains
    instead systematically over-assigns the rare Viruses domain, because the bulk
    of that residual is cellular-organisms reads that Bracken never routes to
    Viruses.

    Args:
        rows: Parsed Kraken2 report rows.

    Returns:
        Domain abundance summary rows.
    """
    rows_by_taxid = {row.taxid: row for row in rows}
    root_row = rows_by_taxid.get(ROOT_TAXID)
    if root_row is None:
        logger.warning("Kraken report has no root row. Creating empty output.")
        return []

    # One row per domain taxid, in report order. A well-formed Kraken report has
    # no duplicate taxids; dedupe defensively so a malformed report can't
    # double-count (the dict keeps the last row per taxid, matching rows_by_taxid).
    domain_rows = list(
        {row.taxid: row for row in rows if row.taxid in DOMAIN_TAXIDS}.values()
    )
    if sum(row.reads_clade for row in domain_rows) == 0:
        logger.warning("Kraken report has no recognized domain reads.")
        return []

    added: dict[int, int] = {}

    # Level 1: residual within "cellular organisms" -> cellular domains only.
    cellular_present = [
        row for row in domain_rows if row.taxid in CELLULAR_DOMAIN_TAXIDS
    ]
    cellular_clade_sum = sum(row.reads_clade for row in cellular_present)
    cellular_organisms_row = rows_by_taxid.get(CELLULAR_ORGANISMS_TAXID)
    # Anchor the cellular subtree at the "cellular organisms" clade when that node
    # and at least one cellular domain are present; otherwise treat all residual
    # as root-level so the allocation degrades gracefully.
    if cellular_organisms_row is not None and cellular_present:
        cellular_anchor = cellular_organisms_row.reads_clade
    else:
        cellular_anchor = cellular_clade_sum
    cellular_residual = max(cellular_anchor - cellular_clade_sum, 0)
    for row, allocation in zip(
        cellular_present,
        allocate_reads_proportionally(
            cellular_residual, [row.reads_clade for row in cellular_present]
        ),
        strict=True,
    ):
        added[row.taxid] = allocation

    # Level 2: remaining root-level residual -> all domains.
    virus_row = rows_by_taxid.get(VIRUS_TAXID)
    virus_clade = virus_row.reads_clade if virus_row else 0
    root_residual = max(root_row.reads_clade - cellular_anchor - virus_clade, 0)
    for row, allocation in zip(
        domain_rows,
        allocate_reads_proportionally(
            root_residual, [row.reads_clade for row in domain_rows]
        ),
        strict=True,
    ):
        added[row.taxid] = added.get(row.taxid, 0) + allocation

    summary_rows = []
    for domain_row in domain_rows:
        domain_added = added.get(domain_row.taxid, 0)
        new_est_reads = domain_row.reads_clade + domain_added
        summary_rows.append(
            DomainSummaryRow(
                name=domain_row.name,
                taxid=domain_row.taxid,
                rank=domain_row.rank,
                kraken2_assigned_reads=domain_row.reads_clade,
                added_reads=domain_added,
                new_est_reads=new_est_reads,
                fraction_total_reads=new_est_reads / root_row.reads_clade,
            )
        )
    return summary_rows


def write_summary(rows: list[DomainSummaryRow], output_path: str | Path) -> None:
    """
    Write domain abundance rows.

    Args:
        rows: Domain abundance summary rows.
        output_path: Path to the output TSV, optionally gzipped.
    """
    with open_by_suffix(output_path, "w") as output_file:
        if not rows:
            output_file.write("")
            return

        output_file.write("\t".join(OUTPUT_FIELDS) + "\n")
        for row in rows:
            fields = [
                row.name,
                str(row.taxid),
                row.rank,
                str(row.kraken2_assigned_reads),
                str(row.added_reads),
                str(row.new_est_reads),
                f"{row.fraction_total_reads:.5f}",
            ]
            output_file.write("\t".join(fields) + "\n")


def create_domain_summary(report_path: str | Path, output_path: str | Path) -> None:
    """
    Create a domain-level abundance summary from a Kraken2 report.

    Args:
        report_path: Path to a plain or gzipped Kraken2 report.
        output_path: Path to write the output TSV.
    """
    rows = read_kraken_report(report_path)
    summary_rows = summarize_domains(rows)
    write_summary(summary_rows, output_path)


#########################
# COMMAND-LINE INTERFACE #
#########################


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
    logger.info("Writing domain summary: %s", args.output)
    create_domain_summary(args.report, args.output)
    logger.info("Done.")


if __name__ == "__main__":
    main()
