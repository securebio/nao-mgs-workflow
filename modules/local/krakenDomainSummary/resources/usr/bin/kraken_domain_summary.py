#!/usr/bin/env python3
DESC = """
Summarize domain-level abundance from a Kraken2 report.

Kraken2 clade counts already give the reads assigned at or below each domain.
Reads classified *above* the domains are redistributed down the tree in two
levels, mirroring Bracken:

1. Reads at "cellular organisms" (taxid 131567) can only be cellular life, so
   they are split among Bacteria/Archaea/Eukaryota by clade count, never Viruses.
2. The remaining root-level reads are split across all four domains by clade count.

Splitting the whole above-domain residual across all four domains at once would
over-assign the rare Viruses domain, since most of that residual is cellular reads.

Domains are keyed on stable NCBI taxid rather than Kraken rank code, so the
summary survives the taxonomy-version rank-code shifts that broke Bracken.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, NamedTuple

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

ROOT_TAXID = 1
CELLULAR_ORGANISMS_TAXID = 131567
VIRUS_TAXID = 10239
# Bacteria, Archaea, Eukaryota: the cellular domains, nested under "cellular
# organisms" (131567). Viruses sits directly under the root.
CELLULAR_DOMAIN_TAXIDS = (2, 2157, 2759)
DOMAIN_TAXIDS = (*CELLULAR_DOMAIN_TAXIDS, VIRUS_TAXID)
# The domains plus the structural nodes we prorate against.
RELEVANT_TAXIDS = frozenset({ROOT_TAXID, CELLULAR_ORGANISMS_TAXID, *DOMAIN_TAXIDS})
# Kraken2 --report-minimizer-data columns:
# pct, clade reads, direct reads, minimizers, distinct minimizers, rank, taxid, name.
KRAKEN_FIELD_COUNT = 8
OUTPUT_FIELDS = [
    "name",
    "taxid",
    "rank",
    "kraken2_assigned_reads",
    "added_reads",
    "new_est_reads",
    "fraction_total_reads",
    "sample",
]


class KrakenRow(NamedTuple):
    """The fields we keep from one Kraken2 report row."""

    clade: int
    rank: str
    name: str


#############
# FUNCTIONS #
#############


def open_by_suffix(filename: str | Path, mode: str = "r") -> IO[str]:
    """
    Open a file for reading or writing, transparently handling gzip.

    Args:
        filename: Path to the file; treated as gzipped iff it ends in ".gz".
        mode: Base file mode, "r" or "w".

    Returns:
        A text-mode file handle appropriate for the file's compression.
    """
    filename_str = str(filename)
    if filename_str.endswith(".gz"):
        return gzip.open(filename_str, mode + "t")  # type: ignore[return-value]
    return open(filename_str, mode)


def read_report(report_path: str | Path) -> dict[int, KrakenRow]:
    """
    Read the rows we summarize from a Kraken2 report (plain or gzipped).

    Args:
        report_path: Path to a Kraken2 report, plain or gzipped.

    Returns:
        {taxid: KrakenRow} for the root, "cellular organisms", and domain taxids.
    """
    rows: dict[int, KrakenRow] = {}
    with open_by_suffix(report_path) as report_file:
        for line in report_file:
            fields = line.rstrip("\n").split("\t")
            if len(fields) < KRAKEN_FIELD_COUNT:
                continue
            taxid = int(fields[6])
            if taxid in RELEVANT_TAXIDS:
                rows[taxid] = KrakenRow(int(fields[1]), fields[5], fields[7].strip())
    return rows


def summarize_domains(rows: dict[int, KrakenRow], sample: str) -> list[list[str]]:
    """
    Build the domain abundance table from parsed Kraken2 report rows.

    See the module docstring for the two-level redistribution scheme.

    Args:
        rows: {taxid: KrakenRow} from read_report.
        sample: Sample identifier, written into the trailing "sample" column.

    Returns:
        Output rows (stringified, in OUTPUT_FIELDS order), or an empty list when
        the report has no root row or no recognized domain reads.
    """
    if ROOT_TAXID not in rows:
        logger.warning("Kraken report has no root row; writing empty output.")
        return []
    domains = [taxid for taxid in DOMAIN_TAXIDS if taxid in rows]
    if sum(rows[taxid].clade for taxid in domains) == 0:
        logger.warning("Kraken report has no domain reads; writing empty output.")
        return []

    clade = {taxid: row.clade for taxid, row in rows.items()}
    added = dict.fromkeys(domains, 0)

    def prorate(taxids: list[int], residual: int) -> None:
        """
        Split `residual` across `taxids` by clade count, banking into `added`.

        Uses largest-remainder apportionment so the whole residual is allocated;
        plain floor division would drop up to len(taxids) - 1 reads, which can
        matter for low-count domains like Viruses.
        """
        total = sum(clade[taxid] for taxid in taxids)
        if not total or residual <= 0:
            return
        shares = {taxid: residual * clade[taxid] for taxid in taxids}
        alloc = {taxid: share // total for taxid, share in shares.items()}
        # Hand the reads lost to flooring to the largest fractional remainders.
        ranked = sorted(taxids, key=lambda taxid: shares[taxid] % total, reverse=True)
        for taxid in ranked[: residual - sum(alloc.values())]:
            alloc[taxid] += 1
        for taxid in taxids:
            added[taxid] += alloc[taxid]

    # Level 1: reads inside "cellular organisms" go to the cellular domains only,
    # falling back to the cellular-domain total when that node is absent.
    cellular = [taxid for taxid in domains if taxid in CELLULAR_DOMAIN_TAXIDS]
    cellular_clade = sum(clade[taxid] for taxid in cellular)
    cellular_total = clade.get(CELLULAR_ORGANISMS_TAXID, cellular_clade)
    prorate(cellular, cellular_total - cellular_clade)

    # Level 2: the remaining root-level reads go to all four domains.
    prorate(domains, clade[ROOT_TAXID] - cellular_total - clade.get(VIRUS_TAXID, 0))

    total_reads = clade[ROOT_TAXID]
    summary: list[list[str]] = []
    for taxid in domains:
        row = rows[taxid]
        new_est = row.clade + added[taxid]
        summary.append(
            [
                row.name,
                str(taxid),
                row.rank,
                str(row.clade),
                str(added[taxid]),
                str(new_est),
                f"{new_est / total_reads:.5f}",
                sample,
            ]
        )
    return summary


def create_domain_summary(
    report_path: str | Path, output_path: str | Path, sample: str
) -> None:
    """
    Write a domain abundance table from a Kraken2 report (plain or gzipped).

    Args:
        report_path: Path to a Kraken2 report, plain or gzipped.
        output_path: Path to the output TSV, gzipped iff it ends in ".gz"
            (empty when nothing to report).
        sample: Sample identifier, written into the trailing "sample" column.
    """
    summary = summarize_domains(read_report(report_path), sample)
    with open_by_suffix(output_path, "w") as output_file:
        if not summary:
            return
        output_file.write("\t".join(OUTPUT_FIELDS) + "\n")
        for row in summary:
            output_file.write("\t".join(row) + "\n")


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
    parser.add_argument("report", help="Kraken2 report (plain or .gz).")
    parser.add_argument("output", help="Output TSV path (gzipped if .gz).")
    parser.add_argument("sample", help="Sample identifier for the 'sample' column.")
    return parser.parse_args()


def main() -> None:
    """Run the command-line entry point."""
    args = parse_arguments()
    logger.info("Reading Kraken2 report: %s", args.report)
    create_domain_summary(args.report, args.output, args.sample)
    logger.info("Wrote domain summary: %s", args.output)


if __name__ == "__main__":
    main()
