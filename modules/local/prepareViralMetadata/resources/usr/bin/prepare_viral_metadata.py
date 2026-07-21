#!/usr/bin/env python
"""Prepare viral genome metadata from NCBI datasets CLI output for downstream
filtering. Reads the merged (filtered) assembly metadata TSV, joins it with the
virus taxonomy DB to add `species_taxid`, and expands each assembly row into one
row per constituent `genome_id` using the accession -> genome_id map emitted by
DOWNLOAD_VIRAL_GENOMES. Rows whose accession is absent from the map (i.e. the
genome failed to download) are dropped so the metadata stays consistent with the
concatenated genome FASTA.
"""

import argparse
import csv
import gzip
import logging
import re
import time
from datetime import UTC, datetime
from typing import IO, cast

# Assembly accessions are GCA_/GCF_ (assembly branch); anything else is a
# sequence-branch nuccore accession. Used to prefer the assembly-branch row when
# the same genome_id is reached via both branches.
ASSEMBLY_ACCESSION_RE = re.compile(r"^GC[AF]_")


def is_assembly_accession(accession: str) -> bool:
    """True if `accession` is an assembly (GCA_/GCF_) rather than a nuccore accession."""
    return bool(ASSEMBLY_ACCESSION_RE.match(accession))


class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created, UTC).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(UTCFormatter("[%(asctime)s] %(message)s"))
logger.handlers.clear()
logger.addHandler(handler)


def open_by_suffix(path: str, mode: str = "r", newline: str | None = None) -> IO[str]:
    """Open a file, transparently handling .gz compression (text mode)."""
    if path.endswith(".gz"):
        return cast(IO[str], gzip.open(path, mode + "t", newline=newline))
    return cast(IO[str], open(path, mode, newline=newline))


def build_species_taxid_map(virus_db_path: str) -> dict[str, str]:
    """Build taxid -> species_taxid mapping from virus taxonomy DB.
    Args:
        virus_db_path: Path to TSV with 'taxid' and 'taxid_species' columns.
    Returns:
        Dictionary mapping taxid to species-level taxid.
    """
    with open_by_suffix(virus_db_path, newline="") as f:
        result = {
            row["taxid"]: row["taxid_species"]
            for row in csv.DictReader(f, delimiter="\t")
        }
    logger.info("Read %d entries from virus DB", len(result))
    return result


def read_accession_map(accession_map_path: str) -> dict[str, list[str]]:
    """Read the assembly_accession -> genome_id map emitted by download.
    Args:
        accession_map_path: Path to TSV with 'assembly_accession' and 'genome_id'
            columns (one row per downloaded sequence).
    Returns:
        Dictionary mapping each assembly accession to its ordered list of
        genome IDs (order preserved as encountered).
    """
    result: dict[str, list[str]] = {}
    with open_by_suffix(accession_map_path, newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            result.setdefault(row["assembly_accession"], []).append(row["genome_id"])
    logger.info(
        "Read map for %d accessions (%d genome IDs)",
        len(result),
        sum(len(v) for v in result.values()),
    )
    return result


def prepare_metadata(
    merged_metadata_path: str,
    virus_db_path: str,
    accession_map_path: str,
    output_metadata_path: str,
) -> None:
    """Add species_taxid and expand each assembly row to one row per genome_id.

    Output rows are deduplicated by `genome_id` unconditionally, keeping the FASTA
    (which `CONCATENATE_GENOME_FASTA` already dedups by name) and metadata 1:1 on
    `genome_id`. When both sourcing branches feed in (virus_source="both"), a
    nuccore genome reached both inside an assembly (assembly branch) and as its
    own sequence record (sequence branch) collapses to one row, preferring the
    assembly-branch row (GCA_/GCF_ accession) so the genome keeps its assembly
    provenance; otherwise the first-seen row wins. In the assembly-only path this
    is inert in practice — distinct assemblies do not share a constituent nuccore
    accession — but it is applied unconditionally for consistency.

    Args:
        merged_metadata_path: Path to merged (filtered) metadata TSV (may be gz).
        virus_db_path: Path to virus taxonomy DB TSV.
        accession_map_path: Path to accession -> genome_id map TSV.
        output_metadata_path: Output path for the expanded metadata TSV (gzip).
    """
    taxid_to_species = build_species_taxid_map(virus_db_path)
    acc_to_gids = read_accession_map(accession_map_path)
    with open_by_suffix(merged_metadata_path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        in_fields = reader.fieldnames or []
        rows = list(reader)
    logger.info("Read %d metadata rows", len(rows))
    out_fields = list(in_fields) + ["species_taxid", "genome_id"]
    n_in = n_dropped = n_expanded = 0
    # First-seen order preserved; on a genome_id collision, prefer the
    # assembly-branch row over a sequence-branch row.
    best: dict[str, dict[str, str]] = {}
    for row in rows:
        n_in += 1
        gids = acc_to_gids.get(row["assembly_accession"])
        if not gids:
            n_dropped += 1
            continue
        species = taxid_to_species.get(row["taxid"], "")
        for gid in gids:
            n_expanded += 1
            out_row = dict(row)
            out_row["species_taxid"] = species
            out_row["genome_id"] = gid
            existing = best.get(gid)
            if existing is None or (
                is_assembly_accession(out_row["assembly_accession"])
                and not is_assembly_accession(existing["assembly_accession"])
            ):
                best[gid] = out_row
    with open_by_suffix(output_metadata_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, delimiter="\t")
        writer.writeheader()
        for out_row in best.values():
            writer.writerow(out_row)
    logger.info(
        "Wrote %d genome rows from %d assemblies (dropped %d undownloaded, "
        "collapsed %d duplicate genome_ids)",
        len(best),
        n_in,
        n_dropped,
        n_expanded - len(best),
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("merged_metadata", help="Path to merged metadata TSV.")
    parser.add_argument("virus_db", help="Path to virus taxonomy DB TSV.")
    parser.add_argument("accession_map", help="Path to accession -> genome_id map TSV.")
    parser.add_argument(
        "output_metadata", help="Output path for expanded metadata TSV (gzip)."
    )
    return parser.parse_args()


def main() -> None:
    start_time = time.time()
    logger.info("Starting prepare_viral_metadata.")
    args = parse_arguments()
    prepare_metadata(
        args.merged_metadata,
        args.virus_db,
        args.accession_map,
        args.output_metadata,
    )
    logger.info("Total time elapsed: %.2f seconds", time.time() - start_time)


if __name__ == "__main__":
    main()
