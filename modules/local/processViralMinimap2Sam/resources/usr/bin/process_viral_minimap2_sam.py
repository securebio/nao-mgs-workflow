#!/usr/bin/env python3

import argparse
import gzip
import logging
import math
import sys
from typing import Any
import tempfile
import time
from datetime import datetime, timezone

import pandas as pd
import pysam
from Bio.Seq import Seq

from sort_fastq import sort_fastq
from sort_sam import sort_sam


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


HEADER_FIELDS = [
    "seq_id",
    "genome_id",
    "genome_id_all",
    "taxid",
    "taxid_all",
    "map_qual",
    "ref_start",
    "cigar",
    "edit_distance",
    "best_alignment_score",
    "next_alignment_score",
    "length_normalized_score",
    "query_seq",
    "query_rc",
    "query_qual",
    "query_len",
    "classification",
]


def read_fastq_record(fh) -> tuple[str, str, str] | None:
    """Read one FASTQ record (4 lines). Returns (read_id, seq, qual) or None at EOF.

    Uses manual line parsing instead of BioPython SeqIO to preserve the raw ASCII
    quality string — SeqIO decodes it into Phred integers, which we'd have to
    re-encode since we pass it through to the output unchanged.
    """
    header = fh.readline().strip()
    if not header:
        return None
    seq = fh.readline().strip()
    fh.readline()  # + line
    qual = fh.readline().strip()
    # strip @ and take first whitespace-delimited token to match SAM QNAME
    read_id = header[1:].split()[0]
    return (read_id, seq, qual)


def extract_viral_taxid(genome_id: str, genbank_metadata, viral_taxids) -> str:
    """Return taxid for a genome, preferring whichever of taxid/species_taxid is viral."""
    try:
        taxid, species_taxid = genbank_metadata[genome_id]
        if taxid in viral_taxids:
            return taxid
        if species_taxid in viral_taxids:
            return species_taxid
        return taxid
    except KeyError:
        raise ValueError(f"No matching genome ID found: {genome_id}")


def parse_sam_alignment(
    read, genbank_metadata, viral_taxids, clean_seq: str, clean_qual: str
) -> dict[str, Any]:
    """Parse a Minimap2 SAM alignment into an output dict."""
    taxid = extract_viral_taxid(read.reference_name, genbank_metadata, viral_taxids)
    query_len = len(clean_seq)
    as_score = read.get_tag("AS")

    # Reverse-complement seq/qual when minimap2 mapped to the RC strand
    if read.is_reverse:
        clean_seq = str(Seq(clean_seq).reverse_complement())
        clean_qual = clean_qual[::-1]

    return {
        "seq_id": read.query_name,
        "genome_id": read.reference_name,
        "genome_id_all": read.reference_name,  # always == genome_id for single-end reads
        "taxid": taxid,
        "taxid_all": taxid,  # always == taxid for single-end reads
        "map_qual": read.mapping_quality,
        "ref_start": read.reference_start,
        "cigar": read.cigarstring,
        "edit_distance": read.get_tag("NM"),
        "best_alignment_score": as_score,
        "next_alignment_score": "NA",
        "length_normalized_score": (
            as_score / math.log(query_len) if query_len > 1 else 0
        ),
        "query_seq": clean_seq,
        "query_rc": read.is_reverse,
        "query_qual": clean_qual,
        "query_len": query_len,
        "classification": (
            "supplementary"
            if read.is_supplementary
            else "secondary" if read.is_secondary else "primary"
        ),
    }


def process_sam(
    sam_file: str, out_file: str, genbank_metadata, viral_taxids, fastq_file: str
) -> None:
    """Process a Minimap2 SAM file using streaming merge join with sorted FASTQ.

    Both sam_file and fastq_file must be sorted by read ID.
    """
    header = "\t".join(HEADER_FIELDS) + "\n"
    with gzip.open(out_file, "wt") as out_fh:
        out_fh.write(header)
        with pysam.AlignmentFile(sam_file, "r") as sam_fh, open(fastq_file) as fastq_fh:
            num_reads = 0
            fastq_record = read_fastq_record(fastq_fh)
            for read in sam_fh:
                num_reads += 1
                if read.is_unmapped:
                    continue
                read_id = read.query_name
                assert read_id is not None

                # Advance FASTQ pointer until we find or pass this read ID
                while fastq_record is not None and fastq_record[0] < read_id:
                    fastq_record = read_fastq_record(fastq_fh)

                if fastq_record is None or fastq_record[0] != read_id:
                    raise ValueError(
                        f"Read {read_id} found in SAM but not in FASTQ. "
                        "Ensure both files are sorted by read ID and FASTQ "
                        "contains all reads in the SAM file."
                    )

                # Don't advance FASTQ — next SAM row may have same read ID
                line = parse_sam_alignment(
                    read,
                    genbank_metadata,
                    viral_taxids,
                    fastq_record[1],
                    fastq_record[2],
                )
                out_fh.write(
                    "\t".join(str(line[field]) for field in HEADER_FIELDS) + "\n"
                )
            if num_reads == 0:
                logger.warning(
                    "Input SAM file is empty. "
                    "Creating empty output with header only."
                )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process Minimap2 SAM output into a TSV with viral alignment information."
    )
    parser.add_argument(
        "-a",
        "--sam",
        required=True,
        help="Path to gzipped Minimap2 SAM alignment file.",
    )
    parser.add_argument(
        "-r",
        "--reads",
        required=True,
        help="Path to gzipped FASTQ file with non-masked viral reads.",
    )
    parser.add_argument(
        "-m", "--metadata", required=True, help="Path to Genbank metadata file."
    )
    parser.add_argument(
        "-v",
        "--viral_db",
        required=True,
        help="Path to TSV with viral taxonomic information.",
    )
    parser.add_argument(
        "-o", "--output", required=True, help="Output path for processed data frame."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    try:
        logger.info("Starting process.")
        start_time = time.time()

        meta_db = pd.read_csv(args.metadata, sep="\t", dtype=str)
        genbank_metadata = {
            genome_id: [taxid, species_taxid]
            for genome_id, taxid, species_taxid in zip(
                meta_db["genome_id"], meta_db["taxid"], meta_db["species_taxid"]
            )
        }
        virus_db = pd.read_csv(args.viral_db, sep="\t", dtype=str)
        viral_taxids = set(virus_db["taxid"].values)
        logger.info(
            f"Imported {len(genbank_metadata)} genomes, {len(viral_taxids)} virus taxa."
        )

        # Use local directory to avoid memory-based tmpfs
        with tempfile.TemporaryDirectory(dir=".") as tmp_dir:
            sorted_sam = f"{tmp_dir}/sorted.sam"
            sorted_fastq = f"{tmp_dir}/sorted.fastq"

            logger.info("Sorting SAM by read ID...")
            sort_sam(args.sam, sorted_sam)

            logger.info("Sorting FASTQ by read ID...")
            sort_fastq(args.reads, sorted_fastq)

            logger.info("Processing SAM file...")
            process_sam(
                sorted_sam, args.output, genbank_metadata, viral_taxids, sorted_fastq
            )

        logger.info(f"Done. Total time: {time.time() - start_time:.2f}s")
    except Exception as e:
        logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
