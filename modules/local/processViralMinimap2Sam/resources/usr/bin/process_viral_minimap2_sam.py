#!/usr/bin/env python3

import sys
import argparse
import pandas as pd
import time
import datetime
import pysam
import gzip
import bz2
import math
from Bio.Seq import Seq

HEADER_FIELDS = [
    "seq_id", "genome_id", "genome_id_all", "taxid", "taxid_all",
    "map_qual", "ref_start", "cigar", "edit_distance",
    "best_alignment_score", "next_alignment_score", "length_normalized_score",
    "query_seq", "query_rc", "query_qual", "query_len", "classification",
]


def print_log(message):
    print("[", datetime.datetime.now(), "]\t", message, sep="", file=sys.stderr)


def open_by_suffix(filename, mode="r"):
    if filename.endswith(".gz"):
        return gzip.open(filename, mode + "t")
    elif filename.endswith(".bz2"):
        return bz2.BZ2file(filename, mode)
    else:
        return open(filename, mode)


def read_fastq_record(fh):
    """Read one FASTQ record (4 lines). Returns (read_id, seq, qual) or None at EOF."""
    header = fh.readline().strip()
    if not header:
        return None
    seq = fh.readline().strip()
    fh.readline()  # + line
    qual = fh.readline().strip()
    read_id = header[1:].split()[0]  # Strip @ prefix, take first field
    return (read_id, seq, qual)


def extract_viral_taxid(genome_id, genbank_metadata, viral_taxids):
    """Extract taxid from the appropriate field of Genbank metadata."""
    try:
        taxid, species_taxid = genbank_metadata[genome_id]
        if taxid in viral_taxids:
            return taxid
        if species_taxid in viral_taxids:
            return species_taxid
        return taxid
    except KeyError:
        raise ValueError(f"No matching genome ID found: {genome_id}")


def parse_sam_alignment(read, genbank_metadata, viral_taxids, clean_seq, clean_qual):
    """Parse a Minimap2 SAM alignment into an output dict."""
    out = {}
    out["seq_id"] = read.query_name
    out["genome_id"] = read.reference_name
    out["genome_id_all"] = out["genome_id"]  # consistent with short read pipeline
    taxid = extract_viral_taxid(read.reference_name, genbank_metadata, viral_taxids)
    out["taxid"] = taxid
    out["taxid_all"] = taxid  # consistent with short read pipeline

    # Reverse-complement seq/qual when minimap2 mapped to the RC strand
    if read.is_reverse:
        query_seq_clean = str(Seq(clean_seq).reverse_complement())
        query_qual_clean = clean_qual[::-1]
    else:
        query_seq_clean = clean_seq
        query_qual_clean = clean_qual

    out["map_qual"] = read.mapping_quality
    out["ref_start"] = read.reference_start
    out["cigar"] = read.cigarstring
    out["edit_distance"] = read.get_tag("NM")
    out["best_alignment_score"] = read.get_tag("AS")
    out["next_alignment_score"] = "NA"
    out["length_normalized_score"] = out["best_alignment_score"] / math.log(
        len(clean_seq)
    )
    out["query_seq"] = query_seq_clean
    out["query_rc"] = read.is_reverse
    out["query_qual"] = query_qual_clean
    out["query_len"] = len(clean_seq)
    out["classification"] = (
        "supplementary"
        if read.is_supplementary
        else "secondary"
        if read.is_secondary
        else "primary"
    )
    return out


def process_sam(sam_file, out_file, genbank_metadata, viral_taxids, fastq_file):
    """Process a Minimap2 SAM file using streaming merge join with sorted FASTQ.

    Both sam_file and fastq_file must be sorted by read ID.
    """
    header = "\t".join(HEADER_FIELDS) + "\n"
    with open_by_suffix(out_file, "w") as out_fh:
        out_fh.write(header)
        with pysam.AlignmentFile(sam_file, "r") as sam_fh, open(
            fastq_file
        ) as fastq_fh:
            num_reads = 0
            fastq_record = read_fastq_record(fastq_fh)
            for read in sam_fh:
                num_reads += 1
                print_log(f"Processing read: {read.query_name}")
                if read.is_unmapped:
                    continue
                read_id = read.query_name

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
                    read, genbank_metadata, viral_taxids,
                    fastq_record[1], fastq_record[2],
                )
                out_fh.write("\t".join(map(str, line.values())) + "\n")
            if num_reads == 0:
                print_log(
                    "Warning: Input SAM file is empty. "
                    "Creating empty output with header only."
                )


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Process Minimap2 SAM output into a TSV with viral alignment information."
    )
    parser.add_argument("-a", "--sam", required=True,
                        help="Path to Minimap2 SAM alignment file.")
    parser.add_argument("-r", "--reads", required=True,
                        help="Path to sorted FASTQ file with non-masked viral reads.")
    parser.add_argument("-m", "--metadata", required=True,
                        help="Path to Genbank metadata file.")
    parser.add_argument("-v", "--viral_db", required=True,
                        help="Path to TSV with viral taxonomic information.")
    parser.add_argument("-o", "--output", required=True,
                        help="Output path for processed data frame.")
    return parser.parse_args()


def main():
    args = parse_arguments()
    try:
        print_log("Starting process.")
        start_time = time.time()

        meta_db = pd.read_csv(args.metadata, sep="\t", dtype=str)
        genbank_metadata = {
            gid: [tid, stid]
            for gid, tid, stid in zip(
                meta_db["genome_id"], meta_db["taxid"], meta_db["species_taxid"]
            )
        }
        virus_db = pd.read_csv(args.viral_db, sep="\t", dtype=str)
        viral_taxids = set(virus_db["taxid"].values)
        print_log(f"Imported {len(genbank_metadata)} genomes, {len(viral_taxids)} virus taxa.")

        print_log("Processing SAM file...")
        process_sam(args.sam, args.output, genbank_metadata, viral_taxids, args.reads)

        print_log(f"Done. Total time: {time.time() - start_time:.2f}s")
    except Exception as e:
        print_log(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
