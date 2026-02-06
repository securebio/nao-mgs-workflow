#!/usr/bin/env python3

# Import modules
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

# Utility functions


def print_log(message):
    print("[", datetime.datetime.now(), "]\t", message, sep="", file=sys.stderr)


def open_by_suffix(filename, mode="r"):
    if filename.endswith(".gz"):
        return gzip.open(filename, mode + "t")
    elif filename.endswith(".bz2"):
        return bz2.BZ2file(filename, mode)
    else:
        return open(filename, mode)


def join_line(fields):
    "Convert a list of arguments into a TSV string for output."
    return "\t".join(map(str, fields)) + "\n"


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


# Alignment-level functions


def parse_sam_alignment(read, genbank_metadata, viral_taxids, clean_seq, clean_qual):
    """Parse a Minimap2 SAM alignment.

    Args:
        read: pysam AlignedSegment
        genbank_metadata: dict of genome_id -> [taxid, species_taxid]
        viral_taxids: set of viral taxid strings
        clean_seq: unmasked read sequence string
        clean_qual: unmasked read quality string (Phred+33 ASCII)
    """
    out = {}
    out["seq_id"] = read.query_name

    reference_genome_name = read.reference_name
    out["genome_id"] = reference_genome_name
    # Added to keep consistent with short read pipeline
    out["genome_id_all"] = out["genome_id"]
    reference_taxid = extract_viral_taxid(
        reference_genome_name, genbank_metadata, viral_taxids
    )
    out["taxid"] = reference_taxid
    # Added to keep consistent with short read pipeline
    out["taxid_all"] = out["taxid"]

    # Adding original read sequence and quality
    if read.is_reverse:
        # When minimap2 maps to the RC version of a strand, return the RC version
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


# File-level functions


def process_sam(sam_file, out_file, genbank_metadata, viral_taxids, fastq_file):
    """Process a Minimap2 SAM file using streaming merge join with sorted FASTQ.

    Both sam_file and fastq_file must be sorted by read ID.
    """
    with open_by_suffix(out_file, "w") as out_fh:
        header = (
            "seq_id\t"
            "genome_id\t"
            "genome_id_all\t"
            "taxid\t"
            "taxid_all\t"
            "map_qual\t"
            "ref_start\t"
            "cigar\t"
            "edit_distance\t"
            "best_alignment_score\t"
            "next_alignment_score\t"
            "length_normalized_score\t"
            "query_seq\t"
            "query_rc\t"
            "query_qual\t"
            "query_len\t"
            "classification\n"
        )
        out_fh.write(header)
        try:
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

                    clean_seq = fastq_record[1]
                    clean_qual = fastq_record[2]
                    # Don't advance FASTQ — next SAM row may have same read ID

                    line = parse_sam_alignment(
                        read, genbank_metadata, viral_taxids, clean_seq, clean_qual
                    )
                    if line is None:
                        continue
                    line_keys = line.keys()
                    test_key_line = "\t".join(line_keys) + "\n"
                    assert test_key_line == header

                    out_fh.write(join_line(line.values()))
                if num_reads == 0:
                    print_log(
                        "Warning: Input SAM file is empty. Creating empty output with header only."
                    )

        except Exception as e:
            import traceback

            error_detail = traceback.format_exc()
            print_log(f"Error processing SAM file: {str(e)}")
            print_log(f"Error details: {error_detail}")
            raise


def parse_arguments():
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Process Minimap2 SAM output into a TSV with viral alignment information."
    )
    parser.add_argument(
        "-a",
        "--sam",
        type=str,
        required=True,
        help="Path to Minimap2 SAM alignment file.",
    )
    parser.add_argument(
        "-r",
        "--reads",
        type=str,
        required=True,
        help="Path to sorted FASTQ file with non-masked viral reads.",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        required=True,
        help="Path to Genbank metadata file containing genomeID and taxid information.",
    )
    parser.add_argument(
        "-v",
        "--viral_db",
        required=True,
        help="Path to TSV containing viral taxonomic information.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Output path for processed data frame.",
    )
    return parser.parse_args()


def main():
    # Parse arguments
    args = parse_arguments()

    try:
        sam_file = args.sam
        clean_reads = args.reads
        meta_path = args.metadata
        vdb_path = args.viral_db

        out_file = args.output
        # Start time tracking
        print_log("Starting process.")
        start_time = time.time()
        # Print parameters
        print_log("SAM file path: {}".format(sam_file))
        print_log("FASTQ file path: {}".format(clean_reads))
        print_log("Genbank metadata file path: {}".format(meta_path))
        print_log("Viral DB file path: {}".format(vdb_path))
        print_log("Output path: {}".format(out_file))

        # Import metadata and viral DB
        print_log("Importing Genbank metadata file...")
        meta_db = pd.read_csv(meta_path, sep="\t", dtype=str)
        genbank_metadata = {
            genome_id: [taxid, species_taxid]
            for genome_id, taxid, species_taxid in zip(
                meta_db["genome_id"], meta_db["taxid"], meta_db["species_taxid"]
            )
        }
        print_log("Importing viral DB file...")
        virus_db = pd.read_csv(vdb_path, sep="\t", dtype=str)
        viral_taxids = set(virus_db["taxid"].values)
        print_log(f"Virus DB imported. {len(virus_db)} total viral taxids.")

        print_log(f"Imported {len(viral_taxids)} virus taxa.")

        # Process SAM with streaming FASTQ merge join
        print_log("Processing SAM file...")
        process_sam(sam_file, out_file, genbank_metadata, viral_taxids, clean_reads)
        print_log("File processed.")

        # Finish time tracking
        end_time = time.time()
        print_log(f"Total time elapsed: {end_time - start_time:.2f} seconds")

    except Exception as e:
        print_log(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
