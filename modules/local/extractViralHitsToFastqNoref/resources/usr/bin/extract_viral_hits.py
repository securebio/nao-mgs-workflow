#!/usr/bin/env python

# Import modules
import argparse
import time
import datetime
import gzip
from typing import IO, cast

def print_log(message: str) -> None:
    print("[", datetime.datetime.now(), "]  ", message, sep="")

def open_by_suffix(filename: str, mode: str = "r", debug: bool = False) -> IO[str]:
    if debug:
        print_log(f"\tOpening file object: {filename}")
        print_log(f"\tOpening mode: {mode}")
        print_log(f"\tGZIP mode: {filename.endswith('.gz')}")
    if filename.endswith('.gz'):
        return cast(IO[str], gzip.open(filename, mode + 't'))
    else:
        return open(filename, mode)

def extract_viral_hit(fields: list[str], indices: dict[str, int], single: bool, drop_unpaired: bool) -> str | None:
    """Convert a single TSV line to a FASTQ entry, handling missing mates (if not single-end)."""
    # Extract fields
    seq_id = fields[indices["seq_id"]]
    query_seq_fwd = fields[indices["query_seq"]]
    query_qual_fwd = fields[indices["query_qual"]]        
    if not single:
        query_seq_rev = fields[indices["query_seq_rev"]]
        query_qual_rev = fields[indices["query_qual_rev"]]
        # Check for unpaired reads
        if query_seq_fwd == "NA":
            if drop_unpaired:
                return None
            query_seq_fwd = "N"
            query_qual_fwd = "!"
        if query_seq_rev == "NA":
            if drop_unpaired:
                return None
            query_seq_rev = "N"
            query_qual_rev = "!"
    # Assemble FASTQ entry
    fastq_entry_fwd = f"@{seq_id} 1\n{query_seq_fwd}\n+\n{query_qual_fwd}\n"
    if single:
        return fastq_entry_fwd
    else:
        fastq_entry_rev = f"@{seq_id} 2\n{query_seq_rev}\n+\n{query_qual_rev}\n"
        return fastq_entry_fwd + fastq_entry_rev
        

def extract_viral_hits(input_path: str, out_path: str, drop_unpaired: bool) -> None:
    """Extract viral sequences from TSV file and write to FASTQ file."""
    with open_by_suffix(input_path) as inf, open_by_suffix(out_path, "w") as outf:
        # Read and handle header line
        headers = inf.readline().rstrip("\n").split("\t")
        # Infer whether paired or single end from header
        if "query_seq_rev" in headers:
            single = False
            print_log("Processing paired-end reads.")
        else:
            single = True
            print_log("Processing single-end reads.")
        headers_exp = ["seq_id", "query_seq", "query_qual"]
        if not single:
            headers_exp += ["query_seq_rev", "query_qual_rev"]
        for header in headers_exp:
            if header not in headers:
                msg = f"Missing column in input TSV: {header}"
                raise ValueError(msg)
        # Get indices of required columns
        indices = {x: headers.index(x) for x in headers_exp}
        # Iterate over lines in input file
        for line in inf:
            fields = line.rstrip("\n").split("\t")
            fastq_entry = extract_viral_hit(fields, indices, single, drop_unpaired)
            if fastq_entry:
                outf.write(fastq_entry)

def main() -> None:
    # Parse arguments
    parser = argparse.ArgumentParser(description="Extract viral hits from a TSV to FASTQ file.")
    parser.add_argument("--input", "-i", required=True, help="Path to input TSV file.")
    parser.add_argument("--output", "-o", required=True, help="Path to output FASTQ file.")
    parser.add_argument("--drop_unpaired", "-d", default=False, action="store_true", help="Drop unpaired reads. (Default: False)")
    args = parser.parse_args()
    input_path = args.input
    out_path = args.output
    drop_unpaired = args.drop_unpaired
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input TSV file: {}".format(input_path))
    print_log("Output FASTQ file: {}".format(out_path))
    print_log("Drop unpaired reads: {}".format(drop_unpaired))
    # Run labeling function
    print_log("Extracting viral hits...")
    extract_viral_hits(input_path, out_path, drop_unpaired)
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
