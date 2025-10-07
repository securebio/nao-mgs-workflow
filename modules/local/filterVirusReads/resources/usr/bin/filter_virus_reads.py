#!/usr/bin/env python

# Import modules
import argparse
import time
import datetime
import gzip
import bz2
import os

def print_log(message):
    print("[", datetime.datetime.now(), "]  ", message, sep="")

def open_by_suffix(filename, mode="r", debug=False):
    if debug:
        print_log(f"\tOpening file object: {filename}")
        print_log(f"\tOpening mode: {mode}")
        print_log(f"\tGZIP mode: {filename.endswith('.gz')}")
        print_log(f"\tBZ2 mode: {filename.endswith('.bz2')}")
    if filename.endswith('.gz'):
        return gzip.open(filename, mode + 't')
    elif filename.endswith('.bz2'):
        return bz2.BZ2file(filename, mode)
    else:
        return open(filename, mode)

def filter_virus_reads(input_path, score_threshold, out_path):
    """Filter putative virus reads based on Kraken and Bowtie2 results."""
    with open_by_suffix(input_path) as inf, open_by_suffix(out_path, "w") as outf:
        # Read and handle header line
        header_line = inf.readline().strip()
        if not header_line:
            print_log("Input file is empty or contains only whitespace. Creating empty output file.")
            outf.write("")
            return
        headers = header_line.split("\t")
        idx = {h: i for i, h in enumerate(headers)}
        # Check for necessary columns in header
        if "seq_id" not in headers:
            raise ValueError("Missing column in input TSV: 'seq_id'")
        if "kraken_classified" not in headers:
            raise ValueError("Missing column in input TSV: 'kraken_classified'")
        if "kraken_assigned_host_virus" not in headers:
            raise ValueError("Missing column in input TSV: 'kraken_assigned_host_virus'")
        if "bowtie2_length_normalized_score_max" not in headers:
            raise ValueError("Missing column in input TSV: 'bowtie2_length_normalized_score_max'")
        outf.write("\t".join(headers) + "\n")
        # Read in lines and filter based on Kraken assignment and score
        for line in inf:
            fields = line.strip().split("\t")
            kraken_classified = fields[idx["kraken_classified"]].lower() == "true"
            kraken_assigned_host_virus = int(fields[idx["kraken_assigned_host_virus"]]) # 4-state: 0, 1, 2, 3
            if (not kraken_classified) and kraken_assigned_host_virus > 0:
                raise ValueError("Inconsistent Kraken fields: 'kraken_classified' is False, but 'kraken_assigned_host_virus' is not 0: {}".format(fields[idx["seq_id"]]))
            adj_score = float(fields[idx["bowtie2_length_normalized_score_max"]])
            # Only write reads that:
            # 1. Are classified by Kraken as host-infecting viruses; or
            # 2. Are classified by Kraken as potentially host-infecting viruses, and have a normalized Bowtie2 score above the threshold
            # 3. Are unclassified by Kraken, but have a normalized Bowtie2 score above the threshold; or
            if kraken_assigned_host_virus == 1:
                outf.write("\t".join(fields) + "\n")
            elif adj_score >= score_threshold and kraken_assigned_host_virus > 1:
                outf.write("\t".join(fields) + "\n")
            elif adj_score >= score_threshold and not kraken_classified:
                outf.write("\t".join(fields) + "\n")
            else:
                pass

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Filter putative virus reads based on Kraken and Bowtie2 results.")
    parser.add_argument("input_path", help="Path to input TSV file.")
    parser.add_argument("score_threshold", help="Length-normalized alignment score threshold for filtering.")
    parser.add_argument("output_file", help="Path to output TSV.")
    args = parser.parse_args()
    input_path = args.input_path
    score_threshold = float(args.score_threshold)
    out_path = args.output_file
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input TSV file: {}".format(input_path))
    print_log("Score threshold: {}".format(score_threshold))
    print_log("Output TSV file: {}".format(out_path))
    # Run labeling function
    print_log("Filtering putative virus reads...")
    filter_virus_reads(input_path, score_threshold, out_path)
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
