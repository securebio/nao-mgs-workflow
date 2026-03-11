#!/usr/bin/env python

# Import modules
import argparse
import time
import datetime
import gzip
import os
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

def write_line(line_list: list[str], output_file: IO[str]) -> None:
    """Write a line to the output file."""
    output_file.write("\t".join(line_list) + "\n")

def filter_blast(input_path: str, output_path: str, max_rank: int, min_frac: float, query_index: int, bitscore_index: int) -> None:
    """Filter BLAST input based on query ID and bitscore."""
    max_field = max(query_index, bitscore_index)
    # Open files
    with open_by_suffix(input_path, "r") as inf, open_by_suffix(output_path, "w") as outf:
        # Write header line
        header_fields = ["qseqid", "sseqid", "sgi", "staxid", "qlen", "evalue",
                         "bitscore", "qcovs", "length", "pident", "mismatch",
                         "gapopen", "sstrand", "qstart", "qend", "sstart", "send",
                         "bitscore_rank_dense", "bitscore_fraction"]
        write_line(header_fields, outf)
        # Read and process lines
        line = inf.readline()
        query_last: str | None = None
        bitscore_rank: int = 0
        bitscore_max: float = 0.0
        bitscore_frac: float = 0.0
        while line:
            # Split line into fields
            fields = line.rstrip("\n").split("\t")
            # Verify that the line has sufficient fields
            if len(fields) <= max_field:
                msg = f"Line does not have sufficient fields: expected {max_field + 1}, got {len(fields)}"
                raise ValueError(msg)
            # Get query ID and bitscore
            query_new = fields[query_index]
            bitscore_new = float(fields[bitscore_index])
            # Verify sorting
            if query_last is not None and query_new < query_last:
                msg = f"Input file is not sorted by query ID: encountered {query_new} after {query_last}"
                raise ValueError(msg)
            if query_new == query_last and bitscore_new > bitscore_max:
                msg = f"Input file is not sorted by bitscore (in descending order): encountered {bitscore_new} after {bitscore_max} for query {query_new}"
                raise ValueError(msg)
            # Calculate bitscore rank and fraction
            if (not query_last) or (query_new != query_last):
                # New query: reset rank and max bitscore
                bitscore_rank = 1
                bitscore_frac = 1.0
                bitscore_max = bitscore_new
                query_last = query_new
            else:
                # Existing query: compare bitscore and filter on rank & frac
                bitscore_frac_new = bitscore_new / bitscore_max
                if bitscore_frac_new < min_frac:
                    line = inf.readline()
                    continue
                if bitscore_frac_new < bitscore_frac:
                    bitscore_rank += 1
                    bitscore_frac = bitscore_frac_new
                if bitscore_rank > max_rank:
                    line = inf.readline()
                    continue
            # Write line & continue to next
            out_fields = fields + [str(bitscore_rank), str(bitscore_frac)]
            write_line(out_fields, outf)
            line = inf.readline()

def main() -> None:
    # Parse arguments
    parser = argparse.ArgumentParser(description="Filter sorted BLAST output by query ID (ascending) and bitscore (descending).")
    parser.add_argument("--input", "-i", type=str, help="Path to input TSV file containing sorted BLAST output.")
    parser.add_argument("--output", "-o", type=str, help="Path to output TSV file.")
    parser.add_argument("--max_rank", "-r", type=int, default=1, help="Maximum rank of hits to keep for each query.")
    parser.add_argument("--min_frac", "-f", type=float, default=0.9, help="Minimum fraction of maximum bitscore to keep for each query.")
    parser.add_argument("--query_index", "-q", type=int, default=0, help="Index of the query ID column (0-based).")
    parser.add_argument("--bitscore_index", "-b", type=int, default=6, help="Index of the bitscore column (0-based).")
    args = parser.parse_args()
    # Assign variables
    input_path = args.input
    output_path = args.output
    max_rank = args.max_rank
    min_frac = args.min_frac
    query_index = args.query_index
    bitscore_index = args.bitscore_index
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input file: {}".format(input_path))
    print_log("Output file: {}".format(output_path))
    print_log("Maximum rank: {}".format(max_rank))
    print_log("Minimum fraction: {}".format(min_frac))
    print_log("Query index: {}".format(query_index))
    print_log("Bitscore index: {}".format(bitscore_index))
    # Run filtering function
    filter_blast(input_path, output_path, max_rank, min_frac, query_index, bitscore_index)
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
