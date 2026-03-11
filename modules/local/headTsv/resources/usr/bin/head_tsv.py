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

def add_header_line(input_path: str, header_fields: list[str], out_path: str) -> None:
    """Add header line to TSV file."""
    with open_by_suffix(input_path) as inf, open_by_suffix(out_path, "w") as outf:
        # Read first line
        first_line_content = inf.readline().strip()
        
        # Check if file is empty
        if not first_line_content:
            print_log(f"Warning: Input file {input_path} is empty. Creating output with header only.")
            # Write header line to output
            header_line = "\t".join(header_fields)
            outf.write(header_line + "\n")
            return
            
        # Parse first line
        first_line = first_line_content.split("\t")
        
        # Check number of fields in input
        if len(first_line) != len(header_fields):
            print_log("Number of header fields: {}".format(len(header_fields)))
            print_log("Number of fields in input file: {}".format(len(first_line)))
            raise ValueError("Number of header fields does not match number of fields in input file.")
            
        # Write header line to output
        header_line = "\t".join(header_fields)
        outf.write(header_line + "\n")
        
        # Write first line of input file to output
        outf.write("\t".join(first_line) + "\n")
        
        # Write entire remainder of input file to output
        for line in inf:
            outf.write(line)

def main() -> None:
    # Parse arguments
    parser = argparse.ArgumentParser(description="Add a header line to a TSV file.")
    parser.add_argument("input_path", help="Path to input TSV file.")
    parser.add_argument("header_fields", help="Comma-separated list of header field names.")
    parser.add_argument("output_file", help="Path to output TSV.")
    args = parser.parse_args()
    input_path = args.input_path
    header_fields = args.header_fields.split(",")
    out_path = args.output_file
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input TSV file: {}".format(input_path))
    print_log("Header fields: {}".format(header_fields))
    print_log("Output TSV file: {}".format(out_path))
    # Run labeling function
    print_log("Adding header line to TSV file...")
    add_header_line(input_path, header_fields, out_path)
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
