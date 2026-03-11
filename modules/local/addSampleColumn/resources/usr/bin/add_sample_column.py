#!/usr/bin/env python

# Import modules
import argparse
import time
import datetime
import gzip
import bz2
import os
from typing import IO

def print_log(message: str) -> None:
    print("[", datetime.datetime.now(), "]  ", message, sep="")

def open_by_suffix(filename: str, mode: str = "r", debug: bool = False) -> IO[str]:
    if debug:
        print_log(f"\tOpening file object: {filename}")
        print_log(f"\tOpening mode: {mode}")
        print_log(f"\tGZIP mode: {filename.endswith('.gz')}")
        print_log(f"\tBZ2 mode: {filename.endswith('.bz2')}")
    if filename.endswith('.gz'):
        return gzip.open(filename, mode + 't')  # type: ignore[return-value]
    elif filename.endswith('.bz2'):
        return bz2.BZ2File(filename, mode)  # type: ignore[call-overload,return-value,no-any-return]
    else:
        return open(filename, mode)

def add_sample_column(input_path: str, sample_name: str, sample_column: str, out_path: str) -> None:
    """Add sample name column to TSV file."""
    with open_by_suffix(input_path) as inf, open_by_suffix(out_path, "w") as outf:
        # Read header line
        header_line = inf.readline().strip()
        
        # Check if file is empty
        if not header_line:
            print_log(f"Warning: Input file {input_path} is empty. Creating empty output file.")
            outf.write("")  # Write empty string to create the file
            return
            
        # Process header
        headers_in = header_line.split("\t")
        if sample_column in headers_in:
            raise ValueError(f"Sample name column already exists: {sample_column}")
            
        # Add sample column to header
        headers_out = headers_in + [sample_column]
        new_header = "\t".join(headers_out)
        outf.write(new_header + "\n")
        
        # Add sample name to each subsequent line and write to output
        for line in inf:
            if line.strip():  # Skip completely empty lines
                outf.write(line.strip() + "\t" + sample_name + "\n")

def main() -> None:
    # Parse arguments
    parser = argparse.ArgumentParser(description="Add sample name column to TSV file.")
    parser.add_argument("input_path", help="Path to input TSV file.")
    parser.add_argument("sample_name", help="Name of the sample.")
    parser.add_argument("sample_column", help="Name of the sample name column.")
    parser.add_argument("output_file", help="Path to output TSV.")
    args = parser.parse_args()
    input_path = args.input_path
    sample_name = args.sample_name
    sample_column = args.sample_column
    out_path = args.output_file
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input TSV file: {}".format(input_path))
    print_log("Sample name: {}".format(sample_name))
    print_log("Sample name column: {}".format(sample_column))
    print_log("Output TSV file: {}".format(out_path))
    # Run labeling function
    print_log("Adding sample name column to TSV...")
    add_sample_column(input_path, sample_name, sample_column, out_path)
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
