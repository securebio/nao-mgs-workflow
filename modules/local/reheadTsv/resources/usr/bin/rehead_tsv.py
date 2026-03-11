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

def rename_columns(input_path: str, input_fields: list[str], output_fields: list[str], out_path: str) -> None:
    """Rename columns in TSV file."""
    if len(input_fields) != len(output_fields):
        raise ValueError("Input and output field lists must be the same length.")
    
    with open_by_suffix(input_path) as inf, open_by_suffix(out_path, "w") as outf:
        # Read the header line
        header_line = inf.readline().strip()
        
        # Check if file is empty
        if not header_line:
            print_log("Warning: Input file is empty. Creating empty output file.")
            outf.write("")  # Write empty string to create the file
            return
        
        # Process header fields
        headers_in = header_line.split("\t")
        
        # Verify all input fields exist in the header
        for field in input_fields:
            if field not in headers_in:
                raise ValueError(f"Input field not found in file header: {field}")
        
        # Rename header fields
        headers_out = headers_in.copy()
        for i in range(len(input_fields)):
            headers_out[headers_in.index(input_fields[i])] = output_fields[i]
        
        # Write new header
        new_header_line = "\t".join(headers_out)
        outf.write(new_header_line + "\n")
        
        # Write entire remainder of input file to output
        for line in inf:
            outf.write(line)

def main() -> None:
    # Parse arguments
    parser = argparse.ArgumentParser(description="Rename one or more columns in a TSV file.")
    parser.add_argument("input_path", help="Path to input TSV file.")
    parser.add_argument("input_fields", help="Comma-separated list of input field names.")
    parser.add_argument("output_fields", help="Comma-separated list of output field names.")
    parser.add_argument("output_file", help="Path to output TSV.")
    args = parser.parse_args()
    input_path = args.input_path
    input_fields = args.input_fields.split(",")
    output_fields = args.output_fields.split(",")
    out_path = args.output_file
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input TSV file: {}".format(input_path))
    print_log("Input fields: {}".format(input_fields))
    print_log("Output fields: {}".format(output_fields))
    print_log("Output TSV file: {}".format(out_path))
    # Run labeling function
    print_log("Renaming columns in TSV...")
    rename_columns(input_path, input_fields, output_fields, out_path)
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
