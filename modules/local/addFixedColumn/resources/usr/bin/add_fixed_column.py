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

def add_column(input_path: str, column_name: str, column_value: str, out_path: str) -> None:
    """Add one or more columns to a TSV file with a fixed value.

    The column_name argument can be a single name or a comma-separated list
    of names. Each new column is appended and filled with column_value.
    """
    column_names = [c.strip() for c in column_name.split(",")]
    with open_by_suffix(input_path) as inf, open_by_suffix(out_path, "w") as outf:
        # Read and handle header line
        header_line = inf.readline().rstrip("\r\n")
        # Handle empty file
        if not header_line:
            return
        headers_in = header_line.split("\t")
        for name in column_names:
            if name in headers_in:
                raise ValueError(f"Column already exists: {name}")
        headers_out = headers_in + column_names
        header_line = "\t".join(headers_out)
        outf.write(header_line + "\n")
        # Add columns to each subsequent line and write to output
        suffix = "\t" + "\t".join([column_value] * len(column_names))
        for line in inf:
            if not line.strip():
                continue
            outf.write(line.rstrip("\r\n") + suffix + "\n")

def main() -> None:
    # Parse arguments
    parser = argparse.ArgumentParser(description="Add column to TSV file with specified name and value.")
    parser.add_argument("input_path", help="Path to input TSV file.")
    parser.add_argument("column_name", help="Name of the column to add (comma-separated for multiple).")
    parser.add_argument("column_value", help="Value for the new column.")
    parser.add_argument("output_file", help="Path to output TSV.")
    args = parser.parse_args()
    input_path = args.input_path
    column_name = args.column_name
    column_value = args.column_value
    out_path = args.output_file
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input TSV file: {}".format(input_path))
    print_log("Column name: {}".format(column_name))
    print_log("Column value: {}".format(column_value))
    print_log("Output TSV file: {}".format(out_path))
    # Run labeling function
    print_log("Adding column to TSV...")
    add_column(input_path, column_name, column_value, out_path)
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
