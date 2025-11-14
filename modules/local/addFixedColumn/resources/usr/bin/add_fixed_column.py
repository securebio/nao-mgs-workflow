#!/usr/bin/env python

# Import modules
import argparse
import time
import datetime
import gzip
import bz2

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
        return bz2.BZ2File(filename, mode)
    else:
        return open(filename, mode)

def add_column(input_path, column_name, column_value, out_path):
    """Add column to TSV file with specified name and value."""
    with open_by_suffix(input_path) as inf, open_by_suffix(out_path, "w") as outf:
        # Read and handle header line
        header_line = inf.readline().strip()
        # Handle empty file
        if not header_line:
            return
        headers_in = header_line.split("\t")
        if column_name in headers_in:
            raise ValueError(f"Column already exists: {column_name}")
        headers_out = headers_in + [column_name]
        header_line = "\t".join(headers_out)
        outf.write(header_line + "\n")
        # Add column to each subsequent line and write to output
        for line in inf:
            line = line.strip()
            if line:  # Skip empty lines
                outf.write(line + "\t" + column_value + "\n")

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Add column to TSV file with specified name and value.")
    parser.add_argument("input_path", help="Path to input TSV file.")
    parser.add_argument("column_name", help="Name of the column to add.")
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
