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

def read_line(file):
    """Read and process a line from a file."""
    line = file.readline()
    return None if not line else line.rstrip("\n").split("\t")

def write_line(file, fields):
    """Write a line to a file."""
    file.write("\t".join(fields) + "\n")

def initialize_output_file(input_path, file_index, headers):
    """Initialize an output file for partitioned data."""
    outf_path = f"partition_{file_index}_{input_path}"
    outf = open_by_suffix(outf_path, "w")
    write_line(outf, headers)
    return outf

def partition(input_path, column):
    """Partition a TSV file based on a specified column header."""
    with open_by_suffix(input_path) as inf:
        # Handle header line
        headers = read_line(inf)
        if not headers:
            raise ValueError("Input file is empty.")
        if column not in headers:
            raise ValueError(f"Required column is missing from header line: {column}")
        column_index = headers.index(column)
        # Read first line of data and initialize first output file
        fields = read_line(inf)
        if fields is None: # Empty apart from headers
            print_log("Input file has no data rows, skipping partition.")
            return
        index = fields[column_index]
        file_index = index
        outf = initialize_output_file(input_path, file_index, headers)
        # Iterate over data and partition
        try:
            while fields is not None:
                index = fields[column_index]
                if index < file_index: # Throw sorting error
                    msg = f"Input file is not sorted by partition column {column}; found {index} after {file_index}."
                    raise ValueError(msg)
                if index != file_index:
                    outf.close()
                    file_index = index
                    outf = initialize_output_file(input_path, file_index, headers)
                write_line(outf, fields)
                fields = read_line(inf)
        finally:
            outf.close()

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Partition a TSV based on a specified column header.")
    parser.add_argument("--input_path", "-i", help="Path to input TSV file.")
    parser.add_argument("--column", "-c", help="Column header to partition on.")
    args = parser.parse_args()
    input_path = args.input_path
    column = args.column
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input TSV file: {}".format(input_path))
    print_log("Partition column header: {}".format(column))
    # Run labeling function
    print_log("Partitioning TSV file...")
    partition(input_path, column)
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
