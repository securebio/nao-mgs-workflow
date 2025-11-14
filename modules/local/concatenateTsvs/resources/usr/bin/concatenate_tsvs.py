#!/usr/bin/env python

# Import modules
import argparse
import time
import datetime
import gzip
import bz2
import os
from typing import List

def print_log(message: str):
    pass
    # print("[", datetime.datetime.now(), "]  ", message, sep="")

def open_by_suffix(filename: str, mode="r", debug=False):
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

def read_header(infile) -> List[str]:
    """Read header from TSV file and return as list.
    Returns empty list if file is empty."""
    header_line = infile.readline()
    if not header_line or not header_line.strip():
        return []
    header = header_line.strip().split("\t")
    return header

def map_headers(header: List[str], reference_header: List[str]) -> List[int]:
    """Generate mapping of columns in header to reference header."""
    header_mapping = {col: i for i, col in enumerate(header)}
    reference_mapping = [header_mapping[col] for col in reference_header]
    return reference_mapping

def check_headers(header: List[str], reference_header: List[str]) -> bool:
    """Check if fields in two headers match and raise error if not."""
    hset = set(header)
    rset = set(reference_header)
    header_difference = hset ^ rset
    if not header_difference:
        return True
    msg = f"Headers do not match:"
    if rset - hset:
        msg += f"\n\tMissing fields: {rset - hset}"
    if hset - rset:
        msg += f"\n\tExtra fields: {hset - rset}"
    raise ValueError(msg)

def concatenate_tsvs(input_files: List[str], out_path: str):
    """Concatenate multiple TSV files with matching headers."""
    with open_by_suffix(out_path, "w") as outf:
        # Find the first non-empty file to extract headers
        reference_header = []
        first_valid_index = -1
        
        for idx, input_path in enumerate(input_files):
            try:
                with open_by_suffix(input_path) as inf:
                    # Extract header information
                    header = read_header(inf)
                    if not header:
                        print_log(f"Warning: File is empty, skipping: {input_path}")
                        continue
                    
                    reference_header = header
                    first_valid_index = idx
                    print_log(f"\tFound first non-empty file: {input_path}")
                    
                    # Write header to output file
                    outf.write("\t".join(reference_header) + "\n")
                    
                    # Write contents of first valid file to output unchanged
                    for line in inf:
                        outf.write(line)
                    
                    break
            except Exception as e:
                print_log(f"Error processing file {input_path}: {str(e)}")
                continue
        
        # If no valid files were found, create an empty output with no header
        if not reference_header:
            print_log("Warning: All input files are empty. Creating empty output file.")
            return
        
        # Parse remaining files (after the first valid file) and write to output
        print_log(f"\tProcessing other files:")
        for input_path in input_files[first_valid_index + 1:]:
            try:
                with open_by_suffix(input_path) as inf:
                    # Extract header information
                    header = read_header(inf)
                    if not header:
                        print_log(f"Warning: File is empty, skipping: {input_path}")
                        continue
                    
                    # Verify header fields match reference header
                    check_headers(header, reference_header)
                    
                    # Generate mapping of header fields to reference header
                    header_mapping = map_headers(header, reference_header)
                    print_log(f"\t\tProcessing file: {input_path}")
                    
                    # Write contents of file to output with mapped fields
                    for line in inf:
                        fields = line.strip().split("\t")
                        mapped_fields = [fields[i] for i in header_mapping]
                        outf.write("\t".join(mapped_fields) + "\n")
            except Exception as e:
                print_log(f"Error processing file {input_path}: {str(e)}")
                raise

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Concatenate multiple TSV files with matching headers.")
    parser.add_argument("input_files", nargs="+", help="Paths to input TSV files.",
                        metavar="INPUT", type=str)
    parser.add_argument("-o", "--output_file", help="Path to output TSV.",
                        type=str, required=True)
    args = parser.parse_args()
    input_files = args.input_files
    out_path = args.output_file
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Output TSV file: {}".format(out_path))
    print_log("Input TSV files:")
    for input_path in input_files:
        print_log(f"\t{input_path}")
    # Concatenate input files
    print_log("Concatenating TSV files...")
    concatenate_tsvs(input_files, out_path)
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
