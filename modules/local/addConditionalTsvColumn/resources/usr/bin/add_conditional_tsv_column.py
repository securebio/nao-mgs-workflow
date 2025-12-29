#!/usr/bin/env python

import argparse
import csv
import time
import datetime
import gzip
import bz2

def print_log(message):
    print("[", datetime.datetime.now(), "]  ", message, sep="")

def open_by_suffix(filename, mode="r", debug=False):
    """Open file with automatic decompression based on file extension."""
    if debug:
        print_log(f"\tOpening file object: {filename}")
        print_log(f"\tOpening mode: {mode}")
        print_log(f"\tGZIP mode: {filename.endswith('.gz')}")
        print_log(f"\tBZ2 mode: {filename.endswith('.bz2')}")
    if filename.endswith('.gz'):
        return gzip.open(filename, mode + 't', encoding='utf-8')
    elif filename.endswith('.bz2'):
        return bz2.open(filename, mode + 't', encoding='utf-8')
    else:
        return open(filename, mode, encoding='utf-8')

def validate_columns(fieldnames, chk_col, if_col, else_col):
    """Validate that all required columns exist in the header."""
    missing_cols = []
    if chk_col not in fieldnames:
        missing_cols.append(f"check column '{chk_col}'")
    if if_col not in fieldnames:
        missing_cols.append(f"if column '{if_col}'")
    if else_col not in fieldnames:
        missing_cols.append(f"else column '{else_col}'")

    if missing_cols:
        raise ValueError(
            f"could not find all requested columns in header\n"
            f" Missing: {', '.join(missing_cols)}\n"
            f" Available columns: {', '.join(fieldnames)}"
        )

def process_rows(reader, chk_col, match_val, if_col, else_col, new_hdr):
    """Generator that processes rows and adds conditional column value."""
    for row in reader:
        # Select value based on condition
        row[new_hdr] = row[if_col] if row[chk_col] == match_val else row[else_col]
        yield row

def add_conditional_column(input_path, chk_col, match_val, if_col, else_col, new_hdr, out_path):
    """Add conditional column to TSV file based on check column value."""
    with open_by_suffix(input_path) as inf, open_by_suffix(out_path, "w") as outf:
        # Use DictReader for cleaner column access by name
        reader = csv.DictReader(inf, delimiter='\t')

        # Handle empty file
        if reader.fieldnames is None:
            return

        # Validate required columns exist
        validate_columns(reader.fieldnames, chk_col, if_col, else_col)

        # Write header with new column
        fieldnames_out = list(reader.fieldnames) + [new_hdr]
        writer = csv.DictWriter(outf, fieldnames=fieldnames_out, delimiter='\t', lineterminator='\n')
        writer.writeheader()

        # Process and write data rows using generator for memory efficiency
        writer.writerows(process_rows(reader, chk_col, match_val, if_col, else_col, new_hdr))

def main():
    # Parse arguments using named parameters
    parser = argparse.ArgumentParser(
        description="Add a conditional column to TSV file based on check column value."
    )
    parser.add_argument("--input", required=True, help="Path to input TSV file.")
    parser.add_argument("--chk-col", required=True, help="Name of column to check.")
    parser.add_argument("--match-val", required=True, help="Value to match in check column.")
    parser.add_argument("--if-col", required=True, help="Column to use when check matches.")
    parser.add_argument("--else-col", required=True, help="Column to use when check doesn't match.")
    parser.add_argument("--new-hdr", required=True, help="Name of the new column to add.")
    parser.add_argument("--output", required=True, help="Path to output TSV.")
    args = parser.parse_args()

    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()

    # Print parameters
    print_log(f"Input TSV file: {args.input}")
    print_log(f"Check column: {args.chk_col}")
    print_log(f"Match value: {args.match_val}")
    print_log(f"If column: {args.if_col}")
    print_log(f"Else column: {args.else_col}")
    print_log(f"New header: {args.new_hdr}")
    print_log(f"Output TSV file: {args.output}")

    # Run conditional column function
    print_log("Adding conditional column to TSV...")
    add_conditional_column(
        args.input, args.chk_col, args.match_val,
        args.if_col, args.else_col, args.new_hdr, args.output
    )
    print_log("...done.")

    # Finish time tracking
    end_time = time.time()
    print_log(f"Total time elapsed: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
