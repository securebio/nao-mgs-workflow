#!/usr/bin/env python

"""
Given two sorted TSV files, join them linewise on a shared column.
Can perform inner, left, right, outer, or strict joins; the latter will
raise an error if there are any missing values in the join field on either
side.
"""

#=======================================================================
# Import libraries
#=======================================================================

# Import modules
import argparse
import time
import gzip
import bz2
import os
import logging
from datetime import datetime, timezone
from typing import IO, cast

#=======================================================================
# Configure logging
#=======================================================================

class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter('[%(asctime)s] %(message)s')
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#=======================================================================
# I/O functions
#=======================================================================

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    # Create parser
    desc = "Given two sorted TSV files, join them linewise on a shared column."
    parser = argparse.ArgumentParser(description=desc)
    # Add arguments
    parser.add_argument("tsv1", help="Path to first TSV file.")
    parser.add_argument("tsv2", help="Path to second TSV file.")
    parser.add_argument("field", type=str, help="Field to join on.")
    parser.add_argument("join_type",
                        type=str,
                        choices=("inner", "left", "right", "outer", "strict"),
                        help="Type of join to perform (inner, left, right, outer, strict).")
    parser.add_argument("output_file", type=str, help="Path to output TSV file.")
    # Return parsed arguments
    return parser.parse_args()

def open_by_suffix(filename: str, mode: str = "r") -> IO[str]:
    """Parse the suffix of a filename to determine the right open method
    to use, then open the file. Can handle .gz, .bz2, and uncompressed files."""
    logger.debug(f"Opening file object: {filename}")
    logger.debug(f"Opening mode: {mode}")
    logger.debug(f"GZIP mode: {filename.endswith('.gz')}")
    logger.debug(f"BZ2 mode: {filename.endswith('.bz2')}")
    if filename.endswith('.gz'):
        return cast(IO[str], gzip.open(filename, mode + 't'))
    elif filename.endswith('.bz2'):
        return cast(IO[str], bz2.BZ2File(filename, mode))  # type: ignore[call-overload]
    else:
        return open(filename, mode)

#=======================================================================
# Auxiliary functions
#=======================================================================

def write_line(line_list: list[str], output_file: IO[str]) -> None:
    """Write a line to the output file."""
    line_joined = "\t".join(line_list)
    logger.debug(f"Writing line to output: {line_joined}")
    output_file.write(line_joined + "\n")

def fill_right(row_1: list[str], placeholder_2: list[str]) -> list[str]:
    """Fill in fields from file 2 with placeholder values."""
    new_row = row_1 + placeholder_2
    logger.debug(f"Filling right: {new_row}")
    return new_row

def fill_left(placeholder_1: list[str], row_2: list[str], id_2: str, field_index_1: int, field_index_2: int) -> list[str]:
    """Fill in fields from file 1 with placeholder values."""
    # Put join field in appropriate position in columns from file 1
    if field_index_1 == 0:
        merged_row = [id_2] + placeholder_1
    else:
        merged_row = placeholder_1[:field_index_1] + [id_2] + placeholder_1[field_index_1:]
    # Add remainder of row_2 excluding join field
    merged_row += [val for i, val in enumerate(row_2) if i != field_index_2]
    logger.debug(f"Filling left: {merged_row}")
    return merged_row

def process_headers(header_1: list[str], header_2: list[str], field: str) -> tuple[list[str], int, int]:
    """Read, join and check headers from both files."""
    # Log header fields
    logger.debug(f"Header 1: {header_1}")
    logger.debug(f"Header 2: {header_2}")
    # Verify that the field exists in both files
    if field not in header_1:
        msg = f"Join field missing from file 1 ('{field}')."
        logger.error(msg)
        raise ValueError(msg)
    if field not in header_2:
        msg = f"Join field missing from file 2 ('{field}')."
        logger.error(msg)
        raise ValueError(msg)
    # Check for duplicate field names across files
    for h in header_1:
        if h != field and h in header_2:
            msg = f"Duplicate non-join field name found across both files: '{h}'."
            logger.error(msg)
            raise ValueError(msg)
    for h in header_2:
        if h != field and h in header_1:
            msg = f"Duplicate non-join field name found across both files: '{h}'."
            logger.error(msg)
            raise ValueError(msg)
    # Get index of join field
    field_index_1 = header_1.index(field)
    field_index_2 = header_2.index(field)
    logger.debug(f"Field index 1: {field_index_1}")
    logger.debug(f"Field index 2: {field_index_2}")
    # Prepare new header
    merged_header = header_1 + \
            [h for i, h in enumerate(header_2) if i != field_index_2]
    logger.debug(f"Merged header: {merged_header}")
    return merged_header, field_index_1, field_index_2

def get_line_id(file: IO[str], field_index: int) -> tuple[str, list[str] | None, str | None]:
    """Get the next line ID from a file."""
    line = file.readline()
    row = line.rstrip("\n").split("\t") if line else None
    id = row[field_index] if row else None
    return line, row, id

def check_sorting(id_curr: str | None, id_next: str | None, file_str: str, input_path: str) -> None:
    """Check that the file is sorted and raise an error if not."""
    logger.debug(f"Checking sorting for file {file_str}: {id_curr}, {id_next}")
    if id_next is not None and id_curr is not None and id_curr > id_next:
        msg = f"File {file_str} is not sorted: encountered ID {id_curr} after {id_next} ({input_path})."
        logger.error(msg)
        raise ValueError(msg)

def copy_file(file: IO[str], header: list[str], output: IO[str]) -> None:
    """Copy a file to the output file, starting with header."""
    write_line(header, output)
    for line in file:
        output.write(line)

def handle_empty_files(file_1: IO[str], file_2: IO[str], is_empty_1: bool, is_empty_2: bool, join_type: str, header_1: str, header_2: str, output_file: IO[str]) -> None:
    """Handle joining when one or both files are empty."""
    if not is_empty_1 and not is_empty_2:
        msg = "Error: called handle_empty_files with non-empty files"
        logger.error(msg)
        raise ValueError(msg)
    # Both files are empty - create empty output file (for all join types)
    if is_empty_1 and is_empty_2:
        logger.warning("Both input files are empty. Creating empty output file.")
        return
    empty_file = file_1 if is_empty_1 else file_2 # Otherwise, get non-empty file for logging
    # For strict joins, raise error if any single file is empty
    if join_type == "strict":
        msg = f"Strict join cannot be performed with empty file: {empty_file}"
        logger.error(msg)
        raise ValueError(msg)
    # For other join types, act based on which file is empty
    empty_file_log = "left" if is_empty_1 else "right"
    nonempty_file_log = "right" if is_empty_1 else "left"
    match (join_type, is_empty_1):
        case ("inner", True) | ("inner", False) | ("left", True) | ("right", False):
            logger.warning(f"{join_type} join with empty {empty_file_log} file. Creating empty output.")
            return
        case ("right", True) | ("outer", True):
            logger.warning(f"{join_type} join with empty {empty_file_log} file. Using {nonempty_file_log} file as output.")
            copy_file(file_2, header_2.split("\t"), output_file)
        case ("left", False) | ("outer", False):
            logger.warning(f"{join_type} join with empty {empty_file_log} file. Using {nonempty_file_log} file as output.")
            copy_file(file_1, header_1.split("\t"), output_file)

#=======================================================================
# Join function
#=======================================================================

def join_tsvs(input_path_1: str, input_path_2: str, field: str, join_type: str, output_path: str) -> None:
    """Join two sorted TSV files linewise on a shared column."""
    # Open files for normal processing
    with open_by_suffix(input_path_1, "r") as file_1, \
         open_by_suffix(input_path_2, "r") as file_2, \
         open_by_suffix(output_path, "w") as output:
        # Read header lines and check for empty files
        header_line_1 = file_1.readline().strip()
        header_line_2 = file_2.readline().strip()
        is_empty_1 = not header_line_1
        is_empty_2 = not header_line_2
        # Handle empty file cases if needed
        if is_empty_1 or is_empty_2:
            return handle_empty_files(file_1, file_2, is_empty_1, is_empty_2, join_type, header_line_1, header_line_2, output)
        # Otherwise, process normally
        header_1 = header_line_1.split("\t")
        header_2 = header_line_2.split("\t")
        merged_header, field_index_1, field_index_2 = \
                process_headers(header_1, header_2, field)
        write_line(merged_header, output)
        # Define placeholders for non-inner joins
        placeholder_file1 = ["NA"] * (len(header_1) - 1)
        placeholder_file2 = ["NA"] * (len(header_2) - 1)
        # Get first two lines from each file
        line_1_curr, row_1_curr, id_1_curr = get_line_id(file_1, field_index_1)
        line_1_next, row_1_next, id_1_next = get_line_id(file_1, field_index_1)
        logger.debug(f"Reading current line from file 1: {row_1_curr}, {id_1_curr}")
        logger.debug(f"Reading next line from file 1: {row_1_next}, {id_1_next}")
        line_2_curr, row_2_curr, id_2_curr = get_line_id(file_2, field_index_2)
        line_2_next, row_2_next, id_2_next = get_line_id(file_2, field_index_2)
        logger.debug(f"Reading current line from file 2: {row_2_curr}, {id_2_curr}")
        logger.debug(f"Reading next line from file 2: {row_2_next}, {id_2_next}")
        # Iterate until we exhaust either file
        while line_1_curr and line_2_curr:
            assert row_1_curr is not None and id_1_curr is not None
            assert row_2_curr is not None and id_2_curr is not None
            # Verify that files are sorted
            check_sorting(id_1_curr, id_1_next, "1", input_path_1)
            check_sorting(id_2_curr, id_2_next, "2", input_path_2)
            logger.debug(f"Comparing IDs between file 1 and file 2: {id_1_curr}, {id_2_curr}")
            # If IDs match, write to output and check for one-to-many join
            if id_1_curr == id_2_curr:
                logger.debug(f"IDs match: {id_1_curr}, {id_2_curr}")
                merged_row = row_1_curr + [val for i, val in enumerate(row_2_curr) if i != field_index_2]
                write_line(merged_row, output)
                if id_1_curr != id_1_next and id_2_curr != id_2_next:
                    logger.debug(f"Neither current ID matches next ID; advancing both files.")
                    line_1_curr, row_1_curr, id_1_curr = line_1_next, row_1_next, id_1_next
                    line_1_next, row_1_next, id_1_next = get_line_id(file_1, field_index_1)
                    logger.debug(f"Updated current line from file 1: {row_1_curr}, {id_1_curr}")
                    logger.debug(f"Updated next line from file 1: {row_1_next}, {id_1_next}")
                    line_2_curr, row_2_curr, id_2_curr = line_2_next, row_2_next, id_2_next
                    line_2_next, row_2_next, id_2_next = get_line_id(file_2, field_index_2)
                    logger.debug(f"Updated current line from file 2: {row_2_curr}, {id_2_curr}")
                    logger.debug(f"Updated next line from file 2: {row_2_next}, {id_2_next}")
                elif id_1_curr != id_1_next:
                    logger.debug(f"Current ID from file 2 matches next ID; advancing file 2 only.")
                    line_2_curr, row_2_curr, id_2_curr = line_2_next, row_2_next, id_2_next
                    line_2_next, row_2_next, id_2_next = get_line_id(file_2, field_index_2)
                    logger.debug(f"Updated current line from file 2: {row_2_curr}, {id_2_curr}")
                    logger.debug(f"Updated next line from file 2: {row_2_next}, {id_2_next}")
                elif id_2_curr != id_2_next:
                    logger.debug(f"Current ID from file 1 matches next ID; advancing file 1 only.")
                    line_1_curr, row_1_curr, id_1_curr = line_1_next, row_1_next, id_1_next
                    line_1_next, row_1_next, id_1_next = get_line_id(file_1, field_index_1)
                    logger.debug(f"Updated current line from file 1: {row_1_curr}, {id_1_curr}")
                    logger.debug(f"Updated next line from file 1: {row_1_next}, {id_1_next}")
                else:
                    ids = f"{id_1_curr}, {id_1_next}, {id_2_curr}, {id_2_next}"
                    msg = f"Unsupported many-to-many join detected for ID {id_1_curr} ({ids})."
                    logger.error(msg)
                    raise ValueError(msg)
            # If IDs don't match, handle on the basis of join type
            elif id_1_curr < id_2_curr:
                logger.debug(f"File 1 ID is less than file 2 ID: {id_1_curr}, {id_2_curr}")
                # File 2 is missing ID present in file 1
                if join_type == "strict":
                    msg = f"Strict join failed: ID {id_1_curr} missing from file 2."
                    logger.error(msg)
                    raise ValueError(msg)
                elif join_type in ("left", "outer"):
                    merged_row = fill_right(row_1_curr, placeholder_file2)
                    write_line(merged_row, output)
                else:
                    logger.debug(f"Skipping line.")
                logger.debug(f"Advancing file 1.")
                line_1_curr, row_1_curr, id_1_curr = line_1_next, row_1_next, id_1_next
                line_1_next, row_1_next, id_1_next = get_line_id(file_1, field_index_1)
                logger.debug(f"Updated current line from file 1: {row_1_curr}, {id_1_curr}")
                logger.debug(f"Updated next line from file 1: {row_1_next}, {id_1_next}")
            else:
                # File 1 is missing ID present in file 2
                logger.debug(f"File 2 ID is less than file 1 ID: {id_2_curr}, {id_1_curr}")
                if join_type == "strict":
                    msg = f"Strict join failed: ID {id_2_curr} missing from file 1."
                    logger.error(msg)
                    raise ValueError(msg)
                elif join_type in ("right", "outer"):
                    merged_row = fill_left(placeholder_file1, row_2_curr, id_2_curr, field_index_1, field_index_2)
                    write_line(merged_row, output)
                else:
                    logger.debug(f"Skipping line.")
                logger.debug(f"Advancing file 2.")
                line_2_curr, row_2_curr, id_2_curr = line_2_next, row_2_next, id_2_next
                line_2_next, row_2_next, id_2_next = get_line_id(file_2, field_index_2)
                logger.debug(f"Updated current line from file 2: {row_2_curr}, {id_2_curr}")
                logger.debug(f"Updated next line from file 2: {row_2_next}, {id_2_next}")
        # Read out file 1
        while line_1_curr:
            assert row_1_curr is not None and id_1_curr is not None
            logger.debug(f"Reading current line from file 1 only: {id_1_curr}")
            check_sorting(id_1_curr, id_1_next, "1", input_path_1)
            if join_type == "strict":
                msg = f"Strict join failed: ID {id_1_curr} missing from file 2."
                logger.error(msg)
                raise ValueError(msg)
            elif join_type in ("left", "outer"):
                merged_row = fill_right(row_1_curr, placeholder_file2)
                write_line(merged_row, output)
            else:
                logger.debug(f"Skipping line.")
            logger.debug(f"Advancing file 1.")
            line_1_curr, row_1_curr, id_1_curr = line_1_next, row_1_next, id_1_next
            line_1_next, row_1_next, id_1_next = get_line_id(file_1, field_index_1)
            logger.debug(f"Updated current line from file 1: {row_1_curr}, {id_1_curr}")
            logger.debug(f"Updated next line from file 1: {row_1_next}, {id_1_next}")
        # Read out file 2
        while line_2_curr:
            assert row_2_curr is not None and id_2_curr is not None
            logger.debug(f"Reading current line from file 2 only: {id_2_curr}")
            check_sorting(id_2_curr, id_2_next, "2", input_path_2)
            if join_type == "strict":
                msg = f"Strict join failed: ID {id_2_curr} missing from file 1."
                logger.error(msg)
                raise ValueError(msg)
            elif join_type in ("right", "outer"):
                merged_row = fill_left(placeholder_file1, row_2_curr, id_2_curr, field_index_1, field_index_2)
                write_line(merged_row, output)
            else:
                logger.debug(f"Skipping line.")
            logger.debug(f"Advancing file 2.")
            line_2_curr, row_2_curr, id_2_curr = line_2_next, row_2_next, id_2_next
            line_2_next, row_2_next, id_2_next = get_line_id(file_2, field_index_2)
            logger.debug(f"Updated current line from file 2: {row_2_curr}, {id_2_curr}")
            logger.debug(f"Updated next line from file 2: {row_2_next}, {id_2_next}")

#=======================================================================
# Main function
#=======================================================================

def main() -> None:
    logger.info("Initializing script.")
    start_time = time.time()
    # Parse arguments
    logger.info("Parsing arguments.")
    args = parse_args()
    logger.info(f"Arguments: {args}")
    # Run joining function
    logger.info("Executing join.")
    join_tsvs(args.tsv1, args.tsv2, args.field, args.join_type, args.output_file)
    # Log completion
    logger.info("Script completed successfully.")
    end_time = time.time()
    logger.info(f"Total time elapsed: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
