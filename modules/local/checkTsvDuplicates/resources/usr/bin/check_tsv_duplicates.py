#!/usr/bin/env python

"""
Given a sorted TSV file with an initial header line, check that
every line in the file has a unique value of the specified column.
If so, write the file to the output path. If not, throw an error.
"""

#=======================================================================
# Import modules
#=======================================================================

import logging
from datetime import datetime, timezone
import argparse
import time
import gzip
import bz2
import io
from typing import IO

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
    desc = "Given a sorted TSV file with an initial header line, " \
           "check that every line has a unique value of the specified column. " \
           "If so, write the file to the output path. If not, throw an error."
    parser = argparse.ArgumentParser(description=desc)
    # Add arguments
    parser.add_argument("--input", "-i", help="Path to sorted input TSV.")
    parser.add_argument("--output", "-o", help="Path to output TSV.")
    parser.add_argument("--field", "-f", help="Field to check for duplicates.")
    # Return parsed arguments
    return parser.parse_args()

def open_by_suffix(filename: str, mode: str = "r") -> IO[str]:
    """
    Open a file with appropriate compression, based on the filename suffix.
    Args:
        filename (str): Path to file.
        mode (str): Mode to open file in.
    Returns:
        IO[str]: File object.
    """
    logger.debug(f"\tOpening file object: {filename}")
    logger.debug(f"\tOpening mode: {mode}")
    logger.debug(f"\tGZIP mode: {filename.endswith('.gz')}")
    logger.debug(f"\tBZ2 mode: {filename.endswith('.bz2')}")
    if filename.lower().endswith(".gz"):
        return gzip.open(filename, mode + 't')  # type: ignore[return-value]
    elif filename.lower().endswith(".bz2"):
        return bz2.BZ2File(filename, mode)  # type: ignore[call-overload,return-value,no-any-return]
    else:
        return open(filename, mode)

#=======================================================================
# TSV processing functions
#=======================================================================

def get_header_index(headers: list[str], field: str) -> int:
    """
    Get the index of a field in a header line.
    Args:
        headers (list[str]): List of header fields.
        field (str): Field to get index of.
    Returns:
        int: Index of selected field.
    """
    try:
        return headers.index(field)
    except ValueError:
        raise ValueError(f"Field not found in header: {field}")

def process_header(header_line: str, field: str) -> int:
    """
    Process header line and return index of selected field.
    Args:
        header_line (str): Header line from TSV file.
        field (str): Field to check for duplicates.
    Returns:
        int: Index of selected field.
    """
    # Check header exists
    logger.debug(f"Input header: {header_line}")
    if not header_line:
        # No header = selected fields are missing
        raise ValueError("No header to select fields from.")
    # Split header line into list of fields
    headers_in = header_line.split("\t")
    # Get index of selected field
    index = get_header_index(headers_in, field)
    # Return index
    return index

def check_duplicates(input_path: str, output_path: str, field: str) -> None:
    """
    Check for duplicates in specified field of a sorted TSV file.
    Args:
        input_path (str): Path to sorted input TSV file.
        output_path (str): Path to output TSV file.
        field (str): Field to check for duplicates.
    """
    with open_by_suffix(input_path) as inf, open_by_suffix(output_path, "w") as outf:
        # Process header line
        header_line = inf.readline().strip()
        index = process_header(header_line, field)
        # Write header to output
        outf.write(header_line + "\n")
        # Check body for duplicates
        field_prev = None
        for line in inf:
            # If empty line, break
            line = line.strip()
            if not line:
                break
            # Get value of selected field
            field_curr = line.split("\t")[index]
            # Check file is sorted
            if field_prev is not None and field_curr < field_prev:
                msg = f"File is not sorted: {field_curr} < {field_prev}"
                logger.error(msg)
                raise ValueError(msg)
            # Check if field is duplicate with previous line
            elif field_prev is not None and field_curr == field_prev:
                msg = f"Duplicate value found in field {field}: {field_curr}"
                logger.error(msg)
                raise ValueError(msg)
            # Write line to output
            outf.write(line + "\n")
            # Update previous field
            field_prev = field_curr

#=======================================================================
# Main function
#=======================================================================

def main() -> None:
    logger.info("Initializing script.")
    start_time = time.time()
    # Parse arguments
    logger.info("Parsing arguments.")
    args = parse_args()
    input_path = args.input
    output_path = args.output
    field = args.field
    logger.info(f"Input TSV file: {input_path}")
    logger.info(f"Output TSV file: {output_path}")
    logger.info(f"Field to check for duplicates: {field}")
    # Check for duplicates
    logger.info("Checking for duplicates.")
    check_duplicates(input_path, output_path, field)
    # Finish time tracking
    logger.info("Script completed successfully.")
    end_time = time.time()
    logger.info(f"Total time elapsed: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()