#!/usr/bin/env python

"""
Given a TSV column with an initial header line,
return a new TSV containing either:
1. Only the specified columns, with all others dropped ("keep" mode)
2. All columns except the specified ones, with only the latter dropped ("drop" mode)
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
    desc = (
        "Given a TSV column with an initial header line, "
        "return a new TSV containing either: "
        "1. Only the specified columns, with all others dropped ('keep' mode) or "
        "2. All columns except the specified ones, with only the latter dropped ('drop' mode)."
    )
    parser = argparse.ArgumentParser(description=desc)
    # Add arguments
    parser.add_argument("--input", "-i", help="Path to input TSV.")
    parser.add_argument("--output", "-o", help="Path to output TSV.")
    parser.add_argument("--fields", "-f", help="Comma-separated list of fields to select.")
    parser.add_argument("--mode", "-m", 
                        choices=["keep", "drop"],
                        help="Mode to select columns: 'keep' or 'drop'.")
    # Return parsed arguments
    return parser.parse_args()

def open_by_suffix(filename: str, mode: str = "r") -> IO[str]:
    """
    Open a file with the appropriate compression, as inferred from the filename suffix.
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
    if filename.endswith('.gz'):
        return gzip.open(filename, mode + 't')  # type: ignore[return-value]
    elif filename.endswith('.bz2'):
        return bz2.BZ2File(filename, mode)  # type: ignore[call-overload,return-value,no-any-return]
    else:
        return open(filename, mode)

#=======================================================================
# TSV processing functions
#=======================================================================

def get_header_index(
    headers: list[str],
    field: str,
    mode: str = "keep"
) -> int | None:
    """
    Get the index of a field in a header line. If the field is not found,
    raise an error (if mode is "keep") or raise a warning and return None
    (if mode is "drop").
    Args:
        headers (list[str]): List of header fields.
        field (str): Field to get index of.
        mode (str): Mode to select columns: 'keep' or 'drop'.
    Returns:
        int: Index of field in header.
    """
    try:
        return headers.index(field)
    except ValueError:
        msg = f"Field not found in header: {field}"
        if mode == "keep":
            logger.error(msg)
            raise ValueError(msg)
        else:
            logger.warning(msg)
            return None
    
def join_line(inputs: list[str]) -> str:
    """Join a list of strings with tabs followed by a newline."""
    return "\t".join(inputs) + "\n"

def subset_line(inputs: list[str], indices: list[int]) -> list[str]:
    """Subset a list of strings to a list of indices."""
    return [inputs[i] for i in indices]

def select_columns(input_path: str, output_path: str, fields: list[str], mode: str) -> None:
    """Select columns in TSV file."""
    with open_by_suffix(input_path) as inf, open_by_suffix(output_path, "w") as outf:
        # Read the header line
        header_line = inf.readline().strip()
        logger.debug(f"Input header: {header_line}")
        if not header_line:
            # No header = empty file, log and create empty output
            logger.warning("Input file is empty (no header). Creating empty output file.")
            return
        headers_in = header_line.split("\t")
        # Get indices of selected fields
        indices_raw = [get_header_index(headers_in, field, mode) for field in fields]
        # Get indices of fields to keep
        indices_keep: list[int]
        if mode == "keep":
            indices_keep = [i for i in indices_raw if i is not None]
        elif mode == "drop":
            indices_drop = [i for i in indices_raw if i is not None]
            indices_keep = [i for i in range(len(headers_in)) if i not in indices_drop]
        if not indices_keep:
            raise ValueError("Dropping all fields.")
        # Write new header
        new_header_line = join_line(subset_line(headers_in, indices_keep))
        logger.debug(f"Output header: {new_header_line}")
        outf.write(new_header_line)
        # Subset and write body lines of input file
        for line in inf:
            line = line.strip()
            if not line:
                continue
            line_split_subset = subset_line(line.split("\t"), indices_keep)
            outf.write(join_line(line_split_subset))

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
    logger.info(f"Input TSV file: {input_path}")
    logger.info(f"Output TSV file: {output_path}")
    # Check mode
    mode = args.mode
    logger.info(f"Mode: {mode}")
    # Extract fields to select from header
    fields = args.fields.split(",")
    logger.info(f"Fields to {mode}: {fields}")
    if len(fields) == 0:
        raise ValueError("No fields to select.")
    # Select columns from input TSV
    logger.info("Selecting columns from input TSV.")
    select_columns(input_path, output_path, fields, mode)
    # Finish time tracking
    logger.info("Script completed successfully.")
    end_time = time.time()
    logger.info(f"Total time elapsed: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()