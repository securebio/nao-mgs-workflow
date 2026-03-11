#!/usr/bin/env python3

"""
Sort a TSV file by a specific column header using GNU sort.
"""

#=======================================================================
# Import modules
#=======================================================================

import os
import sys
import gzip
import argparse
import subprocess
import logging
from datetime import datetime, timezone
import time
import tempfile
import io
from typing import IO, cast
import shutil
import math

#=======================================================================
# Configure logging
#=======================================================================

class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
logging.basicConfig(level=logging.DEBUG)
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
    parser = argparse.ArgumentParser(description="Sort a TSV file by a specific column header.")
    parser.add_argument("input_file", help="Input TSV file path")
    parser.add_argument("sort_field", help="Column header to sort by")
    parser.add_argument("output_file", help="Output TSV file path")
    parser.add_argument("--memory-limit", "-m", type=int, help="Memory limit in GB", required=True)
    # Return parsed arguments
    return parser.parse_args()

def open_by_suffix(filename: str, mode: str = "r") -> IO[str]:
    """
    Open a file with the appropriate compression, based on
    the filename suffix.
    Args:
        filename (str): The path to the file to open.
        mode (str): The mode to open the file in.
    Returns:
        IO[str]: The opened file object.
    """
    logger.debug(f"Opening file object: {filename}")
    logger.debug(f"Opening mode: {mode}")
    logger.debug(f"GZIP mode: {filename.endswith('.gz')}")
    if filename.endswith('.gz'):
        return cast(IO[str], gzip.open(filename, mode + 't'))
    else:
        return open(filename, mode)

def process_header(header_line: str, sort_field: str) -> int | None:
    """
    Process the header line and find the index of the sort field.
    Args:
        header_line (str): The header line of the TSV file.
        sort_field (str): The column header to sort by.
    Returns:
        int | None: The index of the sort field, or None if the file is empty.
    """
    # Check if file is empty (no header)
    if not header_line:
        logger.warning(f"Input file is empty. Creating empty output file.")
        return None
    # Split the header line into fields
    header_fields = header_line.split('\t')
    # Look for the sort field in the header fields
    if sort_field not in header_fields:
        msg = f"Could not find sort field in input header: '{sort_field}', {header_fields}"
        logger.error(msg)
        raise ValueError(msg)
    index = header_fields.index(sort_field)
    logger.info(f"Found '{sort_field}' in column {index+1} (1-indexed). Sorting by column {index+1}.")
    return index

#=======================================================================
# Sorting function
#=======================================================================

def sort_tsv_file(input_file: str,
                  output_file: str,
                  sort_field: str,
                  memory_limit: int) -> None:
    """
    Sort a TSV file by a specific column header.
    Args:
        input_file (str): The input TSV file path.
        output_file (str): The output TSV file path.
        sort_field (str): The column header to sort by.
    """
    # Create local temp directory base if it doesn't exist
    temp_base = 'sort_tsv_tmp'
    temp_base_exists = os.path.exists(temp_base)
    os.makedirs(temp_base, exist_ok=True)
    logger.debug(f"Using temporary directory base: {temp_base}")
    # Use local directory to avoid memory-based tmpfs
    with tempfile.TemporaryDirectory(dir=temp_base) as temp_dir, \
            open_by_suffix(input_file) as infile, \
            open_by_suffix(output_file, "w") as outfile:
        # Define temporary file paths
        logger.debug(f"Temporary directory: {temp_dir}")
        temp_body = os.path.join(temp_dir, "body.tsv")
        temp_body_sorted = os.path.join(temp_dir, "body_sorted.tsv")
        logger.debug(f"Temporary body files: {temp_body}, {temp_body_sorted}")
        # Read the header and extract the sort field index
        header = infile.readline().strip()
        logger.debug(f"Header: {header}")
        col_index = process_header(header, sort_field)
        if col_index is None: # If no header, write empty output file
            return
        else: # Otherwise, write header to output
            outfile.write(header + "\n")
        # Check if there are any data rows by reading the first data line
        first_data_line = infile.readline().strip()
        if not first_data_line: # If no data rows, stop here
            logger.warning("No data rows to sort, writing header only")
            return
        # Otherwise, write data lines to temp file
        with open_by_suffix(temp_body, "w") as temp:
            temp.write(first_data_line + "\n")
            for line in infile:
                temp.write(line)
        # Sort temporary file (with custom temp directory for GNU sort)
        run_sort(temp_body, temp_body_sorted, col_index, temp_base, memory_limit)
        # Write sorted data to output
        with open_by_suffix(temp_body_sorted) as temp:
            for line in temp:
                outfile.write(line)
    # Clean up temporary directory
    if not temp_base_exists:
        shutil.rmtree(temp_base)

def run_sort(input_file: str,
             output_file: str,
             sort_index: int,
             temp_dir: str,
             memory_limit: int) -> None:
    """
    Run the sort command on a TSV file with a specified field separator,
    handling errors and logging, and write to an output file.
    Args:
        input_file (str): The input TSV file path.
        output_file (str): The output TSV file path.
        sort_index (int): The index of the column to sort by.
        temp_dir (str): The temporary directory for GNU sort to use.
        memory_limit (int): The memory limit in GB.
    """
    # Create the sort temporary directory
    sort_temp_dir = os.path.join(temp_dir, "sort")
    os.makedirs(sort_temp_dir, exist_ok=True)
    logger.debug(f"Created sort temporary directory: {sort_temp_dir}")
    # Build sort command with memory limits and proper temporary directory
    sort_cmd = [
        "sort", 
        "-t", "\t",  # Tab delimiter
        "-k", f"{sort_index+1},{sort_index+1}",  # Sort key
        f"--temporary-directory={sort_temp_dir}",  # Temporary directory
        f"--buffer-size={memory_limit}G",  # Memory limit
        "-o", output_file,  # Output file
        input_file  # Input file
    ]
    logger.debug(f"Running sort command: {sort_cmd}")
    try:
        subprocess.run(sort_cmd, check=True)
        logger.debug("Sort command completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Sort command failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error running sort command: {e}")
        raise

#=======================================================================
# Main function
#=======================================================================

def main() -> None:
    logger.info("Initializing script.")
    start_time = time.time()
    # Parse arguments
    logger.info("Parsing arguments.")
    args = parse_args()
    memory_limit = math.floor(args.memory_limit * 0.9)
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Sort field: {args.sort_field}")
    logger.info(f"Output file: {args.output_file}")
    logger.info(f"Memory limit: {memory_limit} GB")
    # Sort the TSV file
    logger.info("Sorting TSV file.")
    sort_tsv_file(args.input_file, args.output_file, args.sort_field, memory_limit)
    # Finish time tracking
    logger.info("Script completed successfully.")
    end_time = time.time()
    logger.info(f"Total time elapsed: {end_time - start_time} seconds")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.exit(1)