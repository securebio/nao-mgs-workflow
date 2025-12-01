#!/usr/bin/env python3

"""
Filters TSV files to keep only rows where a specified column matches a given value.
Treats quote characters as regular characters, not as delimiters.
"""

# =======================================================================
# Preamble
# =======================================================================

import logging
import argparse
import csv
import sys
import gzip
import bz2
from typing import TextIO, Any, Union
from datetime import datetime, timezone


# Configure logging
class UTCFormatter(logging.Formatter):
    """
    Custom logging formatter that displays timestamps in UTC.

    Returns:
        Formatted log timestamps in UTC timezone
    """

    def formatTime(self, record, datefmt=None):
        """
        Format log timestamps in UTC timezone.

        Args:
            record: LogRecord object containing timestamp data
            datefmt: Optional date format string (unused)

        Returns:
            Formatted timestamp string in UTC
        """
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

# =======================================================================
# I/O functions
# =======================================================================


def open_by_suffix(filename: str, mode: str = "r"):
    """
    Parse the suffix of a filename to determine the right open method
    to use, then open the file. Can handle .gz, .bz2, and uncompressed files.

    Args:
        filename (str): Path to file to open
        mode (str): File open mode (default "r")

    Returns:
        File handle appropriate for the file compression type
    """
    if filename.endswith(".gz"):
        return gzip.open(filename, mode + "t")
    elif filename.endswith(".bz2"):
        return bz2.BZ2File(filename, mode)
    else:
        return open(filename, mode)


def parse_args() -> argparse.Namespace:
    """
    Parse and return command-line arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Filter TSV file to keep only rows where specified column matches given value."
    )
    parser.add_argument(
        "-i",
        "--input",
        type=lambda f: open_by_suffix(f, "r"),
        required=True,
        help="Input TSV file. Supports .gz and .bz2 compression.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=lambda f: open_by_suffix(f, "w"),
        required=True,
        help="Output filtered TSV file. Supports .gz and .bz2 compression.",
    )
    parser.add_argument(
        "-c",
        "--column",
        type=str,
        required=True,
        help="Name of the column to filter on.",
    )
    parser.add_argument(
        "-v",
        "--value",
        type=str,
        required=True,
        help="Value to match in the specified column. Will be converted to appropriate type.",
    )
    parser.add_argument(
        "--keep-matching",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep rows that match the value (default: True). Use --no-keep-matching to keep non-matching rows instead.",
    )
    return parser.parse_args()


# =======================================================================
# Column functions
# =======================================================================


def validate_input_columns(
    header: list[str], column_name: str, logger: logging.Logger
) -> int:
    """
    Validate that the input header contains the specified column.

    Args:
        header (list[str]): list of column names from CSV header.
        column_name (str): Name of the column to look for.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        int: Index of the specified column.

    Raises:
        ValueError: If the specified column is missing.
    """
    if column_name not in header:
        error_msg = (
            f"Column '{column_name}' not found in header. Available columns: {header}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    column_index = header.index(column_name)
    logger.info(
        f"Input validation passed. Found {len(header)} columns. Target column '{column_name}' at index {column_index}."
    )
    return column_index


def convert_value(value_str: str) -> Union[str, bool, int, float]:
    """
    Convert string value to appropriate type (bool, int, float, or str).

    Args:
        value_str (str): String representation of the value.

    Returns:
        Union[str, bool, int, float]: Converted value in appropriate type.
    """
    # Handle empty or whitespace-only values
    if not value_str or value_str.isspace():
        return value_str
    # Try boolean conversion first
    if value_str.lower() in ("true", "false"):
        return value_str.lower() == "true"
    # Try integer conversion
    try:
        return int(value_str)
    except ValueError:
        logger.debug(f"'{value_str}' is not an integer")
    # Try float conversion
    try:
        return float(value_str)
    except ValueError:
        logger.debug(f"'{value_str}' is not a float")
    # Return as string
    logger.debug(f"'{value_str}' treated as string")
    return value_str


# =======================================================================
# Main functions
# =======================================================================


def stream_and_filter_tsv(
    input_file: TextIO,
    output_file: TextIO,
    column_name: str,
    filter_value: Any,
    keep_matching: bool,
    logger: logging.Logger,
) -> None:
    """
    Stream TSV file and filter based on column value using memory-efficient processing.

    Processes the input file row by row without loading the entire dataset into memory,
    making it suitable for large files.

    Args:
        input_file (TextIO): Input file handle.
        output_file (TextIO): Output file handle.
        column_name (str): Name of the column to filter on.
        filter_value (Any): Value to match in the column.
        keep_matching (bool): If True, keep matching rows; if False, keep non-matching rows.
        logger (logging.Logger): Logger instance for logging messages.
    """
    logger.info("Initializing streaming TSV reader and writer")
    reader = csv.reader(input_file, delimiter="\t", quotechar=None)
    writer = csv.writer(output_file, delimiter="\t", lineterminator="\n", quotechar=None)
    # Read and validate header
    try:
        header = next(reader)
    except StopIteration:
        logger.warning("Input file is empty - writing empty output file")
        return
    column_index = validate_input_columns(header, column_name, logger)
    writer.writerow(header)
    logger.info("Header processed and written to output")
    # Initialize counters
    initial_rows = 0
    filtered_rows = 0
    logger.info("Starting row-by-row streaming processing")
    logger.info(
        f"Filtering column '{column_name}' for value '{filter_value}' (type: {type(filter_value).__name__})"
    )
    logger.info(f"Keep matching: {keep_matching}")
    # Stream and process each row individually
    for row_num, row in enumerate(reader, start=1):
        initial_rows += 1
        # Log progress for large files
        if initial_rows % 100000 == 0:
            logger.info(f"Processed {initial_rows} rows, kept {filtered_rows} rows")
        try:
            # Validate row
            if len(row) != len(header):
                raise ValueError(f"Row has {len(row)} columns but header has {len(header)} columns")
            cell_value = row[column_index]
            converted_cell_value = convert_value(cell_value)
            matches = converted_cell_value == filter_value
            # Apply filtering logic
            if (keep_matching and matches) or (not keep_matching and not matches):
                writer.writerow(row)
                filtered_rows += 1
        except (ValueError, IndexError) as e:
            logger.error(f"Row {row_num}: {str(e)}")
            logger.error("Exiting due to data format error")
            raise
    # Handle header-only files
    if initial_rows == 0:
        logger.warning("Input file contains only header with no data rows")
    # Final statistics
    removed_rows = initial_rows - filtered_rows
    action = "kept" if keep_matching else "removed"
    logger.info("Streaming processing completed:")
    logger.info(f"  - Input rows processed: {initial_rows}")
    logger.info(f"  - Rows {action}: {filtered_rows}")
    logger.info(f"  - Rows {'removed' if keep_matching else 'kept'}: {removed_rows}")


def main() -> None:
    """Main function to execute the filtering process."""
    logger.info("Starting TSV column filtering.")
    try:
        # Parse arguments
        args = parse_args()
        logger.info("Parsed command-line arguments successfully.")
        keep_matching = args.keep_matching
        # Convert filter value to appropriate type
        filter_value = convert_value(args.value)
        logger.info(
            f"Filter value '{args.value}' converted to {type(filter_value).__name__}: {filter_value}"
        )
        try:
            # Stream and filter TSV
            logger.info("Starting streaming TSV processing...")
            stream_and_filter_tsv(
                args.input,
                args.output,
                args.column,
                filter_value,
                keep_matching,
                logger,
            )
            logger.info("Streaming processing completed successfully.")
        finally:
            # Close file handles if they're not stdin/stdout
            if args.input != sys.stdin:
                args.input.close()
            if args.output != sys.stdout:
                args.output.close()
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        sys.exit(1)
    logger.info("TSV column filtering completed.")


if __name__ == "__main__":
    main()
