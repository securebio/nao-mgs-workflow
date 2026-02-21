#!/usr/bin/env python3
"""Check the age of the benchmark index.

Downloads the index time.txt from S3, compares its date against
max-stable-index-age-days from pyproject.toml, and raises an error
if the index is too old.
"""

###########
# IMPORTS #
###########

import argparse
import logging
import tomllib
from datetime import UTC, date, datetime

import boto3

###########
# LOGGING #
###########

class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone.
        Args:
            record: The log record to format.
            datefmt: Optional date format string (unused).
        Returns:
            Formatted timestamp string in UTC.
        """
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#####################
# INDEX AGE CHECKS  #
#####################

def get_max_index_age_days(pyproject_path: str = "pyproject.toml") -> int:
    """Read max-stable-index-age-days from pyproject.toml.
    Args:
        pyproject_path: Path to the pyproject.toml file.
    Returns:
        Maximum allowed index age in days.
    """
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    return int(data["tool"]["mgs-workflow"]["max-stable-index-age-days"])

def parse_index_date(time_text: str) -> date:
    """Extract the date from an INDEX workflow time.txt timestamp.
    The timestamp format is 'yyyy-MM-dd HH:mm:ss z (Z)', e.g.
    '2025-01-15 14:30:00 UTC (+0000)'. We parse the full timestamp by
    removing the parentheses around the offset so strptime can handle it.
    Args:
        time_text: Raw contents of time.txt from the INDEX workflow.
    Returns:
        The date portion of the parsed timestamp.
    """
    text = time_text.strip().replace("(", "").replace(")", "")
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S %Z %z").date()

def fetch_time_txt_from_s3(s3_uri: str) -> str:
    """Download and return the contents of time.txt from S3.
    Args:
        s3_uri: S3 URI to the time.txt file (e.g. 's3://bucket/key').
    Returns:
        The contents of the file as a string.
    """
    s3_path = s3_uri.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")

def check_index_age(
    index_date: date,
    max_age_days: int,
    today: date | None = None,
) -> tuple[bool, int]:
    """Check if the index is within the allowed age.
    Args:
        index_date: The date the index was created.
        max_age_days: Maximum allowed age in days.
        today: Override for the current date (for testing).
    Returns:
        Tuple of (is_ok, age_in_days).
    """
    if today is None:
        today = date.today()
    age_days = (today - index_date).days
    return age_days <= max_age_days, age_days

##############
# MAIN LOGIC #
##############

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.
    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--s3-time-txt",
        default="s3://nao-testing/mgs-workflow-test/index-latest/output/logging/time.txt",
        help="S3 URI to the index time.txt file",
    )
    parser.add_argument(
        "--pyproject",
        default="pyproject.toml",
        help="Path to pyproject.toml (default: pyproject.toml)",
    )
    return parser.parse_args()

def main() -> None:
    """Check benchmark index age and raise an error if it is too old."""
    args = parse_arguments()
    max_age_days = get_max_index_age_days(args.pyproject)
    logger.info("Maximum allowed index age: %d days", max_age_days)
    time_text = fetch_time_txt_from_s3(args.s3_time_txt)
    index_date = parse_index_date(time_text)
    logger.info("Index creation date: %s", index_date)
    is_ok, age_days = check_index_age(index_date, max_age_days)
    logger.info("Index age: %d days", age_days)
    if not is_ok:
        raise RuntimeError(
            f"Benchmark index is {age_days} days old "
            f"(max allowed: {max_age_days} days). "
            "Run the 'Rebuild benchmark index' workflow to update it.",
        )
    logger.info(
        "OK: Index age (%d days) is within limit (%d days)",
        age_days,
        max_age_days,
    )

if __name__ == "__main__":
    main()
