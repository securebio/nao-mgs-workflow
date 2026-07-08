#!/usr/bin/env python3
"""Check that the benchmark index was built from a released pipeline version.

Downloads the ``index-latest`` pyproject.toml from S3, reads
``[project].version``, and raises an error if it carries a ``-dev``
pre-release suffix. Used as a release gate so a production index built from a
``-dev`` pipeline branch is never promoted to ``main``.
"""

###########
# IMPORTS #
###########

import argparse
import logging
import tomllib
from datetime import UTC, datetime

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

########################
# INDEX VERSION CHECKS #
########################


def fetch_pyproject_from_s3(s3_uri: str) -> str:
    """Download and return the contents of a pyproject.toml from S3.
    Args:
        s3_uri: S3 URI to the pyproject.toml file (e.g. 's3://bucket/key').
    Returns:
        The contents of the file as a string.
    """
    s3_path = s3_uri.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content: str = response["Body"].read().decode("utf-8")
    return content


def get_index_version(pyproject_text: str) -> str:
    """Extract ``[project].version`` from pyproject.toml text.
    Args:
        pyproject_text: Raw contents of the index pyproject.toml.
    Returns:
        The pipeline version the index was built with.
    """
    data = tomllib.loads(pyproject_text)
    return str(data["project"]["version"])


def is_dev_version(version: str) -> bool:
    """Return True if the version carries a ``-dev`` pre-release suffix.
    Args:
        version: A pipeline/index version string.
    Returns:
        True if the version ends in ``-dev``, False otherwise.
    """
    return version.endswith("-dev")


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
        "--s3-pyproject",
        default="s3://nao-testing/mgs-workflow-test/index-latest/output/logging/pyproject.toml",
        help="S3 URI to the benchmark index pyproject.toml",
    )
    return parser.parse_args()


def main() -> None:
    """Check the benchmark index version and error if it is a -dev build."""
    args = parse_arguments()
    pyproject_text = fetch_pyproject_from_s3(args.s3_pyproject)
    version = get_index_version(pyproject_text)
    logger.info("Benchmark index version: %s", version)
    if is_dev_version(version):
        raise RuntimeError(
            f"Benchmark index at {args.s3_pyproject} was built from a -dev "
            f"pipeline version ({version}); a release must use an index built "
            "from a tagged (non-dev) pipeline version. Rebuild the benchmark "
            "index from a released version before merging to main.",
        )
    logger.info("OK: index version %s is not a -dev build", version)


if __name__ == "__main__":
    main()
