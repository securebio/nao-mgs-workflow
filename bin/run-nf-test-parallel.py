#!/usr/bin/env python3
DESC = """
Parallelize nf-test execution by running tests in shards across multiple processes.
"""

###########
# IMPORTS #
###########

import argparse
import logging
import os
import subprocess
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Optional
from functools import partial

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

###########
# LOGGING #
###########

class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

####################
# HELPER FUNCTIONS #
####################

def load_aws_credentials() -> None:
    """
    Ensure AWS credentials are available in the environment.
    If not already set, attempt to load them using boto3's credential chain.
    This will check environment variables, ~/.aws/credentials, IAM roles, etc.
    Raises:
        RuntimeError: If credentials cannot be loaded
    """
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        logger.info("AWS credentials found in environment")
        return
    logger.info("AWS credentials not in environment, attempting to load via boto3...")
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials is None:
            raise RuntimeError("Failed to load AWS credentials via boto3.")
        frozen_creds = credentials.get_frozen_credentials()
        os.environ["AWS_ACCESS_KEY_ID"] = frozen_creds.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = frozen_creds.secret_key
        if frozen_creds.token:
            os.environ["AWS_SESSION_TOKEN"] = frozen_creds.token
        logger.info("AWS credentials loaded successfully via boto3")
    except (NoCredentialsError, PartialCredentialsError) as e:
        raise RuntimeError("Failed to load AWS credentials via boto3.") from e

def construct_test_command(additional_args: List[str]) -> List[str]:
    """
    Construct the command to run a single nf-test shard.
    Args:
        additional_args: Additional arguments to pass to nf-test
    Returns:
        List of command arguments
    """
    env_vars = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    }
    if "AWS_SESSION_TOKEN" in os.environ:
        env_vars["AWS_SESSION_TOKEN"] = os.environ["AWS_SESSION_TOKEN"]
    cmd = ["sudo", "env"]
    for key, value in env_vars.items():
        cmd.append(f"{key}={value}")
    cmd.extend(["nf-test", "test"])
    cmd.extend(additional_args)
    return cmd

def execute_subprocess(cmd: List[str], log_file: Path, repo_root: Path) -> int:
    """
    Execute a subprocess and log the output to a file.
    Args:
        cmd: Command to execute
        log_file: Path to log file
    Returns:
        Exit code of the subprocess
    """
    with open(log_file, 'w') as f:
        result = subprocess.run(cmd, stdout=f, stderr=f, text=True, cwd=str(repo_root))
    return result.returncode

def run_nf_test_shard(shard: int, total_shards: int,
                      test_command: List[str],
                      log_dir: Path,
                      repo_root: Path) -> Tuple[int, int, str]:
    """
    Run a single nf-test shard.
    Args:
        shard: Shard number (1-indexed)
        total_shards: Total number of shards
        test_command: Command to run nf-test
        log_dir: Directory to store log files
        repo_root: Repository root directory
    Returns:
        Tuple of (shard_number, exit_code, log_file_path)
    """
    log_file = log_dir / f"shard_{shard}.log"
    logger.info(f"Starting shard {shard}/{total_shards}")
    cmd = test_command + ["--shard", f"{shard}/{total_shards}"]
    result = execute_subprocess(cmd, log_file, repo_root)
    if result != 0:
        logger.error(f"Shard {shard}/{total_shards} failed with exit code {result}")
    else:
        logger.info(f"Shard {shard}/{total_shards} completed successfully")
    return (shard, result, str(log_file))

def extract_failures_from_log(log_file: Path) -> List[str]:
    """
    Extract FAILED test names from a log file.
    Args:
        log_file: Path to log file
    Returns:
        List of failed test names
    """
    failures = []
    try:
        with open(log_file, 'r') as f:
            for line in f:
                if "FAILED" in line:
                    failures.append(line.strip())
    except Exception as e:
        logger.warning(f"Could not read log file {log_file}: {e}")
    return failures

def log_failures(failed_shards: List[Tuple[int, int, str]], n_shards: int, output_log: Path) -> None:
    """
    Write failure information to log file and console.
    Args:
        failed_shards: List of (shard_num, exit_code, log_file) tuples for failed shards
        n_shards: Total number of shards
        output_log: Path to consolidated error log file
    """
    logger.error(f"{len(failed_shards)} shard(s) failed")

    # Write consolidated error log to file
    with open(output_log, 'w') as out:
        out.write("=" * 80 + "\n")
        out.write(f"PARALLEL NF-TEST FAILURE SUMMARY\n")
        out.write(f"Failed shards: {len(failed_shards)}/{n_shards}\n")
        out.write("=" * 80 + "\n\n")

        for shard_num, exit_code, log_file in failed_shards:
            out.write(f"\n{'=' * 80}\n")
            out.write(f"SHARD {shard_num} (exit code: {exit_code})\n")
            out.write(f"{'=' * 80}\n\n")

            # Extract and write failures
            failures = extract_failures_from_log(Path(log_file))
            if failures:
                out.write("Failed tests:\n")
                for failure in failures:
                    out.write(f"  {failure}\n")
                out.write("\n")

            # Write full log
            out.write("Full log:\n")
            out.write("-" * 80 + "\n")
            try:
                with open(log_file, 'r') as f:
                    out.write(f.read())
            except Exception as e:
                out.write(f"Could not read log file: {e}\n")
            out.write("\n")

    logger.error(f"Test failures written to: {output_log}")

    # Print summary of failures to console
    logger.error("\n" + "=" * 80)
    logger.error("FAILED TESTS SUMMARY")
    logger.error("=" * 80)
    for shard_num, _, log_file in failed_shards:
        failures = extract_failures_from_log(Path(log_file))
        if failures:
            logger.error(f"\nShard {shard_num}:")
            for failure in failures:
                logger.error(f"  {failure}")
    logger.error(f"\nFull details in: {output_log}")
    logger.error("=" * 80)

def update_plugins() -> None:
    """
    Update nf-test plugins before running parallel tests.
    This prevents race conditions when multiple shards try to install plugins.
    """
    logger.info("Updating nf-test plugins...")
    try:
        result = subprocess.run(
            ["nf-test", "update-plugins"],
            capture_output=True,
            text=True,
            timeout=120
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to update plugins: {result.stderr}")
        logger.info("Plugins updated successfully")
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("Plugin update timed out after 120 seconds") from e

##############
# MAIN LOGIC #
##############

def run_parallel_tests(n_shards: int, additional_args: List[str],
                       output_log: Path, repo_root: Path) -> None:
    """
    Run nf-test in parallel across multiple shards.
    Args:
        n_shards: Number of parallel shards to run
        additional_args: Additional arguments to pass to nf-test
        output_log: Path to consolidated error log file
        repo_root: Repository root directory
    Raises:
        RuntimeError: If any test shards fail
    """
    update_plugins()
    cmd = construct_test_command(additional_args)
    run_shard = partial(
        run_nf_test_shard,
        total_shards=n_shards,
        test_command=cmd,
        repo_root=repo_root
    )
    logger.info(f"Running {n_shards} test shards in parallel...")
    with tempfile.TemporaryDirectory() as tmpdir:
        log_dir = Path(tmpdir)
        results = []
        with ProcessPoolExecutor(max_workers=n_shards) as executor:
            futures = {
                executor.submit(run_shard, shard=shard, log_dir=log_dir): shard
                for shard in range(1, n_shards + 1)
            }
            for future in as_completed(futures):
                shard_num, exit_code, log_file = future.result()
                results.append((shard_num, exit_code, log_file))
        # Analyze results
        failed_shards = [(num, code, log) for num, code, log in results if code != 0]
        if failed_shards:
            log_failures(failed_shards, n_shards, output_log)
            raise RuntimeError(f"{len(failed_shards)} shard(s) failed. See {output_log} for details.")
        else:
            logger.info(f"All {n_shards} shards completed successfully.")

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "n_shards",
        type=int,
        help="Number of parallel shards to run (recommended: number of CPU cores)"
    )
    parser.add_argument(
        "additional_args",
        nargs="*",
        help="Additional arguments to pass to nf-test"
    )
    parser.add_argument(
        "--output-log",
        type=Path,
        default=Path("test-logs.txt"),
        help="Path to consolidated error log file (default: test-logs.txt)"
    )
    args = parser.parse_args()
    if args.n_shards < 1:
        parser.error("n_shards must be at least 1")
    return args

def main() -> None:
    """Main entry point."""
    args = parse_arguments()
    logger.info(f"Arguments: {args}")
    load_aws_credentials()
    repo_root = Path(__file__).resolve().parent
    run_parallel_tests(
        n_shards=args.n_shards,
        additional_args=args.additional_args,
        output_log=args.output_log,
        repo_root=repo_root
    )

if __name__ == "__main__":
    main()
