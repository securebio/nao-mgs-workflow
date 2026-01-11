#!/usr/bin/env python3
DESC = """
Parallelize nf-test execution by running tests in shards across multiple processes.

This script should be invoked via the wrapper script bin/run-nf-test-parallel.sh,
which handles sudo and environment variable passing.
"""

###########
# IMPORTS #
###########

import argparse
import logging
import os
import re
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Optional
from functools import partial
import pwd

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

def strip_ansi_codes(text: str) -> str:
    """
    Remove ANSI escape codes from text.
    Args:
        text: Text containing ANSI escape codes
    Returns:
        Text with ANSI codes removed
    """
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def construct_test_command(additional_args: List[str]) -> List[str]:
    """
    Construct the command to run a single nf-test shard.
    Note: This script should be run with sudo via the wrapper script.
    Args:
        additional_args: Additional arguments to pass to nf-test
    Returns:
        List of command arguments
    """
    cmd = ["nf-test", "test", "--verbose", "--debug"]
    cmd.extend(additional_args)
    return cmd

def execute_subprocess(cmd: List[str]) -> Tuple[int, str, str]:
    """
    Execute a subprocess and capture output.
    Args:
        cmd: Command to execute
    Returns:
        Tuple of (exit_code, stdout, stderr)
    """
    result = subprocess.run(cmd, capture_output=True, text=True)
    return (result.returncode, result.stdout, result.stderr)

def run_nf_test_shard(shard: int, total_shards: int,
                      test_command: List[str]) -> Tuple[int, int, str, str, str]:
    """
    Run a single nf-test shard.
    Args:
        shard: Shard number (1-indexed)
        total_shards: Total number of shards
        test_command: Command to run nf-test
    Returns:
        Tuple of (shard_number, exit_code, stdout, stderr, command_str)
    """
    import pwd

    # Debug: log process info
    logger.info(f"Starting shard {shard}/{total_shards}")

    cmd = test_command + ["--shard", f"{shard}/{total_shards}"]
    cmd_str = " ".join(cmd)
    exit_code, stdout, stderr = execute_subprocess(cmd)

    # Strip ANSI escape codes from output
    stdout = strip_ansi_codes(stdout)
    stderr = strip_ansi_codes(stderr)

    if exit_code != 0:
        logger.error(f"Shard {shard}/{total_shards} failed with exit code {exit_code}")
    else:
        logger.info(f"Shard {shard}/{total_shards} completed successfully")
    return (shard, exit_code, stdout, stderr, cmd_str)

def extract_failures_from_output(output: str) -> List[str]:
    """
    Extract FAILED test names from test output.
    Args:
        output: Test output string (stdout or stderr)
    Returns:
        List of failed test names
    """
    failures = []
    for line in output.splitlines():
        if "FAILED" in line:
            failures.append(line.strip())
    return failures

def write_test_log(all_shards: List[Tuple[int, int, str, str, str]],
                   failed_shards: List[Tuple[int, int, str, str, str]],
                   n_shards: int,
                   output_log: Path) -> None:
    """
    Write comprehensive test log with all results and failure summary.
    Args:
        all_shards: List of (shard_num, exit_code, stdout, stderr, cmd_str) tuples for all shards
        failed_shards: List of (shard_num, exit_code, stdout, stderr, cmd_str) tuples for failed shards
        n_shards: Total number of shards
        output_log: Path to consolidated log file
    """
    with open(output_log, 'w') as out:
        out.write("=" * 80 + "\n")
        out.write(f"NF-TEST RESULTS BY SHARD\n")
        out.write(f"Total shards: {n_shards}\n")
        out.write(f"Passed shards: {n_shards - len(failed_shards)}\n")
        out.write(f"Failed shards: {len(failed_shards)}\n")
        out.write(f"Working directory: {os.getcwd()}\n")
        out.write(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        out.write("=" * 80 + "\n\n")

        for shard_num, exit_code, stdout, stderr, cmd_str in sorted(all_shards):
            status = "PASSED" if exit_code == 0 else "FAILED"
            out.write("=" * 80 + "\n")
            out.write(f"Shard {shard_num}/{n_shards}: {status}\n")
            out.write("=" * 80 + "\n\n")
            out.write("-" * 80 + "\n")
            out.write(f"COMMAND STRING\n")
            out.write("-" * 80 + "\n\n")
            out.write(f"{cmd_str}\n\n")
            out.write("-" * 80 + "\n")
            out.write("STDOUT\n")
            out.write("-" * 80 + "\n\n")
            out.write(stdout.strip() + "\n\n")
            out.write("-" * 80 + "\n")
            out.write("STDERR\n")
            out.write("-" * 80 + "\n\n")
            if stderr:
                out.write(stderr.strip() + "\n\n")
            else:
                out.write("None\n\n")

        # If there were failures, add a failure summary section
        if failed_shards:
            out.write("=" * 80 + "\n")
            out.write("FAILURE SUMMARY\n")
            out.write("=" * 80 + "\n\n")
            for shard_num, exit_code, stdout, stderr, cmd_str in sorted(failed_shards):
                out.write(f"Shard {shard_num} (exit code: {exit_code}):\n")
                failures = extract_failures_from_output(stdout + "\n" + stderr)
                if failures:
                    for failure in failures:
                        out.write(f"    - {failure}\n")
                out.write("\n")
    logger.info(f"Test results written to: {output_log}")

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
                       output_log: Path) -> int:
    """
    Run nf-test in parallel across multiple shards.
    Args:
        n_shards: Number of parallel shards to run
        additional_args: Additional arguments to pass to nf-test
        output_log: Path to consolidated error log file
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    update_plugins()
    cmd = construct_test_command(additional_args)
    run_shard = partial(
        run_nf_test_shard,
        total_shards=n_shards,
        test_command=cmd
    )
    logger.info(f"Running {n_shards} test shards in parallel...")
    results = []
    with ProcessPoolExecutor(max_workers=n_shards) as executor:
        futures = {
            executor.submit(run_shard, shard=shard): shard
            for shard in range(1, n_shards + 1)
        }
        for future in as_completed(futures):
            shard_num, exit_code, stdout, stderr, cmd_str = future.result()
            results.append((shard_num, exit_code, stdout, stderr, cmd_str))

    # Analyze results and write log
    failed_shards = [(num, code, out, err, cmd) for num, code, out, err, cmd in results if code != 0]
    write_test_log(results, failed_shards, n_shards, output_log)

    if failed_shards:
        logger.error(f"{len(failed_shards)} shard(s) failed:")
        for shard_num, exit_code, stdout, stderr, cmd_str in failed_shards:
            logger.error(f"\t- Shard {shard_num} failed with exit code {exit_code}")
        return 1
    else:
        logger.info(f"All {n_shards} shards completed successfully.")
        return 0

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
    repo_root = Path(__file__).resolve().parent.parent
    original_cwd = os.getcwd()
    try:
        os.chdir(repo_root)
        exit_code = run_parallel_tests(
            n_shards=args.n_shards,
            additional_args=args.additional_args,
            output_log=args.output_log,
        )
        exit(exit_code)
    finally:
        os.chdir(original_cwd)

if __name__ == "__main__":
    main()
