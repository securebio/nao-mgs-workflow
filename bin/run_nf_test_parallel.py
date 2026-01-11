#!/usr/bin/env python3
DESC = """
Parallelize nf-test execution by distributing test files across multiple processes.

This script finds all *.nf.test files in the specified paths and divides them
among parallel workers, avoiding nf-test's buggy built-in sharding feature.

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
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Optional
import multiprocessing
import time

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

def find_test_files(paths: List[str]) -> List[Path]:
    """
    Find all *.nf.test files in the specified paths.

    Args:
        paths: List of file paths or directories to search
    Returns:
        Sorted list of Path objects for all found test files
    """
    test_files = []
    for path_str in paths:
        path = Path(path_str)
        if not path.exists():
            logger.warning(f"Path does not exist: {path}")
            continue
        if path.is_file():
            if path.name.endswith('.nf.test'):
                test_files.append(path)
        elif path.is_dir():
            test_files.extend(path.rglob('*.nf.test'))
    return sorted(set(test_files))

def divide_test_files(test_files: List[Path], n_workers: int) -> List[List[Path]]:
    """
    Divide test files evenly among workers using round-robin distribution.
    Args:
        test_files: List of test file paths
        n_workers: Number of workers
    Returns:
        List of lists, where each inner list contains test files for one worker
    """
    if not test_files:
        return [[] for _ in range(n_workers)]
    worker_files = [[] for _ in range(n_workers)]
    for i, test_file in enumerate(test_files):
        worker_files[i % n_workers].append(test_file)
    return worker_files

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

def run_nf_test_worker(worker_id: int,
                       test_files: List[Path],
                       total_workers: int,
                       debug: bool) -> Tuple[int, int, str, str, str]:
    """
    Run nf-test on a specific set of test files.
    Args:
        worker_id: Worker number (1-indexed)
        test_files: List of test files for this worker to run
        total_workers: Total number of workers
        debug: Whether to run in debug mode
    Returns:
        Tuple of (worker_id, exit_code, stdout, stderr, command_str)
    """
    if not test_files:
        logger.info(f"Worker {worker_id}/{total_workers}: No test files assigned")
        return (worker_id, 0, "", "", "")
    logger.info(f"Worker {worker_id}/{total_workers}: Running {len(test_files)} test file(s)")

    cmd = ["nf-test", "test"]
    if debug:
        cmd.append("--debug")
        cmd.append("--verbose")
    cmd.extend([str(f) for f in test_files])
    cmd_str = " ".join(cmd)

    exit_code, stdout, stderr = execute_subprocess(cmd)
    stdout = strip_ansi_codes(stdout)
    stderr = strip_ansi_codes(stderr)
    if exit_code != 0:
        logger.error(f"Worker {worker_id}/{total_workers} failed with exit code {exit_code}")
    else:
        logger.info(f"Worker {worker_id}/{total_workers} completed successfully")
    return (worker_id, exit_code, stdout, stderr, cmd_str)

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

def write_test_log(all_workers: List[Tuple[int, int, str, str, str]],
                   failed_workers: List[Tuple[int, int, str, str, str]],
                   n_workers: int,
                   output_log: Path,
                   test_files: List[Path]) -> None:
    """
    Write comprehensive test log with all results and failure summary.

    Args:
        all_workers: List of (worker_id, exit_code, stdout, stderr, cmd_str) tuples for all workers
        failed_workers: List of (worker_id, exit_code, stdout, stderr, cmd_str) tuples for failed workers
        n_workers: Total number of workers
        output_log: Path to consolidated log file
        test_files: List of test files that were run
    """
    with open(output_log, 'w') as out:
        out.write("=" * 80 + "\n")
        out.write(f"NF-TEST PARALLEL RESULTS\n")
        out.write(f"Total workers: {n_workers}\n")
        out.write(f"Passed workers: {n_workers - len(failed_workers)}\n")
        out.write(f"Failed workers: {len(failed_workers)}\n")
        out.write(f"Working directory: {os.getcwd()}\n")
        out.write(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        out.write("=" * 80 + "\n\n")
        out.write("=" * 80 + "\n")
        out.write(f"TEST FILES ({len(test_files)} total)\n")
        out.write("=" * 80 + "\n\n")
        for test_file in test_files:
            out.write(f"{test_file}\n")
        out.write("\n")
        for worker_id, exit_code, stdout, stderr, cmd_str in sorted(all_workers):
            status = "PASSED" if exit_code == 0 else "FAILED"
            out.write("=" * 80 + "\n")
            out.write(f"Worker {worker_id}/{n_workers}: {status}\n")
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
        if failed_workers:
            out.write("=" * 80 + "\n")
            out.write("FAILURE SUMMARY\n")
            out.write("=" * 80 + "\n\n")
            for worker_id, exit_code, stdout, stderr, cmd_str in sorted(failed_workers):
                out.write(f"Worker {worker_id} (exit code: {exit_code}):\n")
                failures = extract_failures_from_output(stdout + "\n" + stderr)
                if failures:
                    for failure in failures:
                        out.write(f"    - {failure}\n")
                out.write("\n")
    logger.info(f"Test results written to: {output_log}")

def update_plugins() -> None:
    """
    Update nf-test plugins before running parallel tests.
    This prevents race conditions when multiple workers try to install plugins.
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

def run_parallel_tests(n_workers: int,
                       test_paths: List[str],
                       output_log: Path,
                       debug: bool) -> int:
    """
    Run nf-test in parallel by distributing test files across workers.
    Args:
        n_workers: Number of parallel workers to run
        test_paths: List of paths (files or directories) to search for tests
        output_log: Path to consolidated log file
        debug: Whether to run in debug mode (produces additional logging)
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info(f"Searching for test files in: {', '.join(test_paths)}")
    test_files = find_test_files(test_paths)
    if not test_files:
        raise RuntimeError("No test files found.")
    logger.info(f"Found {len(test_files)} test file(s); see {output_log} for details.")
    if n_workers > len(test_files):
        logger.warning(f"Number of workers ({n_workers}) exceeds number of test files ({len(test_files)}). Reducing to {len(test_files)} workers.")
        n_workers = len(test_files)
    update_plugins()
    worker_test_files = divide_test_files(test_files, n_workers)
    logger.info(f"Assigned test files to workers:")
    for i, files in enumerate(worker_test_files, 1):
        if files:
            logger.info(f"\t- Worker {i} assigned {len(files)} test file(s)")
    logger.info(f"Running {n_workers} workers in parallel...")
    worker_args = [(i, worker_test_files[i - 1], n_workers, debug) for i in range(1, n_workers + 1)]

    # Use chunksize=1 to ensure each worker gets its full assignment immediately
    # maxtasksperchild=1 ensures each worker process is fresh (no memory leaks)
    with multiprocessing.Pool(processes=n_workers) as pool:
        results = pool.starmap(run_nf_test_worker, worker_args)
    failed_workers = [(wid, code, out, err, cmd) for wid, code, out, err, cmd in results if code != 0]
    write_test_log(results, failed_workers, n_workers, output_log, test_files)
    if failed_workers:
        logger.error(f"{len(failed_workers)} worker(s) failed:")
        for worker_id, exit_code, _, _, _ in failed_workers:
            logger.error(f"\t- Worker {worker_id} failed with exit code {exit_code}")
        return 1
    else:
        logger.info(f"All {n_workers} workers completed successfully.")
        return 0

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "n_workers",
        type=int,
        help="Number of parallel workers to run (recommended: number of CPU cores)"
    )
    parser.add_argument(
        "test_paths",
        nargs="+",
        help="Paths to test files or directories containing tests"
    )
    parser.add_argument(
        "--output-log",
        type=Path,
        default=Path("test-logs.txt"),
        help="Path to consolidated log file (default: test-logs.txt)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run in debug mode (produces additional logging)"
    )
    args = parser.parse_args()
    if args.n_workers < 1:
        parser.error("n_workers must be at least 1")
    return args

def main() -> None:
    """Main entry point."""
    logger.info(f"Initializing script.")
    start_time = time.time()
    args = parse_arguments()
    logger.info(f"Arguments: n_workers={args.n_workers}, test_paths={args.test_paths}, output_log={args.output_log}")
    repo_root = Path(__file__).resolve().parent.parent
    original_cwd = os.getcwd()
    try:
        os.chdir(repo_root)
        exit_code = run_parallel_tests(
            n_workers=args.n_workers,
            test_paths=args.test_paths,
            output_log=args.output_log,
            debug=args.debug,
        )
        exit(exit_code)
    finally:
        os.chdir(original_cwd)
        end_time = time.time()
        logger.info(f"Total time elapsed: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
