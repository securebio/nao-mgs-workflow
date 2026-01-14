#!/usr/bin/env python3
DESC = """
Execute INDEX, RUN, and DOWNSTREAM workflows in sequence.

This script orchestrates the three main workflows, passing outputs from each
stage as inputs to the next via S3 paths.
"""

###########
# IMPORTS #
###########

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

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

def create_launch_directories(base_launch_dir: Path) -> dict:
    """
    Create launch directory structure with subdirectories for each workflow.
    Args:
        base_launch_dir: Base directory for all workflow launches
    Returns:
        Dictionary mapping workflow names to their launch directory paths
    """
    launch_dirs = {
        'index': base_launch_dir / 'index',
        'run': base_launch_dir / 'run',
        'downstream': base_launch_dir / 'downstream'
    }
    for name, path in launch_dirs.items():
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created launch directory for {name}: {path}")
    return launch_dirs

def execute_nextflow(launch_dir: Path,
                     repo_root: Path,
                     config_file: Path,
                     params: dict,
                     workflow_name: str,
                     profile: str) -> None:
    """
    Execute Nextflow from specified launch directory with given config and parameters.
    Args:
        launch_dir: Directory to run Nextflow from
        repo_root: Path to repository root
        config_file: Path to Nextflow config file
        params: Dictionary of parameters to override
        workflow_name: Name of workflow for logging
        profile: Nextflow profile to use
    """
    logger.info("=" * 80)
    logger.info(f"Starting {workflow_name} workflow")
    logger.info("=" * 80)
    cmd = ["nextflow", "run", str(repo_root)]
    cmd.extend(["-c", str(config_file)])
    cmd.extend(["-profile", profile])
    for key, value in params.items():
        cmd.append(f"--{key}={value}")
    logger.info(f"Executing Nextflow from {launch_dir}")
    logger.info(f"Command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=launch_dir)
    if result.returncode != 0:
        msg = f"{workflow_name} workflow failed with exit code {result.returncode}"
        logger.error(msg)
        raise RuntimeError(msg)
    logger.info(f"{workflow_name} workflow completed successfully")

##############
# MAIN LOGIC #
##############

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--launch-dir",
        type=Path,
        default=Path("test_run"),
        help="Base launch directory (will create subdirectories for each workflow)"
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default="s3://nao-testing/mgs-workflow-test",
        help="S3 base directory for INDEX workflow outputs (default: s3://nao-testing/mgs-workflow-test)"
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="test_run",
        help="Nextflow profile to use (default: test_run)"
    )
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    logger.info("Initializing sequential workflow execution")
    args = parse_arguments()
    repo_root = Path(__file__).resolve().parent.parent
    launch_dirs = create_launch_directories(args.launch_dir)
    execute_nextflow(
        launch_dir=launch_dirs['index'],
        repo_root=repo_root,
        config_file=repo_root / "configs" / "index-for-run-test.config",
        params={"base_dir": args.base_dir.rstrip("/") + "/index"},
        workflow_name="INDEX",
        profile=args.profile
    )

if __name__ == "__main__":
    main()
