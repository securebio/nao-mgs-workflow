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
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import csv
import boto3
from botocore.exceptions import ClientError

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

def resolve_samplesheet_path(samplesheet: str, run_launch_dir: Path, repo_root: Path) -> Path:
    """
    Resolve samplesheet path, downloading from S3 if necessary.

    Args:
        samplesheet: Path to samplesheet (local or S3)
        run_launch_dir: RUN workflow launch directory
        repo_root: Repository root directory

    Returns:
        Path to local samplesheet file
    """
    if samplesheet.startswith("s3://"):
        # Download from S3 to RUN launch directory
        local_path = run_launch_dir / "samplesheet.csv"
        logger.info(f"Downloading samplesheet from {samplesheet} to {local_path}")

        # Parse S3 URI
        s3_parts = samplesheet.replace("s3://", "").split("/", 1)
        bucket = s3_parts[0]
        key = s3_parts[1]

        # Download using boto3
        s3_client = boto3.client('s3')
        try:
            s3_client.download_file(bucket, key, str(local_path))
        except ClientError as e:
            raise RuntimeError(f"Failed to download samplesheet from S3: {e}")

        return local_path
    else:
        # Local path - resolve relative to repo root
        path = Path(samplesheet)
        if not path.is_absolute():
            path = repo_root / path
        return path

def generate_downstream_input(downstream_launch_dir: Path,
                              samplesheet_path: Path,
                              run_results_dir: str,
                              run_id: str) -> Path:
    """
    Generate input files for DOWNSTREAM workflow, with each sample as its own group
    Args:
        downstream_launch_dir: Launch directory for downstream workflow
        samplesheet_path: Path to samplesheet CSV
        run_results_dir: S3 path to RUN workflow output directory
        run_id: Run ID to use for the DOWNSTREAM workflow input file
    Returns:
        Path to generated input.csv file
    """
    input_csv_path = downstream_launch_dir / "input.csv"
    groups_tsv_path = downstream_launch_dir / "groups.tsv"
    hits_tsv_path = f"{run_results_dir}/virus_hits_final.tsv.gz"
    with open(samplesheet_path, 'r') as inf, open(groups_tsv_path, 'w') as outf:
        writer = csv.writer(outf, delimiter='\t')
        writer.writerow(['sample', 'group'])
        reader = csv.DictReader(inf)
        for row in reader:
            writer.writerow([row['sample'], row['sample']])
    with open(input_csv_path, 'w') as inf:
        writer = csv.writer(inf)
        writer.writerow(['label', 'hits_tsv', 'groups_tsv'])
        writer.writerow([run_id, hits_tsv_path, str(groups_tsv_path.resolve())])
    logger.info(f"Generated DOWNSTREAM input file: {input_csv_path}")
    return input_csv_path

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
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created launch directory for {name}: {path}")
        else:
            logger.info(f"Using existing launch directory for {name}: {path}")
    return launch_dirs

def execute_nextflow(launch_dir: Path,
                     repo_root: Path,
                     config_file: Path,
                     params: dict,
                     workflow_name: str,
                     profile: str,
                     resume: bool) -> None:
    """
    Execute Nextflow from specified launch directory with given config and parameters.
    Args:
        launch_dir: Directory to run Nextflow from
        repo_root: Path to repository root
        config_file: Path to Nextflow config file
        params: Dictionary of parameters to override
        workflow_name: Name of workflow for logging
        profile: Nextflow profile to use
        resume: Whether to run Nextflow with the -resume flag
    """
    logger.info("=" * 80)
    logger.info(f"Starting {workflow_name} workflow")
    logger.info("=" * 80)
    cmd = ["nextflow", "run", str(repo_root)]
    if resume:
        cmd.append("-resume")
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
        help="S3 base directory for all workflow outputs (default: s3://nao-testing/mgs-workflow-test)"
    )
    parser.add_argument(
        "--samplesheet",
        type=str,
        default="test-data/samplesheet.csv",
        help="Path to samplesheet for RUN workflow (local or S3, default: test-data/samplesheet.csv)"
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="test_run",
        help="Nextflow profile to use (default: test_run)"
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not resume from the last completed workflow (default: False)"
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default="test_run",
        help="Run ID to use for the DOWNSTREAM workflow input file (default: test_run)"
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Skip the INDEX workflow"
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Skip the RUN workflow"
    )
    parser.add_argument(
        "--skip-downstream",
        action="store_true",
        help="Skip the DOWNSTREAM workflow"
    )
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    logger.info("Initializing sequential workflow execution")
    args = parse_arguments()
    logger.info(f"Parsed arguments: {args}")
    repo_root = Path(__file__).resolve().parent.parent
    logger.info(f"Repository root: {repo_root}")
    launch_dirs = create_launch_directories(args.launch_dir)

    # INDEX
    index_base_dir = args.base_dir.rstrip("/") + "/index"
    if not args.skip_index:
        execute_nextflow(
            launch_dir=launch_dirs['index'],
            repo_root=repo_root,
            config_file=repo_root / "configs" / "index-for-run-test.config",
            params={"base_dir": index_base_dir},
            workflow_name="INDEX",
            profile=args.profile,
            resume=not args.no_resume
        )
    else:
        logger.info("Skipping INDEX workflow")

    # RUN
    run_base_dir = args.base_dir.rstrip("/") + "/run"
    ref_dir = f"{index_base_dir}/output"
    samplesheet_path = resolve_samplesheet_path(args.samplesheet, launch_dirs['run'], repo_root)
    if not args.skip_run:
        execute_nextflow(
            launch_dir=launch_dirs['run'],
            repo_root=repo_root,
            config_file=repo_root / "configs" / "run.config",
            params={
                "base_dir": run_base_dir,
                "ref_dir": ref_dir,
                "platform": "illumina",
                "sample_sheet": str(samplesheet_path.resolve())
            },
            workflow_name="RUN",
            profile=args.profile,
            resume=args.resume
        )
    else:
        logger.info("Skipping RUN workflow")

    # DOWNSTREAM
    downstream_base_dir = args.base_dir.rstrip("/") + "/downstream"
    run_results_dir = f"{run_base_dir}/output/results"
    if not args.skip_downstream:
        downstream_input_path = generate_downstream_input(
            downstream_launch_dir=launch_dirs['downstream'],
            samplesheet_path=samplesheet_path,
            run_results_dir=run_results_dir,
            run_id=args.run_id
        )
        execute_nextflow(
            launch_dir=launch_dirs['downstream'],
            repo_root=repo_root,
            config_file=repo_root / "configs" / "downstream.config",
            params={
                "base_dir": downstream_base_dir,
                "ref_dir": ref_dir,
                "platform": "illumina",
                "input_file": str(downstream_input_path.resolve())
            },
            workflow_name="DOWNSTREAM",
            profile=args.profile,
            resume=args.resume
        )
    else:
        logger.info("Skipping DOWNSTREAM workflow")

if __name__ == "__main__":
    main()
