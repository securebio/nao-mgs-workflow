#!/usr/bin/env python3
"""
Rebuild all ECR containers from specs in containers/ directory.
"""
import argparse
import logging
import sys
from pathlib import Path

# Import the build function from the single container builder
from build_ecr_container import build_and_push_container, logger


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild all ECR containers")
    parser.add_argument(
        "--containers-dir",
        default="containers",
        help="Directory containing container specs (default: containers)",
    )
    parser.add_argument(
        "--config",
        default="configs/containers.config",
        help="Path to containers.config (default: configs/containers.config)",
    )
    parser.add_argument(
        "--prefix",
        default="nao-mgs-workflow",
        help="Repository name prefix (default: nao-mgs-workflow)",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue if a container build fails",
    )
    args = parser.parse_args()

    containers_dir = Path(args.containers_dir)
    if not containers_dir.exists():
        logger.error(f"Containers directory {containers_dir} not found")
        sys.exit(1)

    spec_files = sorted(containers_dir.glob("*.yml"))
    if not spec_files:
        logger.error(f"No .yml files found in {containers_dir}")
        sys.exit(1)

    logger.info(f"Found {len(spec_files)} container specs")
    logger.info("")

    failed = []
    config_file = Path(args.config)

    for i, spec_file in enumerate(spec_files, 1):
        logger.info(f"[{i}/{len(spec_files)}] Processing {spec_file.name}...")
        try:
            build_and_push_container(
                spec_file, args.prefix, config_file
            )
            logger.info("")
        except Exception as e:
            failed.append(spec_file.name)
            logger.error(f"Failed to build {spec_file.name}: {e}")
            if not args.continue_on_error:
                logger.error(f"\nStopping due to error")
                sys.exit(1)
            logger.info("")

    logger.info("=" * 60)
    if failed:
        logger.error(f"Failed to build {len(failed)} containers:")
        for name in failed:
            logger.error(f"  - {name}")
        sys.exit(1)
    else:
        logger.info(f"Successfully processed all {len(spec_files)} containers")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
