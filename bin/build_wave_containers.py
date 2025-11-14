#!/usr/bin/env python3
"""
Rebuild all Wave containers from specs in containers/ directory.
"""
import argparse
import sys
from pathlib import Path

# Import the build function from the single container builder
from build_wave_container import build_container_from_spec


def main():
    parser = argparse.ArgumentParser(description="Rebuild all Wave containers")
    parser.add_argument("--containers-dir", default="containers", help="Directory containing container specs")
    parser.add_argument("--config", default="configs/containers.config", help="Path to containers.config")
    parser.add_argument("--dry-run", action="store_true", help="Don't update config file")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue if a container build fails")
    args = parser.parse_args()
    containers_dir = Path(args.containers_dir)
    if not containers_dir.exists():
        print(f"Error: Containers directory {containers_dir} not found", file=sys.stderr)
        sys.exit(1)
    spec_files = sorted(containers_dir.glob("*.yml"))
    if not spec_files:
        print(f"Error: No .yml files found in {containers_dir}", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(spec_files)} container specs")
    print()
    failed = []
    config_file = Path(args.config)
    for spec_file in spec_files:
        print(f"Processing {spec_file.name}...")
        success = build_container_from_spec(spec_file, config_file, args.dry_run)
        if not success:
            failed.append(spec_file.name)
            if not args.continue_on_error:
                print(f"\nError: Failed to build {spec_file.name}", file=sys.stderr)
                sys.exit(1)
        print()
    if failed:
        print(f"\nFailed to build {len(failed)} containers:")
        for name in failed:
            print(f"  - {name}")
        sys.exit(1)
    else:
        print(f"\nSuccessfully processed all {len(spec_files)} containers")


if __name__ == "__main__":
    main()
