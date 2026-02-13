#!/usr/bin/env python3
"""
Scan container images from Nextflow config with Trivy.
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


def extract_containers(config_file: Path) -> set[str]:
    """Extract unique container addresses from Nextflow config file.

    Skips containers with Nextflow interpolation (${...}) since those
    are dynamically versioned and may not exist in ECR at scan time.
    """
    pattern = re.compile(r'container\s*=\s*"([^"]+)"')
    containers = set()
    with open(config_file) as f:
        for line in f:
            match = pattern.search(line)
            if match:
                container = match.group(1)
                if "${" in container:
                    print(f"Skipping container with dynamic tag: {container}")
                    continue
                containers.add(container)
    return containers


def scan_container(container: str, output_dir: Path) -> Path:
    """Run Trivy scan on a single container and return the output file path."""
    container_safe = container.replace("/", "_").replace(":", "_")
    output_file = output_dir / f"{container_safe}.json"
    print(f"Scanning {container}...")
    subprocess.run(
        ["trivy", "image", "--scanners", "vuln", "--format", "json", "--output", str(output_file), container],
        check=True,
    )
    return output_file


def aggregate_results(output_dir: Path) -> tuple[dict, dict, bool]:
    """
    Aggregate scan results and check for critical/high vulnerabilities.
    Returns a tuple of (summary dict, total counts dict, has_critical_or_high).
    """
    summary = {"containers": []}
    total_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "UNKNOWN": 0}
    has_critical_or_high = False
    for result_file in sorted(output_dir.glob("*.json")):
        with open(result_file) as f:
            data = json.load(f)
        container_name = data.get("ArtifactName", "unknown")
        metadata = data.get("Metadata", {})
        os_info = metadata.get("OS", {})
        os_family = os_info.get("Family", "unknown")
        os_name = os_info.get("Name", "unknown")
        severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "UNKNOWN": 0}
        for result in data.get("Results", []):
            for vuln in result.get("Vulnerabilities", []):
                severity = vuln.get("Severity", "UNKNOWN")
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
                total_counts[severity] = total_counts.get(severity, 0) + 1
        summary["containers"].append({
            "name": container_name,
            "os": {
                "family": os_family,
                "name": os_name
            },
            "vulnerabilities": severity_counts
        })
        if severity_counts["CRITICAL"] > 0 or severity_counts["HIGH"] > 0:
            has_critical_or_high = True
    summary_file = output_dir / "summary.json"
    summary_file.write_text(json.dumps(summary, indent=2))
    return summary, total_counts, has_critical_or_high


def main():
    parser = argparse.ArgumentParser(description="Scan container images with Trivy")
    parser.add_argument("--config", default="configs/containers.config", help="Path to Nextflow config file")
    parser.add_argument("--output-dir", default="trivy_results", help="Directory for scan results")
    args = parser.parse_args()
    config_file = Path(args.config)
    output_dir = Path(args.output_dir)
    if not config_file.exists():
        print(f"Error: Config file {config_file} not found", file=sys.stderr)
        sys.exit(1)
    output_dir.mkdir(parents=True, exist_ok=True)
    containers = extract_containers(config_file)
    for container in containers:
        scan_container(container, output_dir)
    summary, total_counts, has_critical_or_high = aggregate_results(output_dir)
    print("\nAggregate vulnerability counts across all containers:")
    for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "UNKNOWN"]:
        print(f"  {severity}: {total_counts[severity]}")
    if has_critical_or_high:
        print("\n✗ Found CRITICAL or HIGH severity vulnerabilities")
        sys.exit(1)
    else:
        print("\n✓ No CRITICAL or HIGH severity vulnerabilities found")
        sys.exit(0)


if __name__ == "__main__":
    main()
