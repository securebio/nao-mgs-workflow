#!/usr/bin/env python3
"""
Build a container using Seqera Wave API and update containers.config.
"""
import argparse
import base64
import json
import re
import sys
from pathlib import Path
import requests
import yaml


def read_container_spec(spec_file: Path) -> dict:
    """Read container specification from YAML file."""
    with open(spec_file) as f:
        return yaml.safe_load(f)


def build_conda_yaml(spec: dict) -> str:
    """Generate conda environment YAML from spec."""
    conda_yaml = {
        "channels": spec.get("channels", ["conda-forge", "bioconda"]),
        "dependencies": spec["dependencies"]
    }
    return yaml.dump(conda_yaml)


def build_wave_container(spec: dict) -> str:
    """Call Wave API to build container and return the container URL."""
    wave_api = "https://wave.seqera.io/v1alpha2/container"
    deps = spec["dependencies"]
    has_pip = any(isinstance(dep, dict) and "pip" in dep for dep in deps)
    if has_pip:
        conda_yaml = yaml.dump({
            "channels": spec.get("channels", ["conda-forge", "bioconda"]),
            "dependencies": deps
        })
        conda_yaml_b64 = base64.b64encode(conda_yaml.encode()).decode()
        payload = {
            "packages": {
                "type": "CONDA",
                "environment": conda_yaml_b64,
                "channels": spec.get("channels", ["conda-forge", "bioconda"])
            },
            "freeze": True
        }
    else:
        payload = {
            "packages": {
                "type": "CONDA",
                "entries": deps,
                "channels": spec.get("channels", ["conda-forge", "bioconda"])
            },
            "freeze": True
        }
    response = requests.post(wave_api, json=payload)
    if not response.ok:
        print(f"Error response: {response.status_code}", file=sys.stderr)
        print(f"Response body: {response.text}", file=sys.stderr)
    response.raise_for_status()
    data = response.json()
    return data["targetImage"]


def update_containers_config(config_file: Path, label: str, container_url: str):
    """Update the container URL for a specific label in containers.config."""
    content = config_file.read_text()
    pattern = rf'(withLabel:\s+{re.escape(label)}\s+{{\s*container\s*=\s*")[^"]+(")'
    match = re.search(pattern, content, flags=re.DOTALL)
    if not match:
        print(f"Error: Label '{label}' not found in {config_file}", file=sys.stderr)
        return False
    old_url = match.group(0).split('"')[1]
    if old_url == container_url:
        print(f"No change needed: container URL already matches")
        return True
    new_content = re.sub(pattern, rf'\g<1>{container_url}\g<2>', content, flags=re.DOTALL)
    config_file.write_text(new_content)
    print(f"Updated {config_file}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Build Wave container and update config")
    parser.add_argument("spec_file", help="Path to container spec YAML file")
    parser.add_argument("--config", default="configs/containers.config", help="Path to containers.config")
    parser.add_argument("--dry-run", action="store_true", help="Don't update config file")
    args = parser.parse_args()
    spec_file = Path(args.spec_file)
    config_file = Path(args.config)
    if not spec_file.exists():
        print(f"Error: Spec file {spec_file} not found", file=sys.stderr)
        sys.exit(1)
    spec = read_container_spec(spec_file)
    label = spec.get("label")
    if not label:
        print("Error: 'label' field required in spec file", file=sys.stderr)
        sys.exit(1)
    print(f"Building container for {label}...")
    container_url = build_wave_container(spec)
    print(f"Container built: {container_url}")
    if not args.dry_run:
        if update_containers_config(config_file, label, container_url):
            sys.exit(0)
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
