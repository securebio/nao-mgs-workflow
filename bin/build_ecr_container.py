#!/usr/bin/env python3
DESC = """
Build a container from a YAML spec and push to AWS ECR.
Creates the ECR repository if it doesn't exist.
"""

###########
# IMPORTS #
###########

# Standard library imports
import argparse
import hashlib
import json
import logging
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Third-party imports
import boto3
import yaml
from botocore.exceptions import ClientError

###########
# LOGGING #
###########

class UTCFormatter(logging.Formatter):
    """
    Custom logging formatter that displays timestamps in UTC.
    Returns:
        Formatted log timestamps in UTC timezone
    """

    def formatTime(
        self, record: logging.LogRecord, datefmt: str | None = None
    ) -> str:
        """
        Format log timestamps in UTC timezone.
        Args:
            record: LogRecord object containing timestamp data
            datefmt: Optional date format string (unused)
        Returns:
            Formatted timestamp string in UTC
        """
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

##########################
# SPEC PARSING FUNCTIONS #
##########################

def read_container_spec(spec_file: Path) -> dict[str, Any]:
    """
    Read and validate container specification from YAML file.
    Args:
        spec_file (Path): Path to container spec YAML file
    Returns:
        dict[str, Any]: Container specification
    """
    with open(spec_file) as f:
        spec = yaml.safe_load(f)
    required_fields = ["name", "label", "channels", "dependencies"]
    missing = [field for field in required_fields if field not in spec]
    if missing:
        msg = f"Container spec missing required fields: {', '.join(missing)}"
        logger.error(msg)
        raise ValueError(msg)
    return spec

def compute_spec_hash(spec: dict[str, Any]) -> str:
    """
    Compute a hash of the container specification for tagging.
    Args:
        spec (dict[str, Any]): Container specification
    Returns:
        str: Hash of the container specification
    """
    content_str = json.dumps(spec, sort_keys=True)
    hash_obj = hashlib.sha256(content_str.encode())
    return hash_obj.hexdigest()[:16]

#######################
# ECR SETUP FUNCTIONS #
#######################

def setup_ecr_repository(
    label: str,
    prefix: str,
    region: str,
    spec_hash: str,
) -> tuple[str, str, str]:
    """
    Ensure ECR repository exists and return image tags.
    Args:
        label (str): Container label
        prefix (str): Repository name prefix
        region (str): AWS region
        spec_hash (str): Hash of the spec for tagging
    Returns:
        tuple[str, str, str]: Tuple of (image_tag, image_tag_latest, registry_url)
    """
    logger.info("Setting up ECR repository")
    repo_name = f"{prefix}/{label.lower()}"
    ecr_client = boto3.client("ecr", region_name=region)
    try:
        response = ecr_client.describe_repositories(repositoryNames=[repo_name])
        repo = response["repositories"][0]
        logger.info(f"ECR repository exists: {repo['repositoryUri']}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "RepositoryNotFoundException":
            logger.info("ECR repository does not exist; creating")
            try:
                response = ecr_client.create_repository(
                    repositoryName=repo_name,
                    imageScanningConfiguration={"scanOnPush": True},
                    encryptionConfiguration={"encryptionType": "AES256"},
                )
                repo = response["repository"]
                logger.info(f"Created repository: {repo['repositoryUri']}")
            except ClientError as create_error:
                logger.error(f"Error creating repository: {create_error}")
                raise
        else:
            logger.error(f"Error checking repository: {e}")
            raise
    repo_uri = repo["repositoryUri"]
    registry_url = repo_uri.split("/")[0]
    image_tag = f"{repo_uri}:{spec_hash}"
    image_tag_latest = f"{repo_uri}:latest"
    return image_tag, image_tag_latest, registry_url

################################
# LOCAL DOCKER BUILD FUNCTIONS #
################################

def generate_dockerfile(spec_filename: str) -> str:
    """Generate a Dockerfile that uses micromamba with a YAML environment file.
    Args:
        spec_filename (str): Name of the spec file
    Returns:
        str: Dockerfile text
    """
    dockerfile = f"""
FROM mambaorg/micromamba:1.5.10
USER root
RUN mkdir -p /opt/conda
COPY {spec_filename} /tmp/environment.yml
RUN micromamba install -y -n base -f /tmp/environment.yml && \\
    micromamba clean --all --yes
ENV PATH=/opt/conda/bin:$PATH
USER mambauser
"""
    return dockerfile

def build_docker_image_from_spec(
    spec_file: Path,
    image_tag: str,
    build_dir: Path,
) -> None:
    """Build a Docker image from the spec file in a given directory.
    Args:
        spec_file (Path): Path to container spec YAML file
        image_tag (str): Image tag
        build_dir (Path): Path to build directory
    """
    spec_filename = spec_file.name
    shutil.copy(spec_file, build_dir / spec_filename)
    dockerfile_path = build_dir / "Dockerfile"
    dockerfile_path.write_text(generate_dockerfile(spec_filename))
    logger.info(f"Building Docker image: {image_tag}")
    try:
        subprocess.run(
            ["docker", "build", "-t", image_tag, str(build_dir)],
            check=True,
        )
        logger.info(f"Built image: {image_tag}")
    except subprocess.CalledProcessError as e:
        msg = f"Error building Docker image: {e}"
        logger.error(msg)
        raise RuntimeError(msg)

def tag_docker_image(source_tag: str, target_tag: str) -> None:
    """Tag a Docker image with an additional tag.
    Args:
        source_tag (str): Source image tag
        target_tag (str): Target image tag
    """
    try:
        subprocess.run(
            ["docker", "tag", source_tag, target_tag],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        msg = f"Failed to tag {target_tag}: {e}"
        logger.error(msg)
        raise RuntimeError(msg)

def build_container(spec_file: Path,
    image_tag: str,
    image_tag_latest: str,
) -> None:
    """
    Build a Docker container from a spec file.
    Args:
        spec_file: Path to container spec YAML file
        image_tag: Primary image tag (with hash)
        image_tag_latest: Latest tag for the image
    """
    logger.info("Building container locally")
    with tempfile.TemporaryDirectory() as tmpdir:
        build_dir = Path(tmpdir)
        try:
            build_docker_image_from_spec(spec_file, image_tag, build_dir)
            tag_docker_image(image_tag, image_tag_latest)
        except Exception as e:
            msg = f"Error building container: {e}"
            logger.error(msg)
            raise RuntimeError(msg)

######################
# ECR PUSH FUNCTIONS #
######################

def docker_login_ecr(registry_url: str, region: str) -> None:
    """Authenticate Docker with ECR.
    Args:
        registry_url (str): ECR registry URL
        region (str): AWS region
    """
    try:
        # Get login password
        result = subprocess.run(
            ["aws", "ecr", "get-login-password", "--region", region],
            capture_output=True,
            text=True,
            check=True,
        )
        password = result.stdout.strip()
        # Docker login
        subprocess.run(
            ["docker", "login", "--username", "AWS", "--password-stdin", registry_url],
            input=password,
            text=True,
            check=True,
            capture_output=True,
        )
        logger.info("Authenticated with ECR")
    except subprocess.CalledProcessError as e:
        msg = f"Error authenticating with ECR: {e}"
        logger.error(msg)
        raise RuntimeError(msg)

def push_docker_image(image_tag: str) -> None:
    """Push a Docker image to ECR.
    Args:
        image_tag (str): Image tag
    """
    logger.info(f"Pushing image to ECR: {image_tag}")
    try:
        subprocess.run(
            ["docker", "push", image_tag],
            check=True,
        )
        logger.info(f"Pushed image: {image_tag}")
    except subprocess.CalledProcessError as e:
        msg = f"Error pushing Docker image: {e}"
        logger.error(msg)
        raise RuntimeError(msg)

def push_to_ecr(
    image_tag: str,
    image_tag_latest: str,
    registry_url: str,
    region: str,
) -> None:
    """
    Authenticate with ECR and push container images.
    Args:
        image_tag (str): Primary image tag (with hash)
        image_tag_latest (str): Latest tag for the image
        registry_url (str): ECR registry URL
        region (str): AWS region
    """
    try:
        docker_login_ecr(registry_url, region)
        push_docker_image(image_tag)
        push_docker_image(image_tag_latest)
    except Exception as e:
        msg = f"Error pushing to ECR: {e}"
        logger.error(msg)
        raise RuntimeError(msg)

###########################
# CONFIG UPDATE FUNCTIONS #
###########################

def update_containers_config(
    config_file: Path,
    label: str,
    container_url: str,
) -> None:
    """Update the container URL for a specific label in containers.config.
    Args:
        config_file (Path): Path to containers.config file
        label (str): Container label
        container_url (str): Container URL
    """
    logger.info("Updating config")
    content = config_file.read_text()
    pattern = rf'(withLabel:\s+{re.escape(label)}\s+\{{(?:[^}}]|\n)*?container\s*=\s*")[^"]+(")'
    match = re.search(pattern, content, flags=re.DOTALL)
    if not match:
        msg = f"Label '{label}' not found in {config_file}"
        logger.error(msg)
        raise ValueError(msg)
    old_url = match.group(0).split('"')[1]
    if old_url == container_url:
        logger.info("Config already up to date")
    new_content = re.sub(
        pattern, rf"\g<1>{container_url}\g<2>", content, flags=re.DOTALL
    )
    config_file.write_text(new_content)
    logger.info(f"Updated {config_file}")

######################
# MAIN ORCHESTRATION #
######################

def build_and_push_container(
    spec_file: Path,
    region: str,
    prefix: str,
    config_file: Path,
) -> None:
    """
    Build a container from a spec file and push to ECR.
    Args:
        spec_file (Path): Path to container spec YAML file
        region (str): AWS region for ECR
        prefix (str): Repository name prefix
        config_file (Path): Path to containers.config file
    """
    if not spec_file.exists():
        msg = f"Spec file {spec_file} not found"
        logger.error(msg)
        raise FileNotFoundError(msg)
    logger.info(f"Building container from: {spec_file}")
    try:
        spec = read_container_spec(spec_file)
        label = spec["label"]
        spec_hash = compute_spec_hash(spec)
        logger.info(f"Container: {label}, Hash: {spec_hash}")
        image_tag, image_tag_latest, registry_url = setup_ecr_repository(
            label, prefix, region, spec_hash
        )
        build_container(spec_file, image_tag, image_tag_latest)
        push_to_ecr(image_tag, image_tag_latest, registry_url, region)
        update_containers_config(config_file, label, image_tag)
        logger.info(f"Successfully built and pushed: {label}")
        logger.info(f"  Image: {image_tag}")
        logger.info(f"  Config updated: {config_file}")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

###################
# CLI ENTRY POINT #
###################

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument(
        "spec_file", type=Path, help="Path to container spec YAML file"
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region for ECR (default: us-east-1)",
    )
    parser.add_argument(
        "--prefix",
        default="nao-mgs-workflow",
        help="Repository name prefix (default: nao-mgs-workflow)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/containers.config"),
        help="Path to containers.config (default: configs/containers.config)",
    )
    args = parser.parse_args()
    return args

def main() -> None:
    args = parse_args()
    build_and_push_container(args.spec_file, args.region, args.prefix, args.config)

if __name__ == "__main__":
    main()
