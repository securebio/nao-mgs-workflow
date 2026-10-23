#!/usr/bin/env python3
DESC = """
Build the rust-tools container, push it to ECR Public under a tag derived from
its sources, and pin that tag in containers.config. With --check, only verify
that the pinned tag matches the current sources.
"""

###########
# IMPORTS #
###########

import argparse
import hashlib
import logging
import re
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from build_ecr_container import (
    docker_login_ecr,
    push_docker_image,
    setup_ecr_repository,
    update_containers_config,
)

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#############
# CONSTANTS #
#############

DOCKERFILE = "docker/nao-rust-tools.Dockerfile"
# Everything the image is built from: the Dockerfile and the paths it COPYs.
SOURCE_PATHS = ["rust-tools", DOCKERFILE]
CONTAINER_LABEL = "rust_tools"
REPO_NAME = "rust-tools"

##################
# SOURCE HASHING #
##################


def read_sources(repo_root: Path) -> dict[str, bytes]:
    """Read the tracked and untracked, non-ignored files under SOURCE_PATHS.
    Reading once lets the hash and the build use the same snapshot.
    Args:
        repo_root (Path): Repository root
    Returns:
        dict[str, bytes]: File contents keyed by repo-relative path, in sorted order
    """
    result = subprocess.run(
        ["git", "ls-files", "-z", "-co", "--exclude-standard", "--", *SOURCE_PATHS],
        cwd=repo_root,
        capture_output=True,
        check=True,
    )
    sources = {}
    for path in sorted({p for p in result.stdout.decode().split("\0") if p}):
        file = repo_root / path
        if file.is_symlink():
            raise ValueError(f"Symlinks aren't supported in rust-tools sources: {path}")
        # Tracked files deleted from the working tree are still listed
        if file.is_file():
            sources[path] = file.read_bytes()
    return sources


def compute_source_hash(sources: dict[str, bytes]) -> str:
    """Hash the paths and contents of the rust-tools sources.
    Args:
        sources (dict[str, bytes]): Output of read_sources
    Returns:
        str: First 16 hex characters of the SHA-256 digest
    """
    digest = hashlib.sha256()
    for path, content in sources.items():
        digest.update(f"{path}\0{hashlib.sha256(content).hexdigest()}\n".encode())
    return digest.hexdigest()[:16]


##################
# CONFIG PINNING #
##################


def read_pinned_tag(config_file: Path) -> str:
    """Read the image tag pinned for the rust_tools label.
    Args:
        config_file (Path): Path to containers.config
    Returns:
        str: Tag after the last ':' of the pinned image
    """
    pattern = rf'withLabel:\s+{CONTAINER_LABEL}\s+\{{[^}}]*?container\s*=\s*"([^"]+)"'
    match = re.search(pattern, config_file.read_text())
    if not match:
        raise ValueError(f"Label '{CONTAINER_LABEL}' not found in {config_file}")
    return match.group(1).rsplit(":", 1)[-1]


def check_pin(repo_root: Path, config_file: Path) -> None:
    """Raise if the pinned tag doesn't match the current sources.
    Args:
        repo_root (Path): Repository root
        config_file (Path): Path to containers.config
    """
    expected = compute_source_hash(read_sources(repo_root))
    pinned = read_pinned_tag(config_file)
    if pinned != expected:
        raise ValueError(
            f"rust-tools is pinned to '{pinned}', but its sources hash to '{expected}'. "
            "Run bin/build_rust_tools_container.py to build, push and pin the image."
        )
    logger.info(f"rust-tools pin matches sources: {pinned}")


############
# BUILDING #
############


def build_image(sources: dict[str, bytes], image_tag: str) -> None:
    """Build the image from a context containing only the hashed files.
    Args:
        sources (dict[str, bytes]): Output of read_sources
        image_tag (str): Full image reference to tag the build with
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        context = Path(tmpdir)
        for path, content in sources.items():
            (context / path).parent.mkdir(parents=True, exist_ok=True)
            (context / path).write_bytes(content)
        logger.info(f"Building {image_tag}")
        subprocess.run(
            [
                "docker",
                "build",
                "--platform",
                "linux/amd64",
                "-f",
                str(context / DOCKERFILE),
                "-t",
                image_tag,
                str(context),
            ],
            check=True,
        )


def build_push_and_pin(repo_root: Path, config_file: Path, prefix: str) -> None:
    """Build and push the image unless it exists, then pin it.
    Args:
        repo_root (Path): Repository root
        config_file (Path): Path to containers.config
        prefix (str): ECR repository name prefix
    """
    sources = read_sources(repo_root)
    source_hash = compute_source_hash(sources)
    logger.info(f"rust-tools source hash: {source_hash}")
    image_tag, _, registry_url, image_exists = setup_ecr_repository(
        REPO_NAME, prefix, source_hash
    )
    if image_exists:
        logger.info("Image already exists in ECR; skipping build")
    else:
        build_image(sources, image_tag)
        docker_login_ecr(registry_url)
        push_docker_image(image_tag)
    update_containers_config(config_file, CONTAINER_LABEL, image_tag)


###################
# CLI ENTRY POINT #
###################


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only check that the pinned tag matches the sources",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Repository root (default: parent of bin/)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to containers.config (default: <repo-root>/configs/containers.config)",
    )
    parser.add_argument(
        "--prefix",
        default="nao-mgs-workflow",
        help="ECR repository name prefix (default: nao-mgs-workflow)",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the script."""
    start = time.time()
    args = parse_arguments()
    config_file = args.config or args.repo_root / "configs" / "containers.config"
    if args.check:
        check_pin(args.repo_root, config_file)
    else:
        build_push_and_pin(args.repo_root, config_file, args.prefix)
    logger.info(f"Done in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
