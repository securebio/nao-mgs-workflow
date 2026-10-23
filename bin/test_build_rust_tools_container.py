#!/usr/bin/env python3
"""Unit tests for build_rust_tools_container.py"""

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from build_rust_tools_container import (
    build_image,
    build_push_and_pin,
    check_pin,
    compute_source_hash,
    read_pinned_tag,
    read_sources,
)

CONFIG_TEMPLATE = """process {{
    withLabel: python {{
        container = "public.ecr.aws/q0n1c7g8/nao-mgs-workflow/python:e7c8524f75c0f132"
    }}
    withLabel: rust_tools {{
        container = "public.ecr.aws/q0n1c7g8/nao-mgs-workflow/rust-tools:{tag}"
    }}
}}
"""


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A git repo with rust-tools sources, an ignored target dir and an unrelated file."""
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    files = {
        ".gitignore": "**/target\n",
        "rust-tools/Cargo.toml": "[workspace]\n",
        "rust-tools/tool/src/main.rs": "fn main() {}\n",
        "rust-tools/target/release/tool": "binary",
        "docker/nao-rust-tools.Dockerfile": "FROM alpine\n",
        "docker/other.Dockerfile": "FROM debian\n",
        "README.md": "readme\n",
    }
    for path, content in files.items():
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text(content)
    subprocess.run(["git", "add", "rust-tools/Cargo.toml"], cwd=tmp_path, check=True)
    return tmp_path


@pytest.fixture
def repo_sources(repo: Path) -> dict[str, bytes]:
    """Sources read before any test patches subprocess.run."""
    return read_sources(repo)


def write_config(path: Path, tag: str) -> Path:
    path.write_text(CONFIG_TEMPLATE.format(tag=tag))
    return path


class TestReadSources:
    def test_reads_tracked_and_untracked_sources_only(self, repo: Path) -> None:
        assert read_sources(repo) == {
            "docker/nao-rust-tools.Dockerfile": b"FROM alpine\n",
            "rust-tools/Cargo.toml": b"[workspace]\n",
            "rust-tools/tool/src/main.rs": b"fn main() {}\n",
        }

    def test_skips_tracked_files_deleted_from_disk(self, repo: Path) -> None:
        (repo / "rust-tools/Cargo.toml").unlink()
        assert "rust-tools/Cargo.toml" not in read_sources(repo)

    def test_rejects_symlinks(self, repo: Path) -> None:
        (repo / "rust-tools/link.rs").symlink_to("tool/src/main.rs")
        with pytest.raises(ValueError, match="Symlinks"):
            read_sources(repo)


class TestComputeSourceHash:
    def test_is_16_hex_characters(self, repo: Path) -> None:
        source_hash = compute_source_hash(read_sources(repo))
        assert len(source_hash) == 16
        int(source_hash, 16)

    @pytest.mark.parametrize(
        "path,content,changes",
        [
            ("rust-tools/tool/src/main.rs", "fn main() { 1; }\n", True),
            ("docker/nao-rust-tools.Dockerfile", "FROM alpine:3.21\n", True),
            ("rust-tools/tool/src/lib.rs", "", True),
            ("rust-tools/target/release/tool", "other binary", False),
            ("docker/other.Dockerfile", "FROM ubuntu\n", False),
            ("README.md", "changed\n", False),
        ],
    )
    def test_changes_only_with_sources(
        self, repo: Path, path: str, content: str, changes: bool
    ) -> None:
        before = compute_source_hash(read_sources(repo))
        (repo / path).write_text(content)
        assert (compute_source_hash(read_sources(repo)) != before) == changes

    def test_depends_on_paths_not_just_contents(self, repo: Path) -> None:
        before = compute_source_hash(read_sources(repo))
        (repo / "rust-tools/tool/src/main.rs").rename(
            repo / "rust-tools/tool/src/bin.rs"
        )
        assert compute_source_hash(read_sources(repo)) != before


class TestReadPinnedTag:
    def test_reads_rust_tools_tag(self, tmp_path: Path) -> None:
        config = write_config(tmp_path / "containers.config", "0123456789abcdef")
        assert read_pinned_tag(config) == "0123456789abcdef"

    def test_missing_label_raises(self, tmp_path: Path) -> None:
        config = tmp_path / "containers.config"
        config.write_text(
            'process {\n    withLabel: python {\n        container = "x:y"\n    }\n}\n'
        )
        with pytest.raises(ValueError, match="rust_tools"):
            read_pinned_tag(config)


class TestCheckPin:
    def test_matching_pin_passes(self, repo: Path, tmp_path: Path) -> None:
        config = write_config(
            tmp_path / "containers.config", compute_source_hash(read_sources(repo))
        )
        check_pin(repo, config)

    def test_stale_pin_raises(self, repo: Path, tmp_path: Path) -> None:
        config = write_config(tmp_path / "containers.config", "stale")
        with pytest.raises(ValueError, match="pinned to 'stale'"):
            check_pin(repo, config)


def record_build_context(
    contexts: list[dict[str, bytes]],
) -> Callable[..., MagicMock]:
    """Make a subprocess.run stand-in that records the build context's files."""

    def run(cmd: list[str], **kwargs: object) -> MagicMock:
        context = Path(cmd[-1])
        contexts.append(
            {
                str(p.relative_to(context)): p.read_bytes()
                for p in sorted(context.rglob("*"))
                if p.is_file()
            }
        )
        return MagicMock()

    return run


class TestBuildImage:
    @patch("build_rust_tools_container.subprocess.run")
    def test_builds_from_hashed_files_only(self, mock_run: MagicMock) -> None:
        contexts: list[dict[str, bytes]] = []
        mock_run.side_effect = record_build_context(contexts)
        sources = {
            "docker/nao-rust-tools.Dockerfile": b"FROM alpine\n",
            "rust-tools/a/b.rs": b"fn main() {}\n",
        }
        build_image(sources, "registry/rust-tools:abc")
        cmd = mock_run.call_args.args[0]
        assert cmd[:4] == ["docker", "build", "--platform", "linux/amd64"]
        assert cmd[cmd.index("-t") + 1] == "registry/rust-tools:abc"
        assert contexts == [sources]

    @patch("build_rust_tools_container.subprocess.run")
    def test_builds_the_snapshot_not_later_edits(
        self, mock_run: MagicMock, repo: Path, repo_sources: dict[str, bytes]
    ) -> None:
        contexts: list[dict[str, bytes]] = []
        mock_run.side_effect = record_build_context(contexts)
        (repo / "rust-tools/tool/src/main.rs").write_text("edited after hashing\n")
        build_image(repo_sources, "registry/rust-tools:abc")
        assert contexts[0]["rust-tools/tool/src/main.rs"] == b"fn main() {}\n"


@patch("build_rust_tools_container.update_containers_config")
@patch("build_rust_tools_container.push_docker_image")
@patch("build_rust_tools_container.docker_login_ecr")
@patch("build_rust_tools_container.build_image")
@patch("build_rust_tools_container.setup_ecr_repository")
class TestBuildPushAndPin:
    @pytest.mark.parametrize("image_exists", [True, False])
    def test_builds_and_pushes_only_missing_images(
        self,
        mock_setup: MagicMock,
        mock_build: MagicMock,
        mock_login: MagicMock,
        mock_push: MagicMock,
        mock_update: MagicMock,
        repo: Path,
        image_exists: bool,
    ) -> None:
        source_hash = compute_source_hash(read_sources(repo))
        image = f"registry/prefix/rust-tools:{source_hash}"
        mock_setup.return_value = (image, "unused:latest", "registry", image_exists)
        config = Path("containers.config")
        build_push_and_pin(repo, config, "prefix")
        mock_setup.assert_called_once_with("rust-tools", "prefix", source_hash)
        assert mock_build.called is not image_exists
        assert mock_push.called is not image_exists
        if not image_exists:
            mock_push.assert_called_once_with(image)
        mock_update.assert_called_once_with(config, "rust_tools", image)
