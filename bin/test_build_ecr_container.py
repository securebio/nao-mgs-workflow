"""Tests for build_ecr_container.py."""

###########
# IMPORTS #
###########

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from botocore.exceptions import ClientError
from build_ecr_container import (
    build_docker_image_from_spec,
    check_image_exists,
    compute_spec_hash,
    generate_dockerfile,
    get_base_image,
    read_container_spec,
    tag_docker_image,
    update_containers_config,
)

#############
# CONSTANTS #
#############

BASE_IMAGE = "mambaorg/micromamba@sha256:abc123"

MINIMAL_SPEC = {
    "name": "widget",
    "label": "widget",
    "channels": ["conda-forge"],
    "dependencies": ["conda-forge::coreutils=9.5"],
}


###########
# HELPERS #
###########


def write_spec(tmp_path: Path, **overrides: object) -> Path:
    """Write a container spec YAML to a temp dir.

    Args:
        tmp_path: Directory to write into
        **overrides: Fields to add to or override on the minimal spec
    Returns:
        Path: Path to the written spec file
    """
    spec = {**MINIMAL_SPEC, **overrides}
    spec_file = tmp_path / "widget.yml"
    spec_file.write_text(yaml.safe_dump(spec))
    return spec_file


def client_error(code: str) -> ClientError:
    """Build a botocore ClientError with a given error code.

    Args:
        code: AWS error code, e.g. 'ImageNotFoundException'
    Returns:
        ClientError: Error suitable for use as a mock side effect
    """
    return ClientError({"Error": {"Code": code}}, "DescribeImages")


#######################
# read_container_spec #
#######################


def test_read_container_spec_returns_all_fields(tmp_path: Path) -> None:
    """A well-formed spec round-trips through the YAML load."""
    assert read_container_spec(write_spec(tmp_path)) == MINIMAL_SPEC


@pytest.mark.parametrize("missing_field", ["name", "label", "channels", "dependencies"])
def test_read_container_spec_rejects_missing_required_field(
    tmp_path: Path, missing_field: str
) -> None:
    """Every required field is actually enforced, and named in the error."""
    spec = {k: v for k, v in MINIMAL_SPEC.items() if k != missing_field}
    spec_file = tmp_path / "widget.yml"
    spec_file.write_text(yaml.safe_dump(spec))
    with pytest.raises(ValueError, match=missing_field):
        read_container_spec(spec_file)


#####################
# compute_spec_hash #
#####################


def test_compute_spec_hash_is_deterministic() -> None:
    """The same inputs must hash the same, or every run would rebuild."""
    assert compute_spec_hash(MINIMAL_SPEC, "FROM x") == compute_spec_hash(
        MINIMAL_SPEC, "FROM x"
    )


def test_compute_spec_hash_ignores_key_order() -> None:
    """Reordering YAML keys is not a content change and must not force a rebuild."""
    reordered = dict(reversed(list(MINIMAL_SPEC.items())))
    assert compute_spec_hash(reordered, "FROM x") == compute_spec_hash(
        MINIMAL_SPEC, "FROM x"
    )


def test_compute_spec_hash_changes_with_dependencies() -> None:
    """A changed pin must produce a new tag so the image is rebuilt."""
    bumped = {**MINIMAL_SPEC, "dependencies": ["conda-forge::coreutils=9.6"]}
    assert compute_spec_hash(bumped, "FROM x") != compute_spec_hash(
        MINIMAL_SPEC, "FROM x"
    )


def test_compute_spec_hash_changes_with_dockerfile() -> None:
    """Dockerfile content is hashed too, so a base-image bump invalidates the tag
    even though the spec itself is untouched."""
    assert compute_spec_hash(MINIMAL_SPEC, "FROM x") != compute_spec_hash(
        MINIMAL_SPEC, "FROM y"
    )


######################
# check_image_exists #
######################


def test_check_image_exists_true_when_describe_succeeds() -> None:
    """A successful describe means the tag is already published."""
    with patch("build_ecr_container.boto3") as mock_boto:
        client = mock_boto.client.return_value
        assert check_image_exists(client, "repo", "tag") is True


def test_check_image_exists_false_when_image_not_found() -> None:
    """ImageNotFoundException is the expected 'not published yet' signal."""
    with patch("build_ecr_container.boto3") as mock_boto:
        client = mock_boto.client.return_value
        client.describe_images.side_effect = client_error("ImageNotFoundException")
        assert check_image_exists(client, "repo", "tag") is False


def test_check_image_exists_propagates_other_errors() -> None:
    """Any other AWS error must surface rather than be read as 'not found', which
    would silently trigger a rebuild-and-push against a broken repository."""
    with patch("build_ecr_container.boto3") as mock_boto:
        client = mock_boto.client.return_value
        client.describe_images.side_effect = client_error("AccessDeniedException")
        with pytest.raises(ClientError):
            check_image_exists(client, "repo", "tag")


##################
# get_base_image #
##################


def test_get_base_image_reads_pinned_digest(tmp_path: Path) -> None:
    """The base image comes from pyproject.toml, not a hardcoded default."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        f'[tool.mgs-workflow]\ncontainer-base-image = "{BASE_IMAGE}"\n'
    )
    assert get_base_image(pyproject) == BASE_IMAGE


#######################
# generate_dockerfile #
#######################


@patch("build_ecr_container.get_base_image", return_value=BASE_IMAGE)
class TestGenerateDockerfile:
    """Dockerfile generation, with the base image patched so these tests do not
    depend on the repository's real pyproject.toml."""

    def test_uses_the_pinned_base_image(self, _mock_base: object) -> None:
        """Builds must start from the digest-pinned base for reproducibility."""
        dockerfile = generate_dockerfile("widget.yml", Path("pyproject.toml"))
        assert f"FROM {BASE_IMAGE}" in dockerfile

    def test_copies_the_named_spec_file(self, _mock_base: object) -> None:
        """The spec filename is interpolated into the COPY instruction, so the
        Dockerfile only builds in a directory holding that same filename."""
        dockerfile = generate_dockerfile("other-name.yml", Path("pyproject.toml"))
        assert "COPY other-name.yml /tmp/environment.yml" in dockerfile

    def test_installs_the_environment_and_sets_path(self, _mock_base: object) -> None:
        """The image installs the Conda environment and puts it on PATH."""
        dockerfile = generate_dockerfile("widget.yml", Path("pyproject.toml"))
        assert "micromamba install" in dockerfile
        assert "ENV PATH=/opt/conda/bin:$PATH" in dockerfile


###############################
# build_docker_image_from_spec #
###############################


@patch("build_ecr_container.get_base_image", return_value=BASE_IMAGE)
@patch("build_ecr_container.subprocess.run")
class TestBuildDockerImageFromSpec:
    """Assembling the build context and invoking docker."""

    def test_writes_dockerfile_and_copies_spec(
        self, _mock_run: object, _mock_base: object, tmp_path: Path
    ) -> None:
        """Both files the build needs land in the build directory."""
        spec_file = write_spec(tmp_path)
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        build_docker_image_from_spec(spec_file, "img:tag", build_dir)
        assert (build_dir / "Dockerfile").exists()
        assert (build_dir / spec_file.name).exists()

    def test_builds_for_linux_amd64(
        self, mock_run: object, _mock_base: object, tmp_path: Path
    ) -> None:
        """Images are pinned to linux/amd64 so a build on an arm host still
        produces something the Batch fleet can run."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        build_docker_image_from_spec(write_spec(tmp_path), "img:tag", build_dir)
        cmd = mock_run.call_args[0][0]  # type: ignore[attr-defined]
        assert cmd[:2] == ["docker", "build"]
        assert "--platform" in cmd
        assert cmd[cmd.index("--platform") + 1] == "linux/amd64"

    def test_raises_runtime_error_when_docker_fails(
        self, mock_run: object, _mock_base: object, tmp_path: Path
    ) -> None:
        """A failed build must not be reported as success."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "docker")  # type: ignore[attr-defined]
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        with pytest.raises(RuntimeError, match="Error building Docker image"):
            build_docker_image_from_spec(write_spec(tmp_path), "img:tag", build_dir)


####################
# tag_docker_image #
####################


@patch("build_ecr_container.subprocess.run")
def test_tag_docker_image_raises_with_stderr_detail(mock_run: object) -> None:
    """Tagging failures surface docker's own stderr, which is the only useful
    diagnostic when a tag collides or an image is missing."""
    mock_run.side_effect = subprocess.CalledProcessError(  # type: ignore[attr-defined]
        1, "docker", stderr=b"no such image"
    )
    with pytest.raises(RuntimeError, match="no such image"):
        tag_docker_image("src", "dst")


###########################
# update_containers_config #
###########################


def write_config(tmp_path: Path, label: str, url: str) -> Path:
    """Write a minimal containers.config with one labelled container.

    Args:
        tmp_path: Directory to write into
        label: Process label
        url: Container URL to record
    Returns:
        Path: Path to the written config
    """
    config = tmp_path / "containers.config"
    config.write_text(
        "process {\n"
        "    withLabel: other {\n"
        '        container = "unchanged:1"\n'
        "    }\n"
        f"    withLabel: {label} {{\n"
        f'        container = "{url}"\n'
        "    }\n"
        "}\n"
    )
    return config


def test_update_containers_config_replaces_the_matching_label(tmp_path: Path) -> None:
    """The pin for the target label is rewritten, and other labels are untouched."""
    config = write_config(tmp_path, "widget", "old:1")
    assert update_containers_config(config, "widget", "new:2") is True
    content = config.read_text()
    assert 'container = "new:2"' in content
    assert 'container = "unchanged:1"' in content


def test_update_containers_config_is_a_noop_when_already_current(
    tmp_path: Path,
) -> None:
    """Returning False lets the caller skip a redundant commit."""
    config = write_config(tmp_path, "widget", "same:1")
    assert update_containers_config(config, "widget", "same:1") is False


def test_update_containers_config_raises_on_unknown_label(tmp_path: Path) -> None:
    """A missing label means the placeholder was never added; failing loudly beats
    pushing an image nothing references."""
    config = write_config(tmp_path, "widget", "old:1")
    with pytest.raises(ValueError, match="not found"):
        update_containers_config(config, "absent", "new:2")
