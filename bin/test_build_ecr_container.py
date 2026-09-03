"""Tests for build_ecr_container.py."""

###########
# IMPORTS #
###########

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from build_ecr_container import (
    build_container,
    build_docker_image_from_spec,
    compute_spec_hash,
    generate_dockerfile,
    read_container_spec,
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


########################
# read_container_spec  #
########################


def test_read_container_spec_accepts_spec_without_build_steps(tmp_path: Path) -> None:
    """A spec omitting build_steps stays valid: the field is optional."""
    spec = read_container_spec(write_spec(tmp_path))
    assert "build_steps" not in spec


def test_read_container_spec_accepts_valid_build_steps(tmp_path: Path) -> None:
    """A list of strings is accepted and returned verbatim."""
    steps = ["echo one", "echo two"]
    spec = read_container_spec(write_spec(tmp_path, build_steps=steps))
    assert spec["build_steps"] == steps


@pytest.mark.parametrize(
    "bad_value",
    [
        "echo not-a-list",
        ["echo ok", 42],
        {"step": "echo ok"},
        [None],
    ],
    ids=["string", "list-with-int", "dict", "list-with-none"],
)
def test_read_container_spec_rejects_malformed_build_steps(
    tmp_path: Path, bad_value: object
) -> None:
    """build_steps must be a list of strings; anything else is a spec error.

    Caught at parse time rather than surfacing as a confusing Dockerfile syntax
    error partway through a build.
    """
    with pytest.raises(ValueError, match="must be a list of strings"):
        read_container_spec(write_spec(tmp_path, build_steps=bad_value))


########################
# generate_dockerfile  #
########################


@patch("build_ecr_container.get_base_image", return_value=BASE_IMAGE)
class TestGenerateDockerfile:
    """Dockerfile generation, with the base image pinned so tests do not read
    pyproject.toml."""

    def test_omits_run_lines_when_no_build_steps(self, _mock_base: object) -> None:
        """Without build steps the Dockerfile ends at the conda install."""
        dockerfile = generate_dockerfile("widget.yml", Path("pyproject.toml"))
        assert dockerfile.rstrip().endswith("ENV PATH=/opt/conda/bin:$PATH")

    @pytest.mark.parametrize("build_steps", [None, []], ids=["none", "empty"])
    def test_none_and_empty_build_steps_are_equivalent(
        self, _mock_base: object, build_steps: list[str] | None
    ) -> None:
        """An empty list must produce the same Dockerfile as no list at all, so
        adding an empty key to a spec does not change its hash."""
        baseline = generate_dockerfile("widget.yml", Path("pyproject.toml"))
        assert (
            generate_dockerfile("widget.yml", Path("pyproject.toml"), build_steps)
            == baseline
        )

    def test_emits_one_run_line_per_step_in_order(self, _mock_base: object) -> None:
        """Each step becomes its own RUN line, in the order given."""
        steps = ["apt-get install -y gcc", "make install", "apt-get purge -y gcc"]
        dockerfile = generate_dockerfile("widget.yml", Path("pyproject.toml"), steps)
        run_lines = [
            line for line in dockerfile.splitlines() if line.startswith("RUN ")
        ]
        assert run_lines[-3:] == [f"RUN {step}" for step in steps]

    def test_build_steps_follow_the_conda_install(self, _mock_base: object) -> None:
        """Steps must run after the install so they can use the environment."""
        dockerfile = generate_dockerfile(
            "widget.yml", Path("pyproject.toml"), ["bowtie2-build --version"]
        )
        assert dockerfile.index("micromamba install") < dockerfile.index(
            "RUN bowtie2-build --version"
        )


######################
# compute_spec_hash  #
######################


@patch("build_ecr_container.get_base_image", return_value=BASE_IMAGE)
def test_changing_build_steps_changes_the_spec_hash(_mock_base: object) -> None:
    """Editing build steps must invalidate the image tag.

    The tag is what decides whether a rebuild happens at all, so a build-step
    change that hashed the same would silently reuse a stale image.
    """
    spec_a = {**MINIMAL_SPEC, "build_steps": ["make"]}
    spec_b = {**MINIMAL_SPEC, "build_steps": ["make -j8"]}
    hash_a = compute_spec_hash(
        spec_a, generate_dockerfile("widget.yml", Path("p.toml"), spec_a["build_steps"])
    )
    hash_b = compute_spec_hash(
        spec_b, generate_dockerfile("widget.yml", Path("p.toml"), spec_b["build_steps"])
    )
    assert hash_a != hash_b


##########################
# threading through build #
##########################


@patch("build_ecr_container.get_base_image", return_value=BASE_IMAGE)
@patch("build_ecr_container.subprocess.run")
def test_build_docker_image_writes_build_steps_into_dockerfile(
    _mock_run: object, _mock_base: object, tmp_path: Path
) -> None:
    """The Dockerfile handed to `docker build` carries the steps."""
    spec_file = write_spec(tmp_path)
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    build_docker_image_from_spec(
        spec_file, "img:tag", build_dir, Path("pyproject.toml"), ["make install"]
    )
    assert "RUN make install" in (build_dir / "Dockerfile").read_text()


@patch("build_ecr_container.tag_docker_image")
@patch("build_ecr_container.build_docker_image_from_spec")
def test_build_container_forwards_build_steps(
    mock_build: object, _mock_tag: object, tmp_path: Path
) -> None:
    """build_container passes steps down rather than dropping them.

    Without this the hashed Dockerfile and the built Dockerfile would diverge, and
    the pushed image would not match the tag computed from the spec.
    """
    steps = ["make install"]
    build_container(
        write_spec(tmp_path), "img:tag", "img:latest", Path("p.toml"), steps
    )
    assert mock_build.call_args[0][4] == steps  # type: ignore[attr-defined]
