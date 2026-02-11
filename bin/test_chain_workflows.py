#!/usr/bin/env python3
"""Tests for chain_workflows.py."""

import csv
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from chain_workflows import (
    create_launch_directories,
    execute_nextflow,
    generate_downstream_input,
    main,
    resolve_samplesheet_path,
)

################################
# resolve_samplesheet_path     #
################################


class TestResolveSamplesheetPath:
    @pytest.mark.parametrize(
        "samplesheet_arg,resolve_against_repo",
        [
            ("/absolute/path/sheet.csv", False),
            ("relative/sheet.csv", True),
        ],
        ids=["absolute", "relative"],
    )
    def test_resolves_local_paths(
        self, tmp_path: Path, samplesheet_arg: str, resolve_against_repo: bool
    ) -> None:
        repo_root = tmp_path / "repo"
        result = resolve_samplesheet_path(samplesheet_arg, tmp_path, repo_root)
        expected = repo_root / samplesheet_arg if resolve_against_repo else Path(samplesheet_arg)
        assert result == expected

    @patch("chain_workflows.boto3")
    def test_downloads_s3_samplesheet(self, mock_boto3: MagicMock, tmp_path: Path) -> None:
        run_launch_dir = tmp_path / "run"
        run_launch_dir.mkdir()
        result = resolve_samplesheet_path(
            "s3://my-bucket/path/to/sheet.csv", run_launch_dir, tmp_path
        )
        assert result == run_launch_dir / "samplesheet.csv"
        mock_boto3.client.return_value.download_file.assert_called_once_with(
            "my-bucket", "path/to/sheet.csv", str(run_launch_dir / "samplesheet.csv")
        )

    @patch("chain_workflows.boto3")
    def test_raises_on_s3_download_failure(
        self, mock_boto3: MagicMock, tmp_path: Path
    ) -> None:
        run_launch_dir = tmp_path / "run"
        run_launch_dir.mkdir()
        mock_boto3.client.return_value.download_file.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject"
        )
        with pytest.raises(RuntimeError, match="Failed to download"):
            resolve_samplesheet_path("s3://bucket/missing.csv", run_launch_dir, tmp_path)


#################################
# generate_downstream_input     #
#################################


class TestGenerateDownstreamInput:
    @pytest.fixture
    def launch_dir(self, tmp_path: Path) -> Path:
        d = tmp_path / "downstream"
        d.mkdir()
        return d

    @pytest.fixture
    def make_samplesheet(self, tmp_path: Path):
        """Factory fixture that writes a samplesheet CSV and returns its path."""
        def _make(samples: list[str]) -> Path:
            path = tmp_path / "samplesheet.csv"
            with open(path, "w") as f:
                writer = csv.writer(f)
                writer.writerow(["sample"])
                for s in samples:
                    writer.writerow([s])
            return path
        return _make

    @pytest.mark.parametrize(
        "samples",
        [["S1"], ["S1", "S2", "S3"]],
        ids=["single_sample", "multiple_samples"],
    )
    def test_generates_correct_files(
        self, launch_dir: Path, make_samplesheet, samples: list[str]
    ) -> None:
        sheet = make_samplesheet(samples)
        run_results_dir = "s3://bucket/run/output/results"

        result_path = generate_downstream_input(
            launch_dir, sheet, run_results_dir, "test_run"
        )

        # input.csv: headers must match loadDownstreamData/main.nf required_headers
        with open(result_path) as f:
            reader = csv.reader(f)
            assert next(reader) == ["label", "results_dir", "groups_tsv"]
            assert next(reader) == [
                "test_run",
                run_results_dir,
                str((launch_dir / "groups.tsv").resolve()),
            ]

        # groups.tsv: one row per sample, sample used as its own group
        with open(launch_dir / "groups.tsv") as f:
            reader = csv.reader(f, delimiter="\t")
            assert next(reader) == ["sample", "group"]
            assert list(reader) == [[s, s] for s in samples]


#################################
# create_launch_directories     #
#################################


class TestCreateLaunchDirectories:
    EXPECTED_SUBDIRS = {"index", "run", "downstream"}

    @pytest.mark.parametrize("pre_exist", [False, True], ids=["fresh", "existing"])
    def test_creates_subdirectories(self, tmp_path: Path, pre_exist: bool) -> None:
        base = tmp_path / "launch"
        if pre_exist:
            for name in self.EXPECTED_SUBDIRS:
                (base / name).mkdir(parents=True)

        result = create_launch_directories(base)

        assert set(result.keys()) == self.EXPECTED_SUBDIRS
        for name, path in result.items():
            assert path == base / name
            assert path.is_dir()


#################################
# execute_nextflow              #
#################################


class TestExecuteNextflow:
    @pytest.fixture
    def nf_args(self, tmp_path: Path) -> dict:
        """Base arguments for execute_nextflow (without resume/extra_args)."""
        return {
            "launch_dir": tmp_path / "launch",
            "repo_root": tmp_path / "repo",
            "config_file": tmp_path / "test.config",
            "params": {"base_dir": "s3://bucket", "ref_dir": "s3://ref"},
            "workflow_name": "TEST",
            "profile": "test_profile",
        }

    @pytest.mark.parametrize(
        "resume,extra_args,expected_fragments",
        [
            (True, "", ["-resume"]),
            (False, "", []),
            (True, "--foo bar", ["-resume", "--foo", "bar"]),
            (False, "--foo bar", ["--foo", "bar"]),
        ],
        ids=["resume", "no_resume", "resume_with_extra", "extra_args_only"],
    )
    @patch("chain_workflows.subprocess.run")
    def test_constructs_correct_command(
        self,
        mock_run: MagicMock,
        nf_args: dict,
        resume: bool,
        extra_args: str,
        expected_fragments: list[str],
    ) -> None:
        mock_run.return_value = MagicMock(returncode=0)
        execute_nextflow(**nf_args, resume=resume, extra_args=extra_args)

        cmd = mock_run.call_args[0][0]
        assert cmd[:3] == ["nextflow", "run", str(nf_args["repo_root"])]
        assert "-c" in cmd and str(nf_args["config_file"]) in cmd
        assert "-profile" in cmd and nf_args["profile"] in cmd
        for key, value in nf_args["params"].items():
            assert f"--{key}={value}" in cmd
        for fragment in expected_fragments:
            assert fragment in cmd
        if not resume:
            assert "-resume" not in cmd
        assert mock_run.call_args[1]["cwd"] == nf_args["launch_dir"]

    @patch("chain_workflows.subprocess.run")
    def test_raises_on_nonzero_exit(self, mock_run: MagicMock, nf_args: dict) -> None:
        mock_run.return_value = MagicMock(returncode=1)
        with pytest.raises(RuntimeError, match="TEST workflow failed"):
            execute_nextflow(**nf_args, resume=False)


#################################
# main                          #
#################################


class TestMain:
    @pytest.fixture
    def mock_deps(self, tmp_path: Path):
        """Mock all external dependencies of main(), yielding them for assertions."""
        mock_args = MagicMock()
        mock_args.launch_dir = tmp_path / "launch"
        mock_args.base_dir = "s3://bucket/test"
        mock_args.samplesheet = "test-data/samplesheet.csv"
        mock_args.profile = "test_profile"
        mock_args.no_resume = False
        mock_args.run_id = "test_run"
        mock_args.ref_dir = None
        mock_args.skip_index = False
        mock_args.skip_run = False
        mock_args.skip_downstream = False
        mock_args.platform = "illumina"
        mock_args.nextflow_args = ""

        launch_dirs = {
            "index": tmp_path / "launch" / "index",
            "run": tmp_path / "launch" / "run",
            "downstream": tmp_path / "launch" / "downstream",
        }
        samplesheet_path = tmp_path / "resolved_sheet.csv"
        downstream_input = tmp_path / "downstream" / "input.csv"

        with (
            patch("chain_workflows.parse_arguments", return_value=mock_args),
            patch("chain_workflows.create_launch_directories", return_value=launch_dirs),
            patch("chain_workflows.execute_nextflow") as mock_execute,
            patch(
                "chain_workflows.resolve_samplesheet_path",
                return_value=samplesheet_path,
            ),
            patch(
                "chain_workflows.generate_downstream_input",
                return_value=downstream_input,
            ) as mock_generate,
        ):
            yield {
                "args": mock_args,
                "launch_dirs": launch_dirs,
                "samplesheet_path": samplesheet_path,
                "downstream_input": downstream_input,
                "execute_nextflow": mock_execute,
                "generate_downstream_input": mock_generate,
            }

    @pytest.mark.parametrize(
        "skip_index,skip_run,skip_downstream,ref_dir,expected_workflows",
        [
            (False, False, False, None, ["INDEX", "RUN", "DOWNSTREAM"]),
            (True, False, False, None, ["RUN", "DOWNSTREAM"]),
            (False, True, False, None, ["INDEX", "DOWNSTREAM"]),
            (False, False, True, None, ["INDEX", "RUN"]),
            (False, False, False, "s3://ref", ["RUN", "DOWNSTREAM"]),
            (True, True, True, None, []),
        ],
        ids=[
            "all",
            "skip_index",
            "skip_run",
            "skip_downstream",
            "ref_dir_skips_index",
            "skip_all",
        ],
    )
    def test_workflow_orchestration(
        self,
        mock_deps: dict,
        skip_index: bool,
        skip_run: bool,
        skip_downstream: bool,
        ref_dir: str | None,
        expected_workflows: list[str],
    ) -> None:
        mock_deps["args"].skip_index = skip_index
        mock_deps["args"].skip_run = skip_run
        mock_deps["args"].skip_downstream = skip_downstream
        mock_deps["args"].ref_dir = ref_dir

        main()

        called_workflows = [
            c.kwargs["workflow_name"]
            for c in mock_deps["execute_nextflow"].call_args_list
        ]
        assert called_workflows == expected_workflows

    @pytest.mark.parametrize(
        "ref_dir,expected_ref_dir",
        [
            (None, "s3://bucket/test/index/output"),
            ("s3://custom/ref/", "s3://custom/ref"),
        ],
        ids=["derived_from_index", "provided"],
    )
    def test_ref_dir_resolution(
        self, mock_deps: dict, ref_dir: str | None, expected_ref_dir: str
    ) -> None:
        mock_deps["args"].ref_dir = ref_dir

        main()

        run_and_downstream = [
            c
            for c in mock_deps["execute_nextflow"].call_args_list
            if c.kwargs["workflow_name"] in ("RUN", "DOWNSTREAM")
        ]
        for call in run_and_downstream:
            assert call.kwargs["params"]["ref_dir"] == expected_ref_dir

    @pytest.mark.parametrize("no_resume", [False, True], ids=["resume", "no_resume"])
    def test_resume_propagation(self, mock_deps: dict, no_resume: bool) -> None:
        mock_deps["args"].no_resume = no_resume

        main()

        for call in mock_deps["execute_nextflow"].call_args_list:
            assert call.kwargs["resume"] == (not no_resume)

    def test_downstream_input_generation(self, mock_deps: dict) -> None:
        main()

        mock_deps["generate_downstream_input"].assert_called_once_with(
            downstream_launch_dir=mock_deps["launch_dirs"]["downstream"],
            samplesheet_path=mock_deps["samplesheet_path"],
            run_results_dir="s3://bucket/test/run/output/results",
            run_id="test_run",
        )
        downstream_call = [
            c
            for c in mock_deps["execute_nextflow"].call_args_list
            if c.kwargs["workflow_name"] == "DOWNSTREAM"
        ][0]
        assert downstream_call.kwargs["params"]["input_file"] == str(
            mock_deps["downstream_input"].resolve()
        )
