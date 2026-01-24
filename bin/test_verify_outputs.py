#!/usr/bin/env python3
"""Unit tests for verify_outputs.py"""

from unittest.mock import patch, MagicMock

import pytest

from verify_outputs import (
    main,
    compare_outputs,
    expand_group_placeholder,
    get_expected_outputs,
    is_excluded,
    list_files,
    list_local_files,
    list_s3_files,
    parse_groups_from_file,
    parse_groups_from_input_csv,
    report_verification,
    resolve_groups,
)


class TestExpandGroupPlaceholder:
    @pytest.mark.parametrize(
        "patterns,groups,expected",
        [
            (["file.txt"], ["a", "b"], {"file.txt"}),
            (["{GROUP}/out.txt"], ["a", "b"], {"a/out.txt", "b/out.txt"}),
            (["static.txt", "{GROUP}/data.csv"], ["x"], {"static.txt", "x/data.csv"}),
            (["{GROUP}/out.txt", "static.txt"], [], {"static.txt"}),
            (["{GROUP}/{GROUP}.txt"], ["a"], {"a/a.txt"}),
        ],
    )
    def test_expansion(self, patterns, groups, expected):
        assert expand_group_placeholder(patterns, groups) == expected


class TestIsExcluded:
    @pytest.mark.parametrize(
        "path,patterns,expected",
        [
            ("logging/trace.txt", ["logging/trace.txt"], True),
            ("logging/trace-12345.txt", ["logging/trace*"], True),
            ("results/output.csv", ["logging/trace*"], False),
            ("logging/trace.log", ["logging/trace*", "temp/*"], True),
            ("temp/scratch.txt", ["logging/trace*", "temp/*"], True),
            ("results/final.csv", ["logging/trace*", "temp/*"], False),
            ("any/file.txt", [], False),
        ],
    )
    def test_exclusion(self, path, patterns, expected):
        assert is_excluded(path, patterns) == expected


class TestCompareOutputs:
    @pytest.mark.parametrize(
        "expected,actual,excluded,want_missing,want_unexpected",
        [
            ({"a.txt", "b.txt"}, {"a.txt", "b.txt"}, [], set(), set()),
            ({"a.txt", "b.txt"}, {"a.txt"}, [], {"b.txt"}, set()),
            ({"a.txt"}, {"a.txt", "b.txt"}, [], set(), {"b.txt"}),
            ({"a.txt"}, {"a.txt", "log/trace.txt"}, ["log/*"], set(), set()),
            ({"a.txt", "b.txt"}, {"a.txt", "c.txt"}, [], {"b.txt"}, {"c.txt"}),
        ],
    )
    def test_comparison(self, expected, actual, excluded, want_missing, want_unexpected):
        missing, unexpected = compare_outputs(expected, actual, excluded)
        assert missing == want_missing
        assert unexpected == want_unexpected


class TestParseGroupsFromFile:
    @pytest.mark.parametrize(
        "filename,content,expected",
        [
            ("groups.tsv", "sample\tgroup\ns1\ta\ns2\ta\ns3\tb\n", ["a", "b"]),
            ("samplesheet.csv", "sample,fastq_1\ns1,f1.fq\ns2,f2.fq\n", ["s1", "s2"]),
            ("groups.csv", "sample,group\ns1,alpha\ns2,beta\n", ["alpha", "beta"]),
            ("groups.tsv", "sample\tgroup\ns1\tzebra\ns2\talpha\n", ["alpha", "zebra"]),
        ],
    )
    def test_valid_files(self, tmp_path, filename, content, expected):
        groups_file = tmp_path / filename
        groups_file.write_text(content)
        assert parse_groups_from_file(str(groups_file)) == expected

    def test_missing_required_column(self, tmp_path):
        bad_file = tmp_path / "bad.csv"
        bad_file.write_text("name,value\nfoo,bar\n")
        with pytest.raises(ValueError, match="must have 'group' or 'sample' column"):
            parse_groups_from_file(str(bad_file))


class TestResolveGroups:
    def test_local_file(self, tmp_path):
        groups_file = tmp_path / "groups.tsv"
        groups_file.write_text("sample\tgroup\ns1\tgroup_a\ns2\tgroup_b\n")
        assert resolve_groups(str(groups_file)) == ["group_a", "group_b"]


class TestParseGroupsFromInputCsv:
    def test_single_groups_file(self, tmp_path):
        groups_file = tmp_path / "groups.tsv"
        groups_file.write_text("sample\tgroup\ns1\tgroup_a\ns2\tgroup_b\n")
        input_csv = tmp_path / "input.csv"
        input_csv.write_text(f"label,hits_tsv,groups_tsv\nrun1,hits.tsv,{groups_file}\n")
        assert parse_groups_from_input_csv(str(input_csv)) == ["group_a", "group_b"]

    def test_multiple_groups_files(self, tmp_path):
        groups1 = tmp_path / "groups1.tsv"
        groups1.write_text("sample\tgroup\ns1\talpha\n")
        groups2 = tmp_path / "groups2.tsv"
        groups2.write_text("sample\tgroup\ns2\tbeta\ns3\tgamma\n")
        input_csv = tmp_path / "input.csv"
        input_csv.write_text(
            f"label,hits_tsv,groups_tsv\nrun1,h1.tsv,{groups1}\nrun2,h2.tsv,{groups2}\n"
        )
        assert parse_groups_from_input_csv(str(input_csv)) == ["alpha", "beta", "gamma"]


class TestGetExpectedOutputs:
    @pytest.mark.parametrize(
        "workflow,outputs",
        [
            ("run", ["file1.txt", "file2.txt"]),
            ("downstream", ["{GROUP}/output.csv"]),
            ("downstream-ont", ["ont_results.tsv"]),
        ],
    )
    def test_valid_keys(self, tmp_path, workflow, outputs):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(f"""
[tool.mgs-workflow]
expected-outputs-{workflow} = {outputs!r}
""")
        assert get_expected_outputs(pyproject, workflow) == outputs

    @pytest.mark.parametrize(
        "content,workflow,match",
        [
            ("[tool.mgs-workflow]\nexpected-outputs-run = []", "downstream", "expected-outputs-downstream"),
            ("[tool.other]\nkey = 'value'", "run", "expected-outputs-run"),
        ],
    )
    def test_missing_key(self, tmp_path, content, workflow, match):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(content)
        with pytest.raises(ValueError, match=f"Missing '{match}'"):
            get_expected_outputs(pyproject, workflow)


class TestReportVerification:
    def test_success_does_not_raise(self):
        report_verification("test", set(), set())

    @pytest.mark.parametrize(
        "missing,unexpected,match",
        [
            ({"a.txt"}, set(), "1 missing"),
            (set(), {"a.txt"}, "1 unexpected"),
            ({"a.txt", "b.txt"}, {"c.txt"}, "2 missing.*1 unexpected"),
        ],
    )
    def test_failure_raises(self, missing, unexpected, match):
        with pytest.raises(ValueError, match=match):
            report_verification("test", missing, unexpected)

    def test_error_includes_workflow_name(self):
        with pytest.raises(ValueError, match="MY_WORKFLOW"):
            report_verification("MY_WORKFLOW", {"file.txt"}, set())


class TestListLocalFiles:
    def test_lists_files_recursively(self, tmp_path):
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "b.txt").write_text("b")
        result = list_local_files(str(tmp_path))
        assert result == {"a.txt", "subdir/b.txt"}

    def test_empty_directory(self, tmp_path):
        assert list_local_files(str(tmp_path)) == set()

    def test_nonexistent_directory(self, tmp_path):
        with pytest.raises(ValueError, match="does not exist"):
            list_local_files(str(tmp_path / "nonexistent"))


class TestListFiles:
    def test_local_path(self, tmp_path):
        (tmp_path / "file.txt").write_text("content")
        result = list_files(str(tmp_path))
        assert result == {"file.txt"}


class TestListS3Files:
    def test_invalid_s3_path(self):
        with pytest.raises(ValueError, match="Invalid S3 path"):
            list_s3_files("/local/path")


class TestMain:
    def test_success(self, tmp_path):
        # Create pyproject.toml with expected outputs
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-run = ["results/output.txt"]
""")
        # Create output directory with matching files
        output_dir = tmp_path / "output"
        (output_dir / "results").mkdir(parents=True)
        (output_dir / "results" / "output.txt").write_text("data")

        with patch("sys.argv", [
            "verify_outputs.py",
            "--output-dir", str(output_dir),
            "--expected-outputs-key", "run",
            "--pyproject", str(pyproject),
        ]):
            main()  # Should not raise

    def test_missing_files_raises(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-run = ["results/output.txt", "results/other.txt"]
""")
        output_dir = tmp_path / "output"
        (output_dir / "results").mkdir(parents=True)
        (output_dir / "results" / "output.txt").write_text("data")

        with patch("sys.argv", [
            "verify_outputs.py",
            "--output-dir", str(output_dir),
            "--expected-outputs-key", "run",
            "--pyproject", str(pyproject),
        ]):
            with pytest.raises(ValueError, match="1 missing"):
                main()

    def test_unexpected_files_raises(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-run = ["results/output.txt"]
""")
        output_dir = tmp_path / "output"
        (output_dir / "results").mkdir(parents=True)
        (output_dir / "results" / "output.txt").write_text("data")
        (output_dir / "results" / "extra.txt").write_text("extra")

        with patch("sys.argv", [
            "verify_outputs.py",
            "--output-dir", str(output_dir),
            "--expected-outputs-key", "run",
            "--pyproject", str(pyproject),
        ]):
            with pytest.raises(ValueError, match="1 unexpected"):
                main()

    def test_with_input_csv(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-downstream = ["{GROUP}/output.txt"]
""")
        # Create groups TSV that the input CSV will reference
        groups_file = tmp_path / "groups.tsv"
        groups_file.write_text("sample\tgroup\ns1\tgroup_a\n")

        # Create input CSV that references the groups file
        input_csv = tmp_path / "input.csv"
        input_csv.write_text(f"label,hits_tsv,groups_tsv\nrun1,hits.tsv,{groups_file}\n")

        output_dir = tmp_path / "output"
        (output_dir / "group_a").mkdir(parents=True)
        (output_dir / "group_a" / "output.txt").write_text("data")

        with patch("sys.argv", [
            "verify_outputs.py",
            "--output-dir", str(output_dir),
            "--expected-outputs-key", "downstream",
            "--pyproject", str(pyproject),
            "--input-csv", str(input_csv),
        ]):
            main()  # Should not raise

    def test_excluded_files_not_flagged(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-run = ["results/output.txt"]
""")
        output_dir = tmp_path / "output"
        (output_dir / "results").mkdir(parents=True)
        (output_dir / "results" / "output.txt").write_text("data")
        (output_dir / "logging").mkdir()
        (output_dir / "logging" / "trace-123.txt").write_text("trace")

        with patch("sys.argv", [
            "verify_outputs.py",
            "--output-dir", str(output_dir),
            "--expected-outputs-key", "run",
            "--pyproject", str(pyproject),
        ]):
            main()  # Should not raise - trace file is excluded by default

    def test_exclude_outputs_from_other_workflow(self, tmp_path):
        """Test that --exclude-outputs-from excludes another workflow's expected outputs."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-run = ["results/run_output.txt"]
expected-outputs-downstream = ["results/downstream_output.txt"]
""")
        output_dir = tmp_path / "output"
        (output_dir / "results").mkdir(parents=True)
        (output_dir / "results" / "downstream_output.txt").write_text("downstream")
        (output_dir / "results" / "run_output.txt").write_text("run")

        with patch("sys.argv", [
            "verify_outputs.py",
            "--output-dir", str(output_dir),
            "--expected-outputs-key", "downstream",
            "--pyproject", str(pyproject),
            "--exclude-outputs-from", "run",
        ]):
            main()  # Should not raise - run_output.txt is excluded

    def test_exclude_outputs_from_without_flag_fails(self, tmp_path):
        """Test that without --exclude-outputs-from, other workflow's outputs are unexpected."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.mgs-workflow]
expected-outputs-run = ["results/run_output.txt"]
expected-outputs-downstream = ["results/downstream_output.txt"]
""")
        output_dir = tmp_path / "output"
        (output_dir / "results").mkdir(parents=True)
        (output_dir / "results" / "downstream_output.txt").write_text("downstream")
        (output_dir / "results" / "run_output.txt").write_text("run")

        with patch("sys.argv", [
            "verify_outputs.py",
            "--output-dir", str(output_dir),
            "--expected-outputs-key", "downstream",
            "--pyproject", str(pyproject),
        ]):
            with pytest.raises(ValueError, match="1 unexpected"):
                main()


class TestListS3FilesDirectoryMarkers:
    """Test that S3 directory markers are filtered out."""

    def test_filters_directory_markers(self):
        mock_paginator = type("Paginator", (), {
            "paginate": lambda self, **kwargs: [
                {
                    "Contents": [
                        {"Key": "prefix/file.txt"},
                        {"Key": "prefix/subdir/"},
                        {"Key": "prefix/subdir/file2.txt"},
                        {"Key": "prefix/"},
                    ]
                }
            ]
        })()

        with patch("boto3.client") as mock_client:
            mock_client.return_value.get_paginator.return_value = mock_paginator
            result = list_s3_files("s3://bucket/prefix")

        assert result == {"file.txt", "subdir/file2.txt"}
