#!/usr/bin/env python3
"""Unit tests for verify_outputs.py"""

import pytest

from verify_outputs import (
    compare_outputs,
    expand_group_placeholder,
    get_expected_outputs,
    is_excluded,
    list_files,
    list_local_files,
    list_s3_files,
    parse_groups_from_file,
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
