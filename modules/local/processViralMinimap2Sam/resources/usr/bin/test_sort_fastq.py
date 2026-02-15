#!/usr/bin/env python3

import gzip

import pytest
import sort_fastq


def _write_fastq_gz(path, content):
    with gzip.open(str(path), "wt") as f:
        f.write(content)


def _read_lines(path):
    with open(str(path)) as f:
        return f.readlines()


def _make_fastq_record(read_id, seq="ACGTACGT", qual="IIIIIIII"):
    return f"@{read_id}\n{seq}\n+\n{qual}\n"


class TestSortFastq:

    def test_empty_fastq(self, tmp_path):
        """Empty FASTQ produces empty output."""
        inp = tmp_path / "input.fastq.gz"
        out = tmp_path / "sorted.fastq"
        _write_fastq_gz(inp, "")

        sort_fastq.sort_fastq(str(inp), str(out))

        lines = _read_lines(out)
        assert lines == []

    @pytest.mark.parametrize(
        "order,expected",
        [
            (["readA", "readB", "readC"], ["readA", "readB", "readC"]),
            (["readC", "readB", "readA"], ["readA", "readB", "readC"]),
        ],
        ids=["already_sorted", "reverse_sorted"],
    )
    def test_sort_order(self, tmp_path, order, expected):
        """Reads are sorted by read ID regardless of input order."""
        inp = tmp_path / "input.fastq.gz"
        out = tmp_path / "sorted.fastq"
        content = "".join(_make_fastq_record(rid) for rid in order)
        _write_fastq_gz(inp, content)

        sort_fastq.sort_fastq(str(inp), str(out))

        lines = _read_lines(out)
        read_ids = [lines[i].strip().lstrip("@") for i in range(0, len(lines), 4)]
        assert read_ids == expected

    def test_sequence_quality_association_preserved(self, tmp_path):
        """Each read's sequence and quality stay paired after sorting."""
        inp = tmp_path / "input.fastq.gz"
        out = tmp_path / "sorted.fastq"
        content = (
            _make_fastq_record("readC", "CCCC", "HHHH")
            + _make_fastq_record("readA", "AAAA", "FFFF")
            + _make_fastq_record("readB", "GGGG", "IIII")
        )
        _write_fastq_gz(inp, content)

        sort_fastq.sort_fastq(str(inp), str(out))

        lines = _read_lines(out)
        # After sorting: readA, readB, readC
        records = [
            (lines[i].strip(), lines[i + 1].strip(), lines[i + 3].strip())
            for i in range(0, len(lines), 4)
        ]
        assert records == [
            ("@readA", "AAAA", "FFFF"),
            ("@readB", "GGGG", "IIII"),
            ("@readC", "CCCC", "HHHH"),
        ]
