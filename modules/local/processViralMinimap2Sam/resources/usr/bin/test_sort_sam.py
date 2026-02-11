#!/usr/bin/env python3

import gzip

import pytest
import sort_sam


MINIMAL_HEADER = "@HD\tVN:1.6\tSO:queryname\n@SQ\tSN:ref1\tLN:10000\n"


def _write_sam_gz(path, content):
    with gzip.open(str(path), "wt") as f:
        f.write(content)


def _read_lines(path):
    with open(str(path)) as f:
        return f.readlines()


class TestSortSam:

    def test_empty_sam_header_only(self, tmp_path):
        """Header-only SAM produces output with just headers."""
        inp = tmp_path / "input.sam.gz"
        out = tmp_path / "sorted.sam"
        _write_sam_gz(inp, MINIMAL_HEADER)

        sort_sam.sort_sam(str(inp), str(out))

        lines = _read_lines(out)
        assert len(lines) == 2
        assert all(l.startswith("@") for l in lines)

    @pytest.mark.parametrize(
        "order,expected",
        [
            (["readA", "readB", "readC"], ["readA", "readB", "readC"]),
            (["readC", "readB", "readA"], ["readA", "readB", "readC"]),
        ],
        ids=["already_sorted", "reverse_sorted"],
    )
    def test_sort_order(self, tmp_path, order, expected):
        """Alignments are sorted by QNAME regardless of input order."""
        inp = tmp_path / "input.sam.gz"
        out = tmp_path / "sorted.sam"
        alignment_lines = "".join(
            f"{name}\t0\tref1\t100\t60\t10M\t*\t0\t0\tACGT\tIIII\n"
            for name in order
        )
        _write_sam_gz(inp, MINIMAL_HEADER + alignment_lines)

        sort_sam.sort_sam(str(inp), str(out))

        lines = _read_lines(out)
        qnames = [l.split("\t")[0] for l in lines if not l.startswith("@")]
        assert qnames == expected

    def test_multiple_alignments_per_read(self, tmp_path):
        """Multiple alignments for the same read are grouped together."""
        inp = tmp_path / "input.sam.gz"
        out = tmp_path / "sorted.sam"
        content = (
            MINIMAL_HEADER
            + "readB\t0\tref1\t100\t60\t10M\t*\t0\t0\tACGT\tIIII\n"
            + "readA\t0\tref1\t200\t60\t10M\t*\t0\t0\tACGT\tIIII\n"
            + "readA\t256\tref1\t300\t30\t10M\t*\t0\t0\tACGT\tIIII\n"
        )
        _write_sam_gz(inp, content)

        sort_sam.sort_sam(str(inp), str(out))

        lines = _read_lines(out)
        qnames = [l.split("\t")[0] for l in lines if not l.startswith("@")]
        assert qnames == ["readA", "readA", "readB"]

    def test_header_preservation(self, tmp_path):
        """All header types (@HD, @SQ, @RG, @PG, @CO) are preserved in order."""
        inp = tmp_path / "input.sam.gz"
        out = tmp_path / "sorted.sam"
        headers = (
            "@HD\tVN:1.6\n"
            "@SQ\tSN:ref1\tLN:10000\n"
            "@RG\tID:sample1\n"
            "@PG\tID:minimap2\tPN:minimap2\n"
            "@CO\tThis is a comment\n"
        )
        content = headers + "readA\t0\tref1\t100\t60\t10M\t*\t0\t0\tACGT\tIIII\n"
        _write_sam_gz(inp, content)

        sort_sam.sort_sam(str(inp), str(out))

        lines = _read_lines(out)
        header_lines = [l for l in lines if l.startswith("@")]
        assert len(header_lines) == 5
        assert header_lines[0].startswith("@HD")
        assert header_lines[1].startswith("@SQ")
        assert header_lines[2].startswith("@RG")
        assert header_lines[3].startswith("@PG")
        assert header_lines[4].startswith("@CO")
