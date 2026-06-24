import gzip
from pathlib import Path

import kraken_domain_summary
import pytest

HEADER = (
    "name\ttaxid\trank\tkraken2_assigned_reads\t"
    "added_reads\tnew_est_reads\tfraction_total_reads"
)


def write_report(path: Path, rows: list[str]) -> None:
    """Write report rows to a gzipped Kraken2 report (the only input form)."""
    with gzip.open(path, "wt") as report_file:
        report_file.write("\n".join(rows) + "\n")


def read_output(path: Path) -> list[str]:
    with gzip.open(path, "rt") as output_file:
        return output_file.read().splitlines()


@pytest.mark.parametrize(
    ("report_rows", "expected"),
    [
        # With a "cellular organisms" node: 100 reads there are split among the
        # cellular domains only (40/40/20), and 120 root-level reads (root clade
        # minus the cellular-organisms and Viruses clades) across all four
        # (40/40/20/20). Viruses receives only the root-level share. An
        # unclassified row (taxid 0) is ignored.
        pytest.param(
            [
                "5.00\t80\t80\t0\t0\tU\t0\tunclassified",
                "100.00\t1420\t0\t100\t80\tD\t1\troot",
                "77.46\t1100\t100\t90\t70\tD\t131567\t  cellular organisms",
                "28.17\t400\t0\t60\t40\tD\t2\t    Bacteria",
                "28.17\t400\t0\t40\t30\tD\t2157\t    Archaea",
                "14.08\t200\t0\t20\t15\tD\t2759\t    Eukaryota",
                "14.08\t200\t0\t10\t8\tD\t10239\t  Viruses",
            ],
            [
                HEADER,
                "Bacteria\t2\tD\t400\t80\t480\t0.33803",
                "Archaea\t2157\tD\t400\t80\t480\t0.33803",
                "Eukaryota\t2759\tD\t200\t40\t240\t0.16901",
                "Viruses\t10239\tD\t200\t20\t220\t0.15493",
            ],
            id="with-cellular-organisms-node",
        ),
        # Without a "cellular organisms" node (post-2024 rank codes, no "D"): the
        # whole above-domain residual is root-level and split across all four
        # domains by floor-proportional allocation, leaving one read unallocated.
        # Domains are still identified by taxid despite the non-"D" rank codes.
        pytest.param(
            [
                "100.00\t1000\t25\t100\t80\tR\t1\troot",
                "65.00\t650\t0\t50\t40\tR2\t2\t    Bacteria",
                "0.50\t5\t0\t25\t20\tR2\t2157\t    Archaea",
                "4.00\t40\t0\t20\t15\tR2\t2759\t    Eukaryota",
                "0.50\t5\t0\t10\t8\tR2\t10239\t    Viruses",
            ],
            [
                HEADER,
                "Bacteria\t2\tR2\t650\t278\t928\t0.92800",
                "Archaea\t2157\tR2\t5\t2\t7\t0.00700",
                "Eukaryota\t2759\tR2\t40\t17\t57\t0.05700",
                "Viruses\t10239\tR2\t5\t2\t7\t0.00700",
            ],
            id="without-cellular-organisms-node",
        ),
    ],
)
def test_create_domain_summary(
    tmp_path: Path, report_rows: list[str], expected: list[str]
) -> None:
    report = tmp_path / "kraken.report.gz"
    output = tmp_path / "domain.tsv.gz"
    write_report(report, report_rows)

    kraken_domain_summary.create_domain_summary(report, output)

    assert read_output(output) == expected


@pytest.mark.parametrize(
    "report_rows",
    [
        # Root present but no recognized domain rows: nothing to summarize.
        pytest.param(["100.00\t5\t5\t100\t80\tR\t1\troot"], id="no-domain-reads"),
        # No root (taxid 1) row: fractions can't be normalized, so emit nothing.
        pytest.param(["60.00\t15\t0\t550\t308\tD\t10239\t  Viruses"], id="no-root-row"),
    ],
)
def test_create_domain_summary_writes_empty_output_when_unusable(
    tmp_path: Path, report_rows: list[str]
) -> None:
    report = tmp_path / "kraken.report.gz"
    output = tmp_path / "domain.tsv.gz"
    write_report(report, report_rows)

    kraken_domain_summary.create_domain_summary(report, output)

    assert read_output(output) == []
