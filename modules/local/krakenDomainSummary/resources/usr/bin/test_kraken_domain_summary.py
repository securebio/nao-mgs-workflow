import gzip
from pathlib import Path

import kraken_domain_summary
import pytest


def write_report(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows) + "\n")


def read_output(path: Path) -> list[str]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt") as output_file:
            return output_file.read().splitlines()
    return path.read_text().splitlines()


@pytest.mark.parametrize(
    ("reads_to_allocate", "domain_reads", "expected"),
    [
        # Exact division leaves no remainder.
        (100, [60, 40], [60, 40]),
        # Largest-remainder distributes the leftover; ties break by index order.
        (10, [1, 1, 1], [4, 3, 3]),
        # Nothing to allocate, or no weight to allocate against, yields zeros.
        (0, [5, 5], [0, 0]),
        (10, [0, 0], [0, 0]),
        # A zero-weight domain receives nothing; the rest absorb the remainder.
        (10, [7, 3, 0], [7, 3, 0]),
    ],
)
def test_allocate_reads_proportionally(
    reads_to_allocate: int, domain_reads: list[int], expected: list[int]
) -> None:
    allocation = kraken_domain_summary.allocate_reads_proportionally(
        reads_to_allocate, domain_reads
    )
    assert allocation == expected
    # Allocations are integers that sum exactly to the pool when there is weight.
    assert sum(allocation) == (reads_to_allocate if sum(domain_reads) > 0 else 0)


def test_create_domain_summary_without_above_domain_reads(tmp_path: Path) -> None:
    report = tmp_path / "kraken.report"
    output = tmp_path / "domain.tsv"
    write_report(
        report,
        [
            "100.00\t25\t0\t915\t552\tR\t1\troot",
            "60.00\t15\t0\t550\t308\tD\t10239\t  Viruses",
            "20.00\t5\t0\t207\t141\tD\t2\t  Bacteria",
            "20.00\t5\t0\t158\t103\tD\t2759\t  Eukaryota",
        ],
    )

    kraken_domain_summary.create_domain_summary(report, output)

    assert read_output(output) == [
        "name\ttaxid\trank\tkraken2_assigned_reads\tadded_reads\tnew_est_reads\tfraction_total_reads",
        "Viruses\t10239\tD\t15\t0\t15\t0.60000",
        "Bacteria\t2\tD\t5\t0\t5\t0.20000",
        "Eukaryota\t2759\tD\t5\t0\t5\t0.20000",
    ]


def test_create_domain_summary_excludes_viruses_from_cellular_residual(
    tmp_path: Path,
) -> None:
    # The 90 reads sitting at "cellular organisms" above the cellular domains are
    # split among Bacteria/Archaea/Eukaryota only (60/20/10). Viruses, which is
    # not a cellular organism, receives none of them; here the root residual is
    # zero, so Viruses keeps exactly its Kraken clade count.
    report = tmp_path / "kraken.report"
    output = tmp_path / "domain.tsv"
    write_report(
        report,
        [
            "100.00\t1000\t0\t100\t80\tR\t1\troot",
            "99.00\t990\t90\t90\t70\tR1\t131567\t  cellular organisms",
            "60.00\t600\t0\t60\t40\tD\t2\t    Bacteria",
            "20.00\t200\t0\t20\t15\tD\t2157\t    Archaea",
            "10.00\t100\t0\t10\t8\tD\t2759\t    Eukaryota",
            "1.00\t10\t0\t5\t4\tD\t10239\t  Viruses",
        ],
    )

    kraken_domain_summary.create_domain_summary(report, output)

    assert read_output(output) == [
        "name\ttaxid\trank\tkraken2_assigned_reads\tadded_reads\tnew_est_reads\tfraction_total_reads",
        "Bacteria\t2\tD\t600\t60\t660\t0.66000",
        "Archaea\t2157\tD\t200\t20\t220\t0.22000",
        "Eukaryota\t2759\tD\t100\t10\t110\t0.11000",
        "Viruses\t10239\tD\t10\t0\t10\t0.01000",
    ]


def test_create_domain_summary_splits_residual_across_both_levels(
    tmp_path: Path,
) -> None:
    # Two residuals compose: 100 reads inside "cellular organisms" split among the
    # cellular domains only (40/40/20), and 120 root-level reads (root clade minus
    # the cellular-organisms clade minus the Viruses clade) split across all four
    # domains (40/40/20/20). Viruses only receives the root-level share.
    report = tmp_path / "kraken.report"
    output = tmp_path / "domain.tsv"
    write_report(
        report,
        [
            "100.00\t1420\t0\t100\t80\tR\t1\troot",
            "77.46\t1100\t100\t90\t70\tR1\t131567\t  cellular organisms",
            "28.17\t400\t0\t60\t40\tD\t2\t    Bacteria",
            "28.17\t400\t0\t40\t30\tD\t2157\t    Archaea",
            "14.08\t200\t0\t20\t15\tD\t2759\t    Eukaryota",
            "14.08\t200\t0\t10\t8\tD\t10239\t  Viruses",
        ],
    )

    kraken_domain_summary.create_domain_summary(report, output)

    assert read_output(output) == [
        "name\ttaxid\trank\tkraken2_assigned_reads\tadded_reads\tnew_est_reads\tfraction_total_reads",
        "Bacteria\t2\tD\t400\t80\t480\t0.33803",
        "Archaea\t2157\tD\t400\t80\t480\t0.33803",
        "Eukaryota\t2759\tD\t200\t40\t240\t0.16901",
        "Viruses\t10239\tD\t200\t20\t220\t0.15493",
    ]


def test_create_domain_summary_falls_back_without_cellular_organisms_node(
    tmp_path: Path,
) -> None:
    # When the report has no "cellular organisms" node, the whole above-domain
    # residual is treated as root-level and split across all four domains via
    # largest-remainder allocation.
    report = tmp_path / "kraken.report"
    output = tmp_path / "domain.tsv.gz"
    write_report(
        report,
        [
            "100.00\t1000\t25\t100\t80\tR\t1\troot",
            "65.00\t650\t0\t50\t40\tR2\t2\t    Bacteria",
            "0.50\t5\t0\t25\t20\tR2\t10239\t    Viruses",
            "4.00\t40\t0\t20\t15\tR2\t2759\t    Eukaryota",
            "0.50\t5\t0\t10\t8\tR2\t2157\t    Archaea",
        ],
    )

    kraken_domain_summary.create_domain_summary(report, output)

    assert read_output(output) == [
        "name\ttaxid\trank\tkraken2_assigned_reads\tadded_reads\tnew_est_reads\tfraction_total_reads",
        "Bacteria\t2\tR2\t650\t279\t929\t0.92900",
        "Viruses\t10239\tR2\t5\t2\t7\t0.00700",
        "Eukaryota\t2759\tR2\t40\t17\t57\t0.05700",
        "Archaea\t2157\tR2\t5\t2\t7\t0.00700",
    ]


def test_create_domain_summary_accepts_labeled_kraken_reports(
    tmp_path: Path,
) -> None:
    report = tmp_path / "kraken.tsv"
    output = tmp_path / "domain.tsv"
    write_report(
        report,
        [
            "pc_reads_total\tn_reads_clade\tn_reads_direct\tn_minimizers_total\tn_minimizers_distinct\trank\ttaxid\tname\tsample\tribosomal",
            "100.00\t10\t2\t100\t80\tR\t1\troot\ts1\tFALSE",
            "80.00\t8\t0\t80\t60\tR2\t2\t    Bacteria\ts1\tFALSE",
        ],
    )

    kraken_domain_summary.create_domain_summary(report, output)

    assert read_output(output) == [
        "name\ttaxid\trank\tkraken2_assigned_reads\tadded_reads\tnew_est_reads\tfraction_total_reads",
        "Bacteria\t2\tR2\t8\t2\t10\t1.00000",
    ]


def test_create_domain_summary_with_no_domain_reads_writes_empty_output(
    tmp_path: Path,
) -> None:
    report = tmp_path / "kraken.report"
    output = tmp_path / "domain.tsv"
    write_report(
        report,
        [
            "100.00\t5\t5\t100\t80\tR\t1\troot",
        ],
    )

    kraken_domain_summary.create_domain_summary(report, output)

    assert output.read_text() == ""


def test_create_domain_summary_with_no_root_writes_empty_output(
    tmp_path: Path,
) -> None:
    # A report missing the root (taxid 1) row cannot be normalized, so the
    # output is empty rather than wrong.
    report = tmp_path / "kraken.report"
    output = tmp_path / "domain.tsv"
    write_report(
        report,
        [
            "60.00\t15\t0\t550\t308\tD\t10239\t  Viruses",
        ],
    )

    kraken_domain_summary.create_domain_summary(report, output)

    assert output.read_text() == ""


def test_create_domain_summary_reads_gzipped_report(tmp_path: Path) -> None:
    # The input report may itself be gzipped (open_by_suffix handles both).
    report = tmp_path / "kraken.report.gz"
    output = tmp_path / "domain.tsv"
    with gzip.open(report, "wt") as report_file:
        report_file.write(
            "\n".join(
                [
                    "100.00\t25\t0\t915\t552\tR\t1\troot",
                    "60.00\t15\t0\t550\t308\tD\t10239\t  Viruses",
                    "40.00\t10\t0\t207\t141\tD\t2\t  Bacteria",
                ]
            )
            + "\n"
        )

    kraken_domain_summary.create_domain_summary(report, output)

    assert read_output(output) == [
        "name\ttaxid\trank\tkraken2_assigned_reads\tadded_reads\tnew_est_reads\tfraction_total_reads",
        "Viruses\t10239\tD\t15\t0\t15\t0.60000",
        "Bacteria\t2\tD\t10\t0\t10\t0.40000",
    ]
