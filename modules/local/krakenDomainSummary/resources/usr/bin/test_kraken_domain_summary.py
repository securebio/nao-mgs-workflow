import gzip
from pathlib import Path

import kraken_domain_summary


def write_report(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows) + "\n")


def read_output(path: Path) -> list[str]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt") as output_file:
            return output_file.read().splitlines()
    return path.read_text().splitlines()


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


def test_create_domain_summary_prorates_all_reads_above_domain(tmp_path: Path) -> None:
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
