"""Integration tests for compare_downstream_runs.py (munging/orchestration).

These exercise the file-discovery and parsing logic on small synthetic inputs
written to tmp_path -- never real delivery data and never network. The numeric
calculations are tested separately in test_downstream_metrics.py.
"""

import gzip
from pathlib import Path

import compare_downstream_runs as cdr
import pytest


def _write_tsv_gz(path: Path, header: list[str], rows: list[list]) -> None:
    lines = ["\t".join(header)]
    lines += ["\t".join(str(v) for v in r) for r in rows]
    with gzip.open(path, "wt") as fh:
        fh.write("\n".join(lines) + "\n")


# Wholly invented group identifiers (no real site/date data) that still exercise
# the underscore- and hyphen-containing group-name parsing.
_USCORE_GROUP = "Demo_Site_19990101"
_HYPHEN_GROUP = "XX-000000-Demo-NAS-P1"


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        (
            f"{_USCORE_GROUP}_qc_basic_stats_raw.tsv.gz",
            (_USCORE_GROUP, "qc_basic_stats_raw"),
        ),
        (f"{_HYPHEN_GROUP}_fastp.json", (_HYPHEN_GROUP, "fastp")),
        ("stray_file.tsv.gz", None),
    ],
)
def test_split_filename(filename: str, expected: tuple[str, str] | None) -> None:
    known = {"qc_basic_stats_raw", "validation_hits", "fastp", "read_counts"}
    assert cdr._split_filename(filename, known) == expected


class TestDiscoverSide:
    KNOWN = {"validation_hits", "kraken", "clade_counts", "qc_basic_stats_raw"}

    def _make_results(self, base: Path, with_clade: bool) -> Path:
        d = base
        d.mkdir(parents=True, exist_ok=True)
        g = "G1"
        _write_tsv_gz(
            d / f"{g}_validation_hits.tsv.gz",
            ["seq_id", "group", "aligner_taxid_lca"],
            [["r1", g, 10], ["r2", g, 20]],
        )
        _write_tsv_gz(d / f"{g}_kraken.tsv.gz", ["taxid", "group"], [[1, g]])
        if with_clade:
            _write_tsv_gz(d / f"{g}_clade_counts.tsv.gz", ["taxid", "group"], [[1, g]])
        return d

    @pytest.mark.parametrize(
        ("with_clade", "platform"), [(True, "illumina"), (False, "ont")]
    )
    def test_platform_inference(
        self, tmp_path: Path, with_clade: bool, platform: str
    ) -> None:
        results = self._make_results(tmp_path / platform, with_clade=with_clade)
        manifest = cdr.discover_side(results, self.KNOWN)
        assert manifest["G1"].platform == platform
        assert manifest["G1"].files["validation_hits"].columns == [
            "seq_id",
            "group",
            "aligner_taxid_lca",
        ]

    def test_group_discovered_without_validation_hits(self, tmp_path: Path) -> None:
        # A group missing its validation_hits is still discovered from other
        # outputs (so the absence can be reported, not the whole group lost).
        d = tmp_path / "partial"
        d.mkdir()
        _write_tsv_gz(d / "G1_kraken.tsv.gz", ["taxid", "group"], [[1, "G1"]])
        _write_tsv_gz(d / "G1_clade_counts.tsv.gz", ["taxid", "group"], [[1, "G1"]])
        manifest = cdr.discover_side(d, self.KNOWN)
        assert "G1" in manifest
        assert "validation_hits" not in manifest["G1"].files

    def test_no_recognized_files_raises(self, tmp_path: Path) -> None:
        d = tmp_path / "empty"
        d.mkdir()
        _write_tsv_gz(d / "stray_file.tsv.gz", ["x"], [[1]])
        with pytest.raises(ValueError, match="No recognized"):
            cdr.discover_side(d, self.KNOWN)


def test_parse_taxonomy_nodes(tmp_path: Path) -> None:
    dmp = tmp_path / "nodes.dmp"
    # Real nodes.dmp rows carry further '\t|\t'-separated fields after rank
    # (embl code, division, ...) then a trailing '\t|'; include one so rank is
    # not the last split element.
    dmp.write_text(
        "1\t|\t1\t|\tno rank\t|\tXX\t|\n"
        "10239\t|\t1\t|\tacellular root\t|\tXX\t|\n"
        "100\t|\t10239\t|\trealm\t|\tXX\t|\n"
    )
    parent, rank = cdr.parse_taxonomy_nodes(dmp)
    assert parent[100] == 10239
    assert rank[10239] == "acellular root"
    assert parent[1] == 1


def test_load_schema_columns(tmp_path: Path) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    (schema_dir / "kraken.schema.json").write_text(
        '{"fields": [{"name": "taxid"}, {"name": "name"}]}'
    )
    cols = cdr.load_schema_columns(schema_dir)
    assert cols["kraken"] == ["taxid", "name"]


def test_expected_downstream_types(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        "[tool.mgs-workflow]\n"
        "expected-outputs-downstream = ["
        '"results_downstream/{GROUP}_clade_counts.tsv.gz", '
        '"results_downstream/{GROUP}_kraken.tsv.gz", '
        '"logging_downstream/pyproject.toml"]\n'
        "expected-outputs-downstream-ont = ["
        '"results_downstream/{GROUP}_kraken.tsv.gz"]\n'
    )
    types = cdr.expected_downstream_types(pyproject)
    assert types["illumina"] == {"clade_counts", "kraken"}
    assert types["ont"] == {"kraken"}
    # logging_downstream entries are not per-group result types.
    assert "pyproject" not in types["illumina"]


def test_load_qc_basic_stats_adds_platform(tmp_path: Path) -> None:
    d = tmp_path / "res"
    d.mkdir()
    _write_tsv_gz(
        d / "G1_validation_hits.tsv.gz",
        ["seq_id", "group", "aligner_taxid_lca"],
        [["r1", "G1", 10]],
    )
    _write_tsv_gz(d / "G1_clade_counts.tsv.gz", ["taxid", "group"], [[1, "G1"]])
    _write_tsv_gz(
        d / "G1_qc_basic_stats_raw.tsv.gz",
        ["mean_seq_len", "stage", "sample", "group"],
        [[150, "raw", "G1", "G1"]],
    )
    manifest = cdr.discover_side(
        d, {"validation_hits", "clade_counts", "qc_basic_stats_raw"}
    )
    qc = cdr.load_qc_basic_stats(d, manifest)
    assert "platform" in qc.columns
    assert qc.iloc[0]["platform"] == "illumina"


def test_read_tsv_handles_leading_quote(tmp_path: Path) -> None:
    # Quoting disabled: a field beginning with a quote must not be swallowed.
    path = tmp_path / "q.tsv"
    path.write_text('a\tb\n"x\ty\n')
    df = cdr.read_tsv(path)
    assert df.iloc[0]["a"] == '"x'


class TestReadPipelineVersion:
    def test_override_wins_without_probing(self, tmp_path: Path) -> None:
        # No files written: an override must be returned verbatim regardless.
        assert (
            cdr.read_pipeline_version(str(tmp_path), tmp_path / "_m", override="9.9.9")
            == "9.9.9"
        )

    @pytest.mark.parametrize(
        ("relative_path", "contents", "expected"),
        [
            (
                "logging/pyproject.toml",
                '[project]\nversion = "3.2.1.5"\n',
                "3.2.1.5",
            ),
            ("logging_downstream/pipeline-version.txt", "2.8.1.2\n", "2.8.1.2"),
        ],
    )
    def test_reads_version_file(
        self, tmp_path: Path, relative_path: str, contents: str, expected: str
    ) -> None:
        path = tmp_path / relative_path
        path.parent.mkdir()
        path.write_text(contents)
        assert cdr.read_pipeline_version(str(tmp_path), tmp_path / "_m") == expected

    def test_returns_none_when_no_version_present(self, tmp_path: Path) -> None:
        assert cdr.read_pipeline_version(str(tmp_path), tmp_path / "_m") is None


def _build_downstream_tree(root: Path, side: str) -> None:
    """Write a tiny synthetic results_downstream/ tree for one side.

    One short-read group (G_ILL) and one ONT group (G_ONT), with wholly invented
    taxids/names. bracken and fastp are omitted so the run exercises the
    'expected output missing on both sides' path; duplicate_stats is written
    header-only to exercise the zero-row path. `side` ('reference' or 'candidate') tweaks
    one read's assignment to create a reassignment in candidate.
    """
    d = root / "results_downstream"
    d.mkdir(parents=True, exist_ok=True)
    seq2_taxid = (
        10 if side == "reference" else 20
    )  # candidate reassigns read 2: 10 -> 20

    for group in ("G_ILL", "G_ONT"):
        # Short-read validation_hits carry prim_align_dup_exemplar (each read is
        # its own exemplar here); ONT has no duplicate marking, so omit it there.
        if group == "G_ILL":
            cols = [
                "seq_id",
                "sample",
                "aligner_taxid_lca",
                "group",
                "validation_distance_aligner",
                "prim_align_dup_exemplar",
            ]
            rows = [
                ["r1", group, 10, group, 0, "r1"],
                ["r2", group, seq2_taxid, group, 1, "r2"],
            ]
        else:
            cols = [
                "seq_id",
                "sample",
                "aligner_taxid_lca",
                "group",
                "validation_distance_aligner",
            ]
            rows = [
                ["r1", group, 10, group, 0],
                ["r2", group, seq2_taxid, group, 1],
            ]
        _write_tsv_gz(d / f"{group}_validation_hits.tsv.gz", cols, rows)
        _write_tsv_gz(
            d / f"{group}_kraken.tsv.gz",
            ["group", "ribosomal", "rank", "taxid", "name", "n_reads_clade"],
            [
                [group, "FALSE", "S", 10, "sp10", 80 if side == "reference" else 60],
                [group, "FALSE", "S", 20, "sp20", 20 if side == "reference" else 40],
                [group, "TRUE", "S", 10, "sp10", 50],
            ],
        )
        for stage in ("raw", "cleaned"):
            _write_tsv_gz(
                d / f"{group}_qc_basic_stats_{stage}.tsv.gz",
                [
                    "percent_gc",
                    "mean_seq_len",
                    "n_reads_single",
                    "n_read_pairs",
                    "percent_duplicates",
                    "n_bases_approx",
                    "per_base_sequence_quality",
                    "stage",
                    "sample",
                    "group",
                ],
                [[45, 150, 1000, 500, 10.0, 150000, "pass", stage, group, group]],
            )
        _write_tsv_gz(
            d / f"{group}_read_counts.tsv.gz",
            ["sample", "n_reads_single", "n_read_pairs", "group"],
            [[group, 1000, 500, group]],
        )
    # clade_counts only for the short-read group (marks it Illumina).
    _write_tsv_gz(
        root / "results_downstream" / "G_ILL_clade_counts.tsv.gz",
        [
            "group",
            "taxid",
            "parent_taxid",
            "reads_direct_total",
            "reads_direct_dedup",
            "reads_clade_total",
            "reads_clade_dedup",
        ],
        [
            # Viruses root: total viral reads (the clade-share denominator).
            ["G_ILL", 10239, 1, 0, 0, 100, 100],
            ["G_ILL", 5, 10239, 0, 0, 100, 100],  # family FamX
            ["G_ILL", 10, 5, 80, 80, 80, 80],  # species
        ],
    )


def _build_index(root: Path) -> None:
    """Write a tiny synthetic index (taxonomy-nodes.dmp + annotated viral DB)."""
    res = root / "output" / "results"
    res.mkdir(parents=True, exist_ok=True)
    (res / "taxonomy-nodes.dmp").write_text(
        "1\t|\t1\t|\tno rank\t|\tXX\t|\n"
        "10239\t|\t1\t|\tacellular root\t|\tXX\t|\n"
        "5\t|\t10239\t|\tfamily\t|\tXX\t|\n"
        "10\t|\t5\t|\tspecies\t|\tXX\t|\n"
        "20\t|\t5\t|\tspecies\t|\tXX\t|\n"
    )
    ann = res / "total-virus-db-annotated.tsv.gz"
    header = ["taxid", "name", "rank", "taxid_species", "infection_status_vertebrate"]
    rows = [
        [5, "FamX", "family", 5, 1],
        [10, "sp10", "species", 10, 1],
        [20, "sp20", "species", 20, 1],
    ]
    with gzip.open(ann, "wt") as fh:
        fh.write("\t".join(header) + "\n")
        for r in rows:
            fh.write("\t".join(str(v) for v in r) + "\n")


def _build_comparison(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    reference_root = tmp_path / "reference"
    candidate_root = tmp_path / "candidate"
    index_root = tmp_path / "index"
    out = tmp_path / "out"
    _build_downstream_tree(reference_root, "reference")
    _build_downstream_tree(candidate_root, "candidate")
    _build_index(index_root)
    for root, version in ((reference_root, "1.2.3.4"), (candidate_root, "1.2.3.5-dev")):
        (root / "logging").mkdir(parents=True, exist_ok=True)
        (root / "logging" / "pyproject.toml").write_text(
            f'[project]\nversion = "{version}"\n'
        )
    return reference_root, candidate_root, index_root, out


def _run_comparison(
    monkeypatch: pytest.MonkeyPatch,
    reference_root: Path,
    candidate_root: Path,
    index_root: Path,
    out: Path,
) -> None:
    import sys

    argv = [
        "compare_downstream_runs.py",
        "--reference",
        str(reference_root),
        "--candidate",
        str(candidate_root),
        "--candidate-index",
        str(index_root),
        "--reference-index",
        str(index_root),
        "--out",
        str(out),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    cdr.main()


def test_main_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reference_root, candidate_root, index_root, out = _build_comparison(tmp_path)
    _run_comparison(monkeypatch, reference_root, candidate_root, index_root, out)

    import pandas as pd

    # Core outputs exist, including pair counts and status flips.
    for name in (
        "flags.tsv",
        "file_inventory.tsv",
        "column_conformance.tsv",
        "qc_numeric.tsv",
        "qc_survival.tsv",
        "kraken_bray_curtis.tsv",
        "viral_read_status.tsv",
        "viral_reassignment_buckets.tsv",
        "viral_reassignment_pairs.tsv",
        "clade_rank_shares.tsv",
        "vertebrate_status_flips.tsv",
        "run_identity.tsv",
        "viral_validation_agreement_by_taxon.tsv",
    ):
        assert (out / name).exists(), f"missing {name}"

    # Run identity carries the auto-detected pipeline versions from logging/.
    ident = pd.read_csv(out / "run_identity.tsv", sep="\t")
    versions = dict(zip(ident.side, ident.pipeline_version, strict=True))
    assert versions["reference"] == "1.2.3.4"
    assert versions["candidate"] == "1.2.3.5-dev"

    inv = pd.read_csv(out / "file_inventory.tsv", sep="\t")
    # Platform inference: G_ILL short-read, G_ONT ONT.
    assert inv[inv.group == "G_ILL"].platform.iloc[0] == "illumina"
    assert inv[inv.group == "G_ONT"].platform.iloc[0] == "ont"
    # C2: bracken is expected for Illumina but absent on BOTH sides -> still a row.
    brk = inv[(inv.group == "G_ILL") & (inv.file_type == "bracken")]
    assert len(brk) == 1
    assert not bool(brk.iloc[0].in_reference) and not bool(brk.iloc[0].in_candidate)
    hits = inv[(inv.group == "G_ILL") & (inv.file_type == "validation_hits")].iloc[0]
    assert hits.n_rows_reference == 2
    assert hits.n_rows_candidate == 2
    assert hits.row_delta == 0

    # The candidate reassignment (read r2: 10 -> 20, same family) is captured.
    status = pd.read_csv(out / "viral_read_status.tsv", sep="\t")
    gil = status[(status.group == "G_ILL") & (status.scope == "vertebrate")].iloc[0]
    assert gil.n_reassigned == 1
    buckets = pd.read_csv(out / "viral_reassignment_buckets.tsv", sep="\t")
    assert (buckets.bucket == "same-family").any()

    # Flag contents: 50% reassignment (> 10% threshold) is flagged for G_ILL.
    flags = pd.read_csv(out / "flags.tsv", sep="\t")
    assert not flags.empty
    reassign_flags = flags[flags.metric.str.contains("reassigned")]
    assert any("G_ILL" in k for k in reassign_flags.key)


def test_main_skips_focus1_when_validation_hits_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reference_root, candidate_root, index_root, out = _build_comparison(tmp_path)
    # Remove ALL validation_hits on the candidate side: Focus 1 must skip cleanly, not
    # crash, and still produce the other focuses + flags.
    for vh in (candidate_root / "results_downstream").glob("*_validation_hits.tsv.gz"):
        vh.unlink()
    _run_comparison(monkeypatch, reference_root, candidate_root, index_root, out)

    assert (out / "flags.tsv").exists()
    assert (out / "file_inventory.tsv").exists()
    # Focus 1 read-level output is skipped (not computed).
    assert not (out / "viral_read_status.tsv").exists()
    # BLAST agreement needs validation_hits too, so it is skipped as well.
    assert not (out / "viral_validation_agreement.tsv").exists()
    # But the analyses that do NOT need read-level matching still run.
    assert (out / "clade_rank_shares.tsv").exists()
    assert (out / "vertebrate_status_flips.tsv").exists()


def test_main_per_group_one_sided_input_is_skipped_not_fabricated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C1: a per-group input missing on one side is skipped, not misread.

    Removing G_ONT's validation_hits and G_ILL's kraken on the candidate side must:
    drop those groups from the affected metric (recorded in skipped_groups.tsv),
    NOT fabricate a lost/Bray-Curtis flag for them, and still compute the other
    group's metric plus the independent viral outputs (clade, status flips).
    """
    reference_root, candidate_root, index_root, out = _build_comparison(tmp_path)
    # One-sided per-group absences (candidate side only).
    (candidate_root / "results_downstream" / "G_ONT_validation_hits.tsv.gz").unlink()
    (candidate_root / "results_downstream" / "G_ILL_kraken.tsv.gz").unlink()
    _run_comparison(monkeypatch, reference_root, candidate_root, index_root, out)

    import pandas as pd

    skipped = pd.read_csv(out / "skipped_groups.tsv", sep="\t")
    skipped_pairs = set(zip(skipped.metric, skipped.group, strict=True))
    assert ("validation_hits", "G_ONT") in skipped_pairs
    assert ("kraken", "G_ILL") in skipped_pairs

    # Read-level join: G_ILL computed (both sides), G_ONT absent (skipped, not
    # fabricated as fully lost).
    status = pd.read_csv(out / "viral_read_status.tsv", sep="\t")
    assert "G_ILL" in set(status.group)
    assert "G_ONT" not in set(status.group)

    # Bray-Curtis: G_ONT computed (both sides), G_ILL absent (skipped, not a
    # fabricated 1.0).
    bray = pd.read_csv(out / "kraken_bray_curtis.tsv", sep="\t")
    assert "G_ONT" in set(bray.group)
    assert "G_ILL" not in set(bray.group)

    # No fabricated flag mentions a skipped group's dropped metric.
    flags = pd.read_csv(out / "flags.tsv", sep="\t")
    if not flags.empty:
        bray_flag_keys = flags[flags.metric.str.contains("bray")].key.astype(str)
        assert not any("G_ILL" in k for k in bray_flag_keys)

    # Independent viral outputs still produced for the group that has its inputs.
    assert (out / "clade_rank_shares.tsv").exists()
    clade = pd.read_csv(out / "clade_rank_shares.tsv", sep="\t")
    assert "G_ILL" in set(clade.group)
    assert (out / "vertebrate_status_flips.tsv").exists()
    # Pair-count table (C2) is still emitted for the computed read-level group.
    assert (out / "viral_reassignment_pairs.tsv").exists()
