"""Integration tests for compare_downstream_runs.py (munging/orchestration).

These exercise the file-discovery and parsing logic on small synthetic inputs
written to tmp_path -- never real delivery data and never network. The numeric
calculations are tested separately in test_downstream_metrics.py.
"""

import gzip
from pathlib import Path

import compare_downstream_runs as cdr


def _write_tsv_gz(path: Path, header: list[str], rows: list[list]) -> None:
    lines = ["\t".join(header)]
    lines += ["\t".join(str(v) for v in r) for r in rows]
    with gzip.open(path, "wt") as fh:
        fh.write("\n".join(lines) + "\n")


###########################
# _strip_group_prefix     #
###########################


class TestStripGroupPrefix:
    GROUPS = ["CA_Riverside_20250814", "PZ-251126-Copl-NAS-P1"]

    def test_underscore_group_matched_by_longest(self):
        # Group names contain underscores; must not split naively.
        got = cdr._strip_group_prefix(
            "CA_Riverside_20250814_qc_basic_stats_raw.tsv.gz", self.GROUPS
        )
        assert got == ("CA_Riverside_20250814", "qc_basic_stats_raw")

    def test_json_suffix_stripped(self):
        got = cdr._strip_group_prefix("PZ-251126-Copl-NAS-P1_fastp.json", self.GROUPS)
        assert got == ("PZ-251126-Copl-NAS-P1", "fastp")

    def test_unknown_prefix_returns_none(self):
        assert cdr._strip_group_prefix("stray_file.tsv.gz", self.GROUPS) is None


###########################
# discover_side           #
###########################


class TestDiscoverSide:
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

    def test_illumina_inferred_from_clade_counts(self, tmp_path):
        results = self._make_results(tmp_path / "ill", with_clade=True)
        manifest = cdr.discover_side(results)
        assert manifest["G1"].platform == "illumina"
        assert manifest["G1"].files["validation_hits"].n_rows == 2
        assert manifest["G1"].files["validation_hits"].columns == [
            "seq_id",
            "group",
            "aligner_taxid_lca",
        ]

    def test_ont_inferred_when_no_clade_counts(self, tmp_path):
        results = self._make_results(tmp_path / "ont", with_clade=False)
        manifest = cdr.discover_side(results)
        assert manifest["G1"].platform == "ont"

    def test_no_validation_hits_raises(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        _write_tsv_gz(d / "G1_kraken.tsv.gz", ["taxid"], [[1]])
        try:
            cdr.discover_side(d)
            raise AssertionError("expected ValueError")
        except ValueError:
            pass


###########################
# parse_taxonomy_nodes    #
###########################


def test_parse_taxonomy_nodes(tmp_path):
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


###########################
# schema / expected types #
###########################


def test_load_schema_columns(tmp_path):
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    (schema_dir / "kraken.schema.json").write_text(
        '{"fields": [{"name": "taxid"}, {"name": "name"}]}'
    )
    cols = cdr.load_schema_columns(schema_dir)
    assert cols["kraken"] == ["taxid", "name"]


def test_expected_downstream_types(tmp_path):
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


###########################
# loaders                 #
###########################


def test_load_qc_basic_stats_adds_platform(tmp_path):
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
    manifest = cdr.discover_side(d)
    qc = cdr.load_qc_basic_stats(d, manifest)
    assert "platform" in qc.columns
    assert qc.iloc[0]["platform"] == "illumina"


def test_read_tsv_handles_leading_quote(tmp_path):
    # Quoting disabled: a field beginning with a quote must not be swallowed.
    path = tmp_path / "q.tsv"
    path.write_text('a\tb\n"x\ty\n')
    df = cdr.read_tsv(path)
    assert df.iloc[0]["a"] == '"x'
