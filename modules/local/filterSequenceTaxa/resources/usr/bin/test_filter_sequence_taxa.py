"""Tests for filter_sequence_taxa.py."""

import io
import sys
from pathlib import Path

import pandas as pd
import pytest
from filter_sequence_taxa import (
    descendant_taxids,
    filter_sequence_taxa,
    main,
    read_parent_map,
)

# A tiny taxonomy: flu family 11308 -> genus 11320 -> strain 11520; an unrelated
# lineage 11118 -> 694009 -> 2697049 (SARS-CoV-2); root 1 self-parents.
PARENT_OF = {
    "1": "1",
    "11308": "11157",
    "11320": "11308",
    "11520": "11320",
    "11118": "1",
    "694009": "11118",
    "2697049": "694009",
}

META_COLS = [
    "assembly_accession",
    "taxid",
    "organism_name",
    "source_database",
    "assembly_status",
    "release_date",
]


def _meta(rows: list[tuple[str, str]]) -> pd.DataFrame:
    """Build a metadata DataFrame from (accession, taxid) tuples."""
    return pd.DataFrame(
        [
            (acc, taxid, "Org", "SOURCE_DATABASE_REFSEQ", "", "2024-01-01")
            for acc, taxid in rows
        ],
        columns=META_COLS,
    )


class TestDescendantTaxids:
    def test_includes_root_and_all_descendants(self) -> None:
        """The clade rooted at flu (11308) is itself plus its descendants."""
        assert descendant_taxids(PARENT_OF, "11308") == {"11308", "11320", "11520"}

    def test_excludes_other_lineages(self) -> None:
        """A separate lineage (SARS-CoV-2, 2697049) is not in the flu clade."""
        assert "2697049" not in descendant_taxids(PARENT_OF, "11308")

    def test_leaf_root_returns_just_itself(self) -> None:
        """A leaf taxid's clade is only the leaf."""
        assert descendant_taxids(PARENT_OF, "11520") == {"11520"}

    def test_self_parent_root_does_not_loop(self) -> None:
        """The NCBI root (taxid 1, self-parent) is cycle-safe and reaches all."""
        result = descendant_taxids(PARENT_OF, "1")
        assert "1" in result and "2697049" in result


class TestFilterSequenceTaxa:
    def test_drops_excluded_clade_keeps_others(self) -> None:
        """Flu-taxid rows are dropped; the non-flu row is kept."""
        meta = _meta(
            [("NC_FLU1.1", "11320"), ("NC_FLU2.1", "11520"), ("NC_SARS.1", "2697049")]
        )
        exclude = descendant_taxids(PARENT_OF, "11308")
        result = filter_sequence_taxa(meta, exclude)
        assert list(result["assembly_accession"]) == ["NC_SARS.1"]

    def test_empty_input_yields_header_only(self) -> None:
        """An empty metadata frame stays empty with its columns preserved."""
        empty = _meta([])
        result = filter_sequence_taxa(empty, {"11308"})
        assert len(result) == 0
        assert list(result.columns) == META_COLS


class TestReadParentMap:
    def test_reads_nodes_dmp_columns_0_and_2(self, tmp_path: Path) -> None:
        """nodes.dmp fields are `\\t|\\t`-separated; taxid is col 0, parent col 2."""
        nodes = tmp_path / "nodes.dmp"
        nodes.write_text(
            "11320\t|\t11308\t|\tgenus\t|\t\t|\n11308\t|\t11157\t|\tfamily\t|\t\t|\n"
        )
        assert read_parent_map(str(nodes)) == {"11320": "11308", "11308": "11157"}


@pytest.mark.parametrize("root,expected_in", [("11308", "11520"), ("11118", "2697049")])
def test_read_then_descend(tmp_path: Path, root: str, expected_in: str) -> None:
    """Integration: build the parent map from a dmp, then descend from a root."""
    lines = "".join(f"{t}\t|\t{p}\t|\tno rank\t|\t\t|\n" for t, p in PARENT_OF.items())
    nodes = io.StringIO(lines)
    dmp = tmp_path / "nodes.dmp"
    dmp.write_text(nodes.getvalue())
    parent_of = read_parent_map(str(dmp))
    assert expected_in in descendant_taxids(parent_of, root)


def test_main_rejects_unknown_exclude_taxid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A typo/obsolete exclude_taxid must fail loudly, not silently no-op."""
    lines = "".join(f"{t}\t|\t{p}\t|\tno rank\t|\t\t|\n" for t, p in PARENT_OF.items())
    dmp = tmp_path / "nodes.dmp"
    dmp.write_text(lines)
    meta = tmp_path / "meta.tsv"
    meta.write_text("assembly_accession\ttaxid\nNC_1.1\t11320\n")
    out = tmp_path / "out.tsv.gz"
    monkeypatch.setattr(
        sys, "argv", ["prog", str(meta), str(dmp), "99999999", str(out)]
    )
    with pytest.raises(ValueError, match="not found in nodes.dmp"):
        main()
