"""Tests for add_genbank_genome_ids.py."""

import csv
import gzip
from pathlib import Path

import pandas as pd
import pytest
from add_genbank_genome_ids import (
    add_genome_ids,
    extract_genome_ids,
    stage_genomes_parallel,
)

META_HEADER = ["assembly_accession", "taxid", "local_filename"]


def _write_fna_gz(path: Path, records: list[tuple[str, str]]) -> None:
    """Write a list of (header, sequence) records to a gzipped FASTA file."""
    with gzip.open(path, "wt") as f:
        for header, seq in records:
            f.write(f">{header}\n{seq}\n")


def _write_metadata(path: Path, rows: list[list[str]]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(META_HEADER)
        w.writerows(rows)


@pytest.fixture()
def two_assemblies(tmp_path: Path) -> tuple[Path, Path]:
    """Two assemblies: one single-segment, one three-segment. Returns (metadata, source_dir)."""
    src = tmp_path / "src"
    src.mkdir()
    _write_fna_gz(src / "GCA_001.fna.gz", [("seq1 organism A", "ACGT")])
    _write_fna_gz(src / "GCA_002.fna.gz", [
        ("seq2a organism B segment 1", "AAAA"),
        ("seq2b organism B segment 2", "CCCC"),
        ("seq2c organism B segment 3", "GGGG"),
    ])
    meta_path = tmp_path / "meta.tsv"
    _write_metadata(meta_path, [
        ["GCA_001.1", "1001", str(src / "GCA_001.fna.gz")],
        ["GCA_002.1", "1002", str(src / "GCA_002.fna.gz")],
    ])
    return meta_path, src


class TestStageGenomesParallel:
    def test_copies_all_files_into_staged_dir(self, tmp_path: Path, two_assemblies: tuple[Path, Path]) -> None:
        """Source files are present in staged_dir under their original basenames."""
        _, src = two_assemblies
        sources = [str(src / "GCA_001.fna.gz"), str(src / "GCA_002.fna.gz")]
        staged = tmp_path / "staged"
        stage_genomes_parallel(sources, staged, parallelism=2)
        assert sorted(p.name for p in staged.iterdir()) == ["GCA_001.fna.gz", "GCA_002.fna.gz"]

    def test_raises_on_missing_source(self, tmp_path: Path) -> None:
        """cp failure on a non-existent source surfaces as CalledProcessError."""
        import subprocess
        with pytest.raises(subprocess.CalledProcessError):
            stage_genomes_parallel(["/nonexistent/file.fna.gz"], tmp_path / "staged", parallelism=1)


class TestExtractGenomeIDs:
    def test_collects_ids_in_input_order(self, tmp_path: Path, two_assemblies: tuple[Path, Path]) -> None:
        """One inner list per input file; segments appear in FASTA order."""
        _, src = two_assemblies
        sources = [str(src / "GCA_001.fna.gz"), str(src / "GCA_002.fna.gz")]
        # extract_genome_ids reads from staged_dir; reuse src as the staged dir here.
        result = extract_genome_ids(sources, src)
        assert result == [["seq1"], ["seq2a", "seq2b", "seq2c"]]


class TestAddGenomeIDs:
    def test_expands_metadata_with_one_row_per_segment(
        self, tmp_path: Path, two_assemblies: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """End-to-end: 2 input rows + (1+3 segments) yields 4 output rows.
        `add_genome_ids` writes its `staged/` dir relative to cwd, mirroring how
        the script runs in a Nextflow task working dir."""
        meta_path, _ = two_assemblies
        monkeypatch.chdir(tmp_path)
        out_path = tmp_path / "out.tsv.gz"
        add_genome_ids(str(meta_path), str(out_path), parallelism=2)
        df = pd.read_csv(out_path, sep="\t", dtype=str)
        assert list(df.columns) == [*META_HEADER, "genome_id"]
        assert list(df["assembly_accession"]) == ["GCA_001.1", "GCA_002.1", "GCA_002.1", "GCA_002.1"]
        assert list(df["genome_id"]) == ["seq1", "seq2a", "seq2b", "seq2c"]
