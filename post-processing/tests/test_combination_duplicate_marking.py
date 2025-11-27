#!/usr/bin/env python3
"""Tests for combination_duplicate_marking.py"""

import sys
from pathlib import Path

import pytest
import csv
import gzip

# Add bin directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "bin"))

import combination_duplicate_marking as cdm


@pytest.fixture
def tsv_factory(tmp_path):
    """Factory fixture for TSV file operations.

    Provides methods to create and read both plain and gzipped TSV files.
    """

    class TSVFactory:
        def __init__(self, tmp_path):
            self.tmp_path = tmp_path

        def create_plain(self, filename, content):
            """Create a plain TSV file with the given content."""
            file_path = self.tmp_path / filename
            file_path.write_text(content)
            return str(file_path)

        def create_gzip(self, filename, content):
            """Create a gzipped TSV file with the given content."""
            file_path = self.tmp_path / filename
            with gzip.open(file_path, "wt") as f:
                f.write(content)
            return str(file_path)

        def read_plain(self, filepath):
            """Read content from a plain TSV file."""
            return Path(filepath).read_text()

        def read_gzip(self, filepath):
            """Read content from a gzipped TSV file."""
            with gzip.open(filepath, "rt") as f:
                return f.read()

        def get_path(self, filename):
            """Get the full path for a file in the temp directory."""
            return str(self.tmp_path / filename)

    return TSVFactory(tmp_path)


class TestReadDedupColumns:
    """Test reading only the columns needed for deduplication."""

    def test_basic_reading(self, tsv_factory):
        """Test reading basic deduplication columns from TSV."""
        content = (
            "seq_id\tquery_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\t"
            "prim_align_dup_exemplar\textra_col\n"
            "read1\tACGT\tTGCA\tIIII\tIIII\tread1\textra1\n"
            "read2\tGGGG\tCCCC\tHHHH\tHHHH\tread2\textra2\n"
        )
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)

        read_pairs, prim_align_exemplars = cdm.read_dedup_columns(input_file)

        assert len(read_pairs) == 2
        assert read_pairs["read1"].read_id == "read1"
        assert read_pairs["read1"].fwd_seq == "ACGT"
        assert read_pairs["read1"].rev_seq == "TGCA"
        assert prim_align_exemplars["read1"] == "read1"

    def test_missing_columns(self, tsv_factory):
        """Test that missing required columns raise KeyError."""
        content = "seq_id\n" "read1\n"
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)

        with pytest.raises(KeyError):
            cdm.read_dedup_columns(input_file)


class TestValidateExemplars:
    """Test validation that all prim_align_dup_exemplar values exist."""

    def test_valid_exemplars(self):
        """Test that valid exemplars pass validation."""
        read_pairs = {
            "read1": cdm.ReadPair("read1", "ACGT", "TGCA", "IIII", "IIII"),
            "read2": cdm.ReadPair("read2", "GGGG", "CCCC", "HHHH", "HHHH"),
            "read3": cdm.ReadPair("read3", "TTTT", "AAAA", "JJJJ", "JJJJ"),
        }
        prim_align_exemplars = {
            "read1": "read1",
            "read2": "read1",
            "read3": "read3",
        }

        # Should not raise
        cdm.validate_exemplars(read_pairs, prim_align_exemplars)

    def test_missing_exemplar(self):
        """Test that missing exemplar raises ValueError."""
        read_pairs = {
            "read1": cdm.ReadPair("read1", "ACGT", "TGCA", "IIII", "IIII"),
            "read2": cdm.ReadPair("read2", "GGGG", "CCCC", "HHHH", "HHHH"),
        }
        prim_align_exemplars = {
            "read1": "read1",
            "read2": "nonexistent",
        }

        with pytest.raises(ValueError, match="not found in file"):
            cdm.validate_exemplars(read_pairs, prim_align_exemplars)


class TestBuildPrimAlignGroups:
    """Test grouping reads by prim_align_dup_exemplar."""

    def test_basic_grouping(self):
        """Test basic grouping by exemplar."""
        prim_align_exemplars = {
            "read1": "read1",
            "read2": "read1",
            "read3": "read3",
            "read4": "read3",
        }

        groups = cdm.build_prim_align_groups(prim_align_exemplars)

        assert len(groups) == 2
        assert groups["read1"] == {"read1", "read2"}
        assert groups["read3"] == {"read3", "read4"}


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_no_duplicates(self, tsv_factory):
        """Test case where no reads are duplicates."""
        content = (
            "seq_id\tquery_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\t"
            "prim_align_dup_exemplar\n"
            "read1\tAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\t"
            "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT\t"
            "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\t"
            "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\t"
            "read1\n"
            "read2\tGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG\t"
            "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\t"
            "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH\t"
            "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH\t"
            "read2\n"
        )
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        cdm.main.__wrapped__ = lambda: None  # Avoid sys.exit
        sys.argv = ["combination_duplicate_marking.py", input_file, output_file]

        # Run the full pipeline
        read_pairs, prim_align_exemplars = cdm.read_dedup_columns(input_file)
        cdm.validate_exemplars(read_pairs, prim_align_exemplars)
        prim_align_groups = cdm.build_prim_align_groups(prim_align_exemplars)
        similarity_exemplars = cdm.run_similarity_dedup(read_pairs)
        merged_groups = cdm.merge_groups_by_similarity(prim_align_groups, similarity_exemplars)
        combined_exemplars = cdm.select_final_exemplars(merged_groups, read_pairs, similarity_exemplars)
        cdm.write_output_with_combined_column(input_file, output_file, combined_exemplars)

        # Verify output
        result = tsv_factory.read_gzip(output_file)
        lines = result.strip().split("\n")

        # Check header
        assert "combined_dup_exemplar" in lines[0]

        # Parse output
        with gzip.open(output_file, 'rt') as f:
            reader = csv.DictReader(f, delimiter='\t')
            rows = list(reader)

        # Each read should be its own exemplar
        assert rows[0]["combined_dup_exemplar"] == "read1"
        assert rows[1]["combined_dup_exemplar"] == "read2"

    def test_alignment_duplicates_only(self, tsv_factory):
        """Test case with only alignment-based duplicates."""
        # Use more realistic sequences that are clearly different
        seq1 = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT"
        seq2 = "TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCA"
        seq3 = "GGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCCGGCC"
        qual = "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII"

        content = (
            "seq_id\tquery_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\t"
            "prim_align_dup_exemplar\n"
            # Group 1: read1 and read2 are alignment duplicates
            f"read1\t{seq1}\t{seq1}\t{qual}\t{qual}\tread1\n"
            f"read2\t{seq2}\t{seq2}\t{qual}\t{qual}\tread1\n"  # alignment duplicate of read1
            # Group 2: read3 is its own group
            f"read3\t{seq3}\t{seq3}\t{qual}\t{qual}\tread3\n"
        )
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        # Run the full pipeline
        read_pairs, prim_align_exemplars = cdm.read_dedup_columns(input_file)
        cdm.validate_exemplars(read_pairs, prim_align_exemplars)
        prim_align_groups = cdm.build_prim_align_groups(prim_align_exemplars)
        similarity_exemplars = cdm.run_similarity_dedup(read_pairs)
        merged_groups = cdm.merge_groups_by_similarity(prim_align_groups, similarity_exemplars)
        combined_exemplars = cdm.select_final_exemplars(merged_groups, read_pairs, similarity_exemplars)
        cdm.write_output_with_combined_column(input_file, output_file, combined_exemplars)

        # Verify output
        with gzip.open(output_file, 'rt') as f:
            reader = csv.DictReader(f, delimiter='\t')
            rows = list(reader)

        # read1 and read2 should have the same combined exemplar
        assert rows[0]["combined_dup_exemplar"] == rows[1]["combined_dup_exemplar"]
        # read3 should be its own exemplar
        assert rows[2]["combined_dup_exemplar"] == "read3"

    def test_similarity_merges_alignment_groups(self, tsv_factory):
        """Test that similarity-based dedup merges alignment groups."""
        # Create two alignment groups where one member from each is very similar
        content = (
            "seq_id\tquery_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\t"
            "prim_align_dup_exemplar\n"
            # Group 1: read1 and read2 are alignment duplicates
            "read1\tAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\t"
            "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT\t"
            "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\t"
            "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\t"
            "read1\n"
            "read2\tCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\t"
            "GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG\t"
            "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH\t"
            "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH\t"
            "read1\n"
            # Group 2: read3 and read4 are alignment duplicates
            # read3 is very similar to read1 (identical in this case)
            "read3\tAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\t"
            "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT\t"
            "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\t"
            "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\t"
            "read3\n"
            "read4\tGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG\t"
            "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\t"
            "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH\t"
            "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH\t"
            "read3\n"
        )
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        # Run the full pipeline
        read_pairs, prim_align_exemplars = cdm.read_dedup_columns(input_file)
        cdm.validate_exemplars(read_pairs, prim_align_exemplars)
        prim_align_groups = cdm.build_prim_align_groups(prim_align_exemplars)
        similarity_exemplars = cdm.run_similarity_dedup(read_pairs)
        merged_groups = cdm.merge_groups_by_similarity(prim_align_groups, similarity_exemplars)
        combined_exemplars = cdm.select_final_exemplars(merged_groups, read_pairs, similarity_exemplars)
        cdm.write_output_with_combined_column(input_file, output_file, combined_exemplars)

        # Verify output
        with gzip.open(output_file, 'rt') as f:
            reader = csv.DictReader(f, delimiter='\t')
            rows = list(reader)

        # All four reads should have the same combined exemplar
        # because read1 and read3 are similar, merging the two alignment groups
        exemplar = rows[0]["combined_dup_exemplar"]
        assert rows[1]["combined_dup_exemplar"] == exemplar
        assert rows[2]["combined_dup_exemplar"] == exemplar
        assert rows[3]["combined_dup_exemplar"] == exemplar
