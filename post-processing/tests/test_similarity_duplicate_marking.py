#!/usr/bin/env python3
"""Tests for similarity_duplicate_marking.py"""

import sys
from pathlib import Path

import pytest
import csv
import gzip

# Add bin directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "bin"))

# Add deps to path so we can import nao_dedup
sys.path.insert(0, str(Path(__file__).parent.parent / "deps"))

import similarity_duplicate_marking as sdm
from nao_dedup.dedup import DedupParams, MinimizerParams, \
    deduplicate_read_pairs_streaming


@pytest.fixture
def params():
    """Fixture providing DedupParams for tests."""
    return DedupParams()


@pytest.fixture
def minimizer_params():
    """Fixture providing MinimizerParams for tests."""
    return MinimizerParams(kmer_len=15, window_len=25, num_windows=4)


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


def create_test_tsv_content(rows, extra_cols=None):
    """Helper to create TSV content from a list of read specifications.

    Args:
        rows: List of tuples (seq_id, seq, qual, prim_align_exemplar)
              or dicts with keys: seq_id, seq, qual, prim_align_exemplar, extra_data
        extra_cols: List of extra column names to include in header

    Returns:
        String containing full TSV content with header
    """
    if extra_cols:
        header = (
            "\t".join(extra_cols[:1]) + "\t" if extra_cols else ""
            "seq_id\t"
            + ("\t".join(extra_cols[1:]) + "\t" if len(extra_cols) > 1 else "")
            + "query_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\t"
            "prim_align_dup_exemplar"
            + ("\t" + "\t".join(extra_cols[2:]) if len(extra_cols) > 2 else "")
        )
    else:
        header = (
            "seq_id\tquery_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\t"
            "prim_align_dup_exemplar"
        )

    lines = [header]
    for row in rows:
        if isinstance(row, dict):
            seq_id = row['seq_id']
            seq = row['seq']
            qual = row['qual']
            exemplar = row['prim_align_exemplar']
            extra_data = row.get('extra_data', [])
            line_parts = []
            if extra_data:
                line_parts.extend(extra_data[:1])
            line_parts.append(seq_id)
            if extra_data and len(extra_data) > 1:
                line_parts.extend(extra_data[1:2])
            line_parts.extend([seq, seq, qual, qual, exemplar])
            if extra_data and len(extra_data) > 2:
                line_parts.extend(extra_data[2:])
            lines.append("\t".join(line_parts))
        else:
            seq_id, seq, qual, exemplar = row
            lines.append(f"{seq_id}\t{seq}\t{seq}\t{qual}\t{qual}\t{exemplar}")

    return "\n".join(lines) + "\n"


def run_pipeline_and_get_output(input_file, output_file, params, minimizer_params):
    """Run the full similarity dedup pipeline and return output rows.

    Returns:
        Tuple of (rows, fieldnames) where rows is a list of dicts
    """
    read_pairs = sdm.read_alignment_unique_reads(input_file)
    similarity_exemplars = deduplicate_read_pairs_streaming(
        read_pairs, params, minimizer_params)
    sdm.write_output_with_sim_column(input_file, output_file, similarity_exemplars)

    with gzip.open(output_file, 'rt') as f:
        reader = csv.DictReader(f, delimiter='\t')
        fieldnames = reader.fieldnames
        rows = list(reader)

    return rows, fieldnames


class TestReadAlignmentUniqueReads:
    """Test reading only alignment-unique reads."""

    def test_basic_reading(self, tsv_factory):
        """Test reading only alignment-unique reads from TSV."""
        content = (
            "seq_id\tquery_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\t"
            "prim_align_dup_exemplar\textra_col\n"
            "read1\tACGT\tTGCA\tIIII\tIIII\tread1\textra1\n"
            "read2\tGGGG\tCCCC\tHHHH\tHHHH\tread1\textra2\n"  # Alignment dup
            "read3\tTTTT\tAAAA\tJJJJ\tJJJJ\tread3\textra3\n"
        )
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)

        read_pairs = list(sdm.read_alignment_unique_reads(input_file))

        # Only read1 and read3 should be yielded (alignment-unique)
        assert len(read_pairs) == 2
        assert read_pairs[0].read_id == "read1"
        assert read_pairs[1].read_id == "read3"
        assert read_pairs[0].fwd_seq == "ACGT"
        assert read_pairs[1].fwd_seq == "TTTT"

    def test_all_duplicates(self, tsv_factory):
        """Test file where all reads are alignment duplicates."""
        content = (
            "seq_id\tquery_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\t"
            "prim_align_dup_exemplar\n"
            "read1\tACGT\tTGCA\tIIII\tIIII\tread1\n"
            "read2\tGGGG\tCCCC\tHHHH\tHHHH\tread1\n"
            "read3\tTTTT\tAAAA\tJJJJ\tJJJJ\tread1\n"
        )
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)

        read_pairs = list(sdm.read_alignment_unique_reads(input_file))

        # Only read1 should be yielded
        assert len(read_pairs) == 1
        assert read_pairs[0].read_id == "read1"


class TestCountReturns:
    """Test the counts returned by write_output_with_sim_column."""

    def test_counts_no_duplicates(self, tsv_factory, params, minimizer_params):
        """Test counts when there are no duplicates."""
        seq1 = "A" * 76
        seq2 = "G" * 76
        qual = "I" * 76

        content = create_test_tsv_content([
            ("read1", seq1, qual, "read1"),
            ("read2", seq2, qual, "read2"),
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        read_pairs = sdm.read_alignment_unique_reads(input_file)
        similarity_exemplars = deduplicate_read_pairs_streaming(
            read_pairs, params, minimizer_params)
        n_reads, n_prim_align_dups, n_sim_dups = sdm.write_output_with_sim_column(
            input_file, output_file, similarity_exemplars)

        assert n_reads == 2
        assert n_prim_align_dups == 0
        assert n_sim_dups == 0

    def test_counts_with_alignment_duplicates(self, tsv_factory, params, minimizer_params):
        """Test counts when there are alignment duplicates."""
        seq = "A" * 76
        qual = "I" * 76

        content = create_test_tsv_content([
            ("read1", seq, qual, "read1"),
            ("read2", seq, qual, "read1"),  # alignment dup
            ("read3", seq, qual, "read1"),  # alignment dup
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        read_pairs = sdm.read_alignment_unique_reads(input_file)
        similarity_exemplars = deduplicate_read_pairs_streaming(
            read_pairs, params, minimizer_params)
        n_reads, n_prim_align_dups, n_sim_dups = sdm.write_output_with_sim_column(
            input_file, output_file, similarity_exemplars)

        assert n_reads == 3
        assert n_prim_align_dups == 2  # read2 and read3
        assert n_sim_dups == 0

    def test_counts_with_similarity_duplicates(self, tsv_factory, params, minimizer_params):
        """Test counts when there are similarity duplicates among alignment-unique reads."""
        seq1 = "A" * 76
        seq2 = "G" * 76
        qual = "I" * 76

        content = create_test_tsv_content([
            ("read1", seq1, qual, "read1"),
            ("read2", seq1, qual, "read2"),  # alignment-unique, but similar to read1
            ("read3", seq2, qual, "read3"),
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        read_pairs = sdm.read_alignment_unique_reads(input_file)
        similarity_exemplars = deduplicate_read_pairs_streaming(
            read_pairs, params, minimizer_params)
        n_reads, n_prim_align_dups, n_sim_dups = sdm.write_output_with_sim_column(
            input_file, output_file, similarity_exemplars)

        assert n_reads == 3
        assert n_prim_align_dups == 0
        assert n_sim_dups == 1  # read2 is sim dup of read1

    def test_counts_mixed_duplicates(self, tsv_factory, params, minimizer_params):
        """Test counts with both alignment and similarity duplicates."""
        seq1 = "A" * 76
        seq2 = "G" * 76
        qual = "I" * 76

        content = create_test_tsv_content([
            ("read1", seq1, qual, "read1"),
            ("read2", seq2, qual, "read1"),  # alignment dup
            ("read3", seq1, qual, "read3"),  # alignment-unique, sim dup of read1
            ("read4", seq2, qual, "read4"),  # alignment-unique, different
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        read_pairs = sdm.read_alignment_unique_reads(input_file)
        similarity_exemplars = deduplicate_read_pairs_streaming(
            read_pairs, params, minimizer_params)
        n_reads, n_prim_align_dups, n_sim_dups = sdm.write_output_with_sim_column(
            input_file, output_file, similarity_exemplars)

        assert n_reads == 4
        assert n_prim_align_dups == 1  # read2
        assert n_sim_dups == 1  # read3


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_no_duplicates(self, tsv_factory, params, minimizer_params):
        """Test case where no reads are duplicates."""
        seq1 = "A" * 76
        seq2 = "G" * 76
        qual1 = "I" * 76
        qual2 = "H" * 76

        content = create_test_tsv_content([
            ("read1", seq1, qual1, "read1"),
            ("read2", seq2, qual2, "read2"),
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        rows, _ = run_pipeline_and_get_output(
            input_file, output_file, params, minimizer_params)

        # Each read should be its own similarity exemplar
        assert rows[0]["sim_dup_exemplar"] == "read1"
        assert rows[1]["sim_dup_exemplar"] == "read2"

    def test_alignment_duplicates_get_na(self, tsv_factory, params, minimizer_params):
        """Test that alignment duplicates get 'NA' for sim_dup_exemplar."""
        seq1 = "ACGT" * 19
        seq2 = "TGCA" * 19
        seq3 = "GGCC" * 19
        qual = "I" * 76

        content = create_test_tsv_content([
            ("read1", seq1, qual, "read1"),
            ("read2", seq2, qual, "read1"),  # alignment duplicate of read1
            ("read3", seq3, qual, "read3"),
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        rows, _ = run_pipeline_and_get_output(
            input_file, output_file, params, minimizer_params)

        # read1 and read3 are alignment-unique, should have sim_dup_exemplar
        assert rows[0]["sim_dup_exemplar"] == "read1"
        assert rows[2]["sim_dup_exemplar"] == "read3"
        # read2 is alignment duplicate, should have 'NA'
        assert rows[1]["sim_dup_exemplar"] == "NA"

    def test_similarity_duplicates_among_alignment_unique(self, tsv_factory, params, minimizer_params):
        """Test similarity deduplication among alignment-unique reads."""
        # read1 and read3 are identical (similarity duplicates)
        seq1 = "A" * 76
        seq2 = "C" * 76
        qual1 = "I" * 76
        qual2 = "H" * 76

        content = create_test_tsv_content([
            ("read1", seq1, qual1, "read1"),
            ("read2", seq2, qual2, "read1"),  # Alignment duplicate of read1
            ("read3", seq1, qual1, "read3"),  # Identical to read1
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        rows, _ = run_pipeline_and_get_output(
            input_file, output_file, params, minimizer_params)

        # read1 and read3 are alignment-unique and similar
        # They should have the same sim_dup_exemplar
        exemplar = rows[0]["sim_dup_exemplar"]
        assert rows[2]["sim_dup_exemplar"] == exemplar
        # read2 is alignment duplicate, should have 'NA'
        assert rows[1]["sim_dup_exemplar"] == "NA"

    def test_empty_file(self, tsv_factory, params, minimizer_params):
        """Test handling of file with only header."""
        content = create_test_tsv_content([])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        rows, fieldnames = run_pipeline_and_get_output(
            input_file, output_file, params, minimizer_params)

        # Verify output has header but no data rows
        assert len(rows) == 0
        assert "sim_dup_exemplar" in fieldnames

    def test_single_read(self, tsv_factory, params, minimizer_params):
        """Test handling of single read."""
        seq = "ACGT" * 12
        qual = "I" * 48

        content = create_test_tsv_content([
            ("read1", seq, qual, "read1"),
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        rows, _ = run_pipeline_and_get_output(
            input_file, output_file, params, minimizer_params)

        assert len(rows) == 1
        assert rows[0]["sim_dup_exemplar"] == "read1"

    def test_column_order_preserved(self, tsv_factory, params, minimizer_params):
        """Test that output columns match input order and extra columns preserved."""
        seq = "ACGT" * 12
        qual = "I" * 48

        # Manually create content with extra columns
        content = (
            "extra1\tseq_id\textra2\tquery_seq\tquery_seq_rev\t"
            "query_qual\tquery_qual_rev\tprim_align_dup_exemplar\textra3\n"
            f"val1\tread1\tval2\t{seq}\t{seq}\t{qual}\t{qual}\tread1\tval3\n"
        )
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        rows, fieldnames = run_pipeline_and_get_output(
            input_file, output_file, params, minimizer_params)

        # Check column order: original columns + sim_dup_exemplar at end
        expected_fields = [
            "extra1", "seq_id", "extra2", "query_seq", "query_seq_rev",
            "query_qual", "query_qual_rev", "prim_align_dup_exemplar",
            "extra3", "sim_dup_exemplar"
        ]
        assert fieldnames == expected_fields

        # Check extra columns preserved
        assert rows[0]["extra1"] == "val1"
        assert rows[0]["extra2"] == "val2"
        assert rows[0]["extra3"] == "val3"

    def test_all_alignment_duplicates(self, tsv_factory, params, minimizer_params):
        """Test file where all reads are alignment duplicates of one read."""
        seq = "ACGT" * 12
        qual = "I" * 48

        content = create_test_tsv_content([
            ("read1", seq, qual, "read1"),
            ("read2", seq, qual, "read1"),  # alignment dup
            ("read3", seq, qual, "read1"),  # alignment dup
        ])
        input_file = tsv_factory.create_gzip("input.tsv.gz", content)
        output_file = tsv_factory.get_path("output.tsv.gz")

        rows, _ = run_pipeline_and_get_output(
            input_file, output_file, params, minimizer_params)

        # Only read1 is alignment-unique
        assert rows[0]["sim_dup_exemplar"] == "read1"
        # read2 and read3 are alignment duplicates
        assert rows[1]["sim_dup_exemplar"] == "NA"
        assert rows[2]["sim_dup_exemplar"] == "NA"
