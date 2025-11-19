#!/usr/bin/env python

import pytest
import gzip

import filter_virus_reads


@pytest.fixture
def virus_hits_test_data():
    """Fixture providing test data matching virus-hits-test.tsv."""
    return (
        "seq_id\tkraken2_classified\tkraken2_assigned_host_virus\taligner_length_normalized_score\n"
        "A0\tTRUE\t0\t10\n"
        "B0\tTRUE\t0\t15\n"
        "C0\tTRUE\t0\t20\n"
        "D0\tTRUE\t0\t25\n"
        "A1\tTRUE\t1\t10\n"
        "B1\tTRUE\t1\t15\n"
        "C1\tTRUE\t1\t20\n"
        "D1\tTRUE\t1\t25\n"
        "A2\tTRUE\t2\t10\n"
        "B2\tTRUE\t2\t15\n"
        "C2\tTRUE\t2\t20\n"
        "D2\tTRUE\t2\t25\n"
        "A3\tFALSE\t0\t10\n"
        "B3\tFALSE\t0\t15\n"
        "C3\tFALSE\t0\t20\n"
        "D3\tFALSE\t0\t25\n"
    )


def filter_expected_output(input_content, threshold):
    """
    Filter input content using the same logic as filter_virus_reads.
    Returns the expected output as a list of lines (including header).
    """
    lines = input_content.strip().split("\n")
    header = lines[0]
    data_lines = lines[1:]

    headers = header.split("\t")
    idx = {h: i for i, h in enumerate(headers)}

    result_lines = [header]

    for line in data_lines:
        fields = line.split("\t")
        kraken2_classified = fields[idx["kraken2_classified"]].upper() == "TRUE"
        kraken2_assigned_host_virus = int(fields[idx["kraken2_assigned_host_virus"]])
        score = float(fields[idx["aligner_length_normalized_score"]])

        # Apply filtering logic:
        # Keep if: status=1 OR (score >= threshold AND (status=2 OR unclassified))
        if kraken2_assigned_host_virus == 1:
            result_lines.append(line)
        elif score >= threshold and kraken2_assigned_host_virus == 2:
            result_lines.append(line)
        elif score >= threshold and not kraken2_classified:
            result_lines.append(line)

    return result_lines


class TestFilterVirusReads:
    """Test the filter_virus_reads module."""

    def test_missing_seq_id_header_raises_error(self, tsv_factory, tmp_path):
        """Test that missing seq_id column raises ValueError."""
        # Create input with all required columns EXCEPT seq_id
        input_content = "kraken2_classified\tkraken2_assigned_host_virus\taligner_length_normalized_score\nTrue\t1\t10\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output = tmp_path / "output.tsv.gz"

        with pytest.raises(ValueError, match="Missing column in input TSV: 'seq_id'"):
            filter_virus_reads.filter_virus_reads(input_file, 0.0, str(output))

    def test_missing_kraken2_classified_header_raises_error(self, tsv_factory, tmp_path):
        """Test that missing kraken2_classified column raises ValueError."""
        # Create input with all required columns EXCEPT kraken2_classified
        input_content = "seq_id\tkraken2_assigned_host_virus\taligner_length_normalized_score\nread1\t1\t10\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output = tmp_path / "output.tsv.gz"

        with pytest.raises(ValueError, match="Missing column in input TSV: 'kraken2_classified'"):
            filter_virus_reads.filter_virus_reads(input_file, 0.0, str(output))

    def test_missing_kraken2_assigned_host_virus_header_raises_error(self, tsv_factory, tmp_path):
        """Test that missing kraken2_assigned_host_virus column raises ValueError."""
        # Create input with all required columns EXCEPT kraken2_assigned_host_virus
        input_content = "seq_id\tkraken2_classified\taligner_length_normalized_score\nread1\tTrue\t10\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output = tmp_path / "output.tsv.gz"

        with pytest.raises(ValueError, match="Missing column in input TSV: 'kraken2_assigned_host_virus'"):
            filter_virus_reads.filter_virus_reads(input_file, 0.0, str(output))

    def test_missing_aligner_score_header_raises_error(self, tsv_factory, tmp_path):
        """Test that missing aligner_length_normalized_score column raises ValueError."""
        # Create input with all required columns EXCEPT aligner_length_normalized_score
        input_content = "seq_id\tkraken2_classified\tkraken2_assigned_host_virus\nread1\tTrue\t1\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output = tmp_path / "output.tsv.gz"

        with pytest.raises(ValueError, match="Missing column in input TSV: 'aligner_length_normalized_score'"):
            filter_virus_reads.filter_virus_reads(input_file, 0.0, str(output))

    def test_inconsistent_kraken_fields_raises_error(self, tsv_factory, tmp_path):
        """Test that inconsistent Kraken fields raise ValueError."""
        # Create input where kraken2_classified is False but kraken2_assigned_host_virus is not 0
        input_content = "seq_id\tkraken2_classified\tkraken2_assigned_host_virus\taligner_length_normalized_score\nread1\tFalse\t1\t10\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output = tmp_path / "output.tsv.gz"

        with pytest.raises(ValueError, match="Inconsistent Kraken fields"):
            filter_virus_reads.filter_virus_reads(input_file, 0.0, str(output))

    @pytest.mark.parametrize(
        "threshold",
        [15, 25, 100, 0],
    )
    def test_filtering_with_different_thresholds(self, tsv_factory, virus_hits_test_data, tmp_path, threshold):
        """Test filtering with various thresholds produces correct output."""
        input_file = tsv_factory.create_plain("input.tsv", virus_hits_test_data)
        output = tmp_path / "output.tsv.gz"

        filter_virus_reads.filter_virus_reads(input_file, threshold, str(output))

        # Read actual output
        with gzip.open(output, "rt") as f:
            actual_lines = [line.strip() for line in f.readlines()]

        # Compute expected output
        expected_lines = filter_expected_output(virus_hits_test_data, threshold)

        # Compare
        assert actual_lines == expected_lines

    def test_empty_file_produces_empty_output(self, tsv_factory, tmp_path):
        """Test that empty input file produces completely empty output."""
        # Create empty input file
        input_content = ""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output = tmp_path / "output.tsv.gz"

        filter_virus_reads.filter_virus_reads(input_file, 0, str(output))

        # Read output
        with gzip.open(output, "rt") as f:
            content = f.read()

        # Should be completely empty (no header, no data)
        assert content == ""
