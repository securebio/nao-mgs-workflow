#!/usr/bin/env python

import os

import pytest

import join_fastq_interleaved


class TestJoinPairedReads:
    """Test the join_paired_reads function."""

    @pytest.mark.parametrize(
        "fwd_seq,fwd_qual,rev_seq,rev_qual,gap,expected_seq,expected_qual",
        [
            # Forward: ACGT, Reverse: AAAA -> RC: TTTT
            ("ACGT", "IIII", "AAAA", "ABCD", "N", "ACGTNTTTT", "IIII!DCBA"),
            # With triple N gap
            ("ACGT", "IIII", "AAAA", "ABCD", "NNN", "ACGTNNNTTTT", "IIII!!!DCBA"),
            # With no gap
            ("ACGT", "IIII", "AAAA", "ABCD", "", "ACGTTTTT", "IIIIDCBA"),
            # Test reverse complement: TGCA -> RC: TGCA (palindrome)
            ("ACGT", "IIII", "TGCA", "IIII", "N", "ACGTNTGCA", "IIII!IIII"),
        ],
        ids=["single_n_gap", "triple_n_gap", "no_gap", "palindrome_rc"],
    )
    def test_join_with_different_gaps(
        self,
        temp_file_helper,
        fwd_seq,
        fwd_qual,
        rev_seq,
        rev_qual,
        gap,
        expected_seq,
        expected_qual,
    ):
        """Test joining with different gap sequences and verify exact output."""
        input_content = (
            f"@read1\n{fwd_seq}\n+\n{fwd_qual}\n" f"@read1\n{rev_seq}\n+\n{rev_qual}\n"
        )

        input_file = temp_file_helper.create_file("input.fastq", input_content)
        output_file = temp_file_helper.get_path("output.fastq")

        join_fastq_interleaved.join_paired_reads(input_file, output_file, gap=gap)

        result = temp_file_helper.read_file(output_file)
        lines = result.strip().split("\n")

        # Should have 4 lines (1 record)
        assert len(lines) == 4
        # Header should contain "joined" and read ID
        assert "joined" in lines[0]
        assert "read1" in lines[0]
        # Sequence should be exactly as expected
        assert lines[1] == expected_seq
        # Plus line
        assert lines[2] == "+"
        # Quality should be exactly as expected
        assert lines[3] == expected_qual

    @pytest.mark.parametrize(
        "input_content",
        ["", "   \n  \n"],
        ids=["empty_file", "whitespace_only"],
    )
    def test_empty_files(self, temp_file_helper, input_content):
        """Test handling of empty or whitespace-only files."""
        input_file = temp_file_helper.create_file("input.fastq", input_content)
        output_file = temp_file_helper.get_path("output.fastq")

        join_fastq_interleaved.join_paired_reads(input_file, output_file)

        result = temp_file_helper.read_file(output_file)
        assert result == ""

    def test_multiple_pairs(self, temp_file_helper):
        """Test joining multiple read pairs."""
        input_content = (
            "@read1\nACGT\n+\nIIII\n"
            "@read1\nAAAA\n+\nABCD\n"
            "@read2\nGGGG\n+\nHHHH\n"
            "@read2\nCCCC\n+\nEFGH\n"
        )

        input_file = temp_file_helper.create_file("input.fastq", input_content)
        output_file = temp_file_helper.get_path("output.fastq")

        join_fastq_interleaved.join_paired_reads(input_file, output_file, gap="N")

        result = temp_file_helper.read_file(output_file)
        lines = result.strip().split("\n")

        # Should have 8 lines (2 records)
        assert len(lines) == 8

        # First record
        assert "read1" in lines[0]
        assert "joined" in lines[0]
        assert lines[1] == "ACGTNTTTT"  # ACGT + N + RC(AAAA)
        assert lines[2] == "+"
        assert lines[3] == "IIII!DCBA"  # Forward qual + gap qual + reversed reverse qual

        # Second record
        assert "read2" in lines[4]
        assert "joined" in lines[4]
        assert lines[5] == "GGGGNGGGG"  # GGGG + N + RC(CCCC)
        assert lines[6] == "+"
        assert lines[7] == "HHHH!HGFE"  # Forward qual + gap qual + reversed reverse qual

    @pytest.mark.parametrize(
        "description,expected_id,expected_desc_part",
        [
            ("SRR12345678.1", "SRR12345678.1", None),
            ("SRR12345678.1 extra info", "SRR12345678.1", "extra info"),
            ("read1", "read1", None),
        ],
        ids=["simple_id", "id_with_extra", "basic_id"],
    )
    def test_preserves_read_id_and_description(
        self, temp_file_helper, description, expected_id, expected_desc_part
    ):
        """Test that read ID and description are preserved correctly."""
        input_content = (
            f"@{description}\nACGT\n+\nIIII\n" f"@{description}\nAAAA\n+\nIIII\n"
        )

        input_file = temp_file_helper.create_file("input.fastq", input_content)
        output_file = temp_file_helper.get_path("output.fastq")

        join_fastq_interleaved.join_paired_reads(input_file, output_file)

        result = temp_file_helper.read_file(output_file)
        lines = result.strip().split("\n")

        # Header should be: @{id} joined {description} or @{id} joined
        assert f"@{expected_id} joined" in lines[0]
        if expected_desc_part:
            assert expected_desc_part in lines[0]
