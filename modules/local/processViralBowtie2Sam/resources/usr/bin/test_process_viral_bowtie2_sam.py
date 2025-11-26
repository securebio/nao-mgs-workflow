#!/usr/bin/env python

import pytest
import gzip

import process_viral_bowtie2_sam


class TestProcessViralBowtie2Sam:
    """Test the process_viral_bowtie2_sam module."""

    def test_empty_file_produces_header_only_output(self, tmp_path):
        """Test that empty SAM file produces output with only header."""
        # Create empty SAM input file (gzipped)
        sam_input = tmp_path / "empty.sam.gz"
        with gzip.open(sam_input, "wt") as f:
            f.write("")

        # Create minimal genbank metadata file
        genbank_metadata = tmp_path / "genbank_metadata.tsv.gz"
        with gzip.open(genbank_metadata, "wt") as f:
            f.write("genome_id\ttaxid\tspecies_taxid\n")

        # Create minimal virus DB file
        virus_db = tmp_path / "virus_db.tsv.gz"
        with gzip.open(virus_db, "wt") as f:
            f.write("taxid\n")

        output = tmp_path / "output.tsv.gz"

        # Read metadata
        genbank_metadata_dict = process_viral_bowtie2_sam.read_genbank_metadata(str(genbank_metadata))
        viral_taxids = process_viral_bowtie2_sam.get_viral_taxids(str(virus_db))

        # Process the empty SAM file (paired mode)
        with gzip.open(sam_input, "rt") as inf, gzip.open(output, "wt") as outf:
            process_viral_bowtie2_sam.process_paired_sam(inf, outf, genbank_metadata_dict, viral_taxids)

        # Read output
        with gzip.open(output, "rt") as f:
            lines = f.readlines()

        # Should have exactly one line (the header)
        assert len(lines) == 1

        # Split header by tabs and verify the expected column headers
        headers = lines[0].strip().split("\t")
        expected_headers = [
            "seq_id",
            "genome_id", "genome_id_all",
            "taxid", "taxid_all",
            "fragment_length"
        ]

        # Verify all expected headers are present and in the right order
        for i in range(len(expected_headers)):
            assert headers[i] == expected_headers[i]
