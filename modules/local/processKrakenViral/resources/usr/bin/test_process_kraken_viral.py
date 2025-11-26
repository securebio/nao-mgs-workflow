#!/usr/bin/env python

import pytest
import gzip

import process_kraken_viral


class TestProcessKrakenViral:
    """Test the process_kraken_viral module."""

    def test_empty_file_produces_header_only_output(self, tmp_path):
        """Test that empty input file produces output with only header."""
        # Create empty input file
        kraken_input = tmp_path / "empty.txt.gz"
        with gzip.open(kraken_input, "wt") as f:
            f.write("")

        # Create minimal virus DB
        virus_db = tmp_path / "virus_db.tsv"
        virus_db.write_text("taxid\tinfection_status_human\n")

        output = tmp_path / "output.tsv.gz"

        # Create virus status dict (empty for this test)
        virus_status_dict = {}

        # Process the empty kraken file
        process_kraken_viral.process_kraken(str(kraken_input), str(output), virus_status_dict)

        # Read output
        with gzip.open(output, "rt") as f:
            lines = f.readlines()

        # Should have exactly one line (the header)
        assert len(lines) == 1

        # Check that header contains the expected column names
        expected_header = "kraken2_classified\tseq_id\tkraken2_assigned_name\tkraken2_assigned_taxid\tkraken2_assigned_host_virus\tkraken2_length\tkraken2_encoded_hits\n"
        assert lines[0] == expected_header
