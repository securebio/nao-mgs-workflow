#!/usr/bin/env python

import gzip
from pathlib import Path

import process_viral_bowtie2_sam
import pytest


class TestProcessViralBowtie2Sam:
    """Test the process_viral_bowtie2_sam module."""

    def test_empty_file_produces_header_only_output(self, tmp_path: Path) -> None:
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
        genbank_metadata_dict = process_viral_bowtie2_sam.read_genbank_metadata(
            str(genbank_metadata)
        )
        viral_taxids = process_viral_bowtie2_sam.get_viral_taxids(str(virus_db))

        # Process the empty SAM file (paired mode)
        with gzip.open(sam_input, "rt") as inf, gzip.open(output, "wt") as outf:
            process_viral_bowtie2_sam.process_paired_sam(
                inf, outf, genbank_metadata_dict, viral_taxids
            )

        # Read output
        with gzip.open(output, "rt") as f:
            lines = f.readlines()

        # Should have exactly one line (the header)
        assert len(lines) == 1

        # Split header by tabs and verify the expected column headers
        headers = lines[0].strip().split("\t")
        expected_headers = [
            "seq_id",
            "genome_id",
            "genome_id_all",
            "taxid",
            "taxid_all",
            "fragment_length",
        ]

        # Verify all expected headers are present and in the right order
        for i in range(len(expected_headers)):
            assert headers[i] == expected_headers[i]


class TestParseCigar:
    """Test CIGAR string parsing."""

    @pytest.mark.parametrize(
        "cigar,expected",
        [
            ("100M", [(100, "M")]),
            ("7S93M", [(7, "S"), (93, "M")]),
            ("5H10S80M5S", [(5, "H"), (10, "S"), (80, "M"), (5, "S")]),
            ("50M10D50M", [(50, "M"), (10, "D"), (50, "M")]),
            ("*", []),
        ],
    )
    def test_parses_operations_in_order(
        self, cigar: str, expected: list[tuple[int, str]]
    ) -> None:
        assert process_viral_bowtie2_sam.parse_cigar(cigar) == expected

    @pytest.mark.parametrize("cigar", ["", "100", "M100", "100Z", "100M5", "10 0M"])
    def test_rejects_a_malformed_string(self, cigar: str) -> None:
        with pytest.raises(ValueError, match="Malformed CIGAR string"):
            process_viral_bowtie2_sam.parse_cigar(cigar)


class TestUnclippedBounds:
    """Test unclipped reference bounds, the coordinates samtools markdup keys on."""

    @pytest.mark.parametrize(
        "ref_start,cigar,expected",
        [
            # A 100 bp read covering 500-599 with nothing clipped
            (500, "100M", (500, 599)),
            # The same fragment end, clipped by the aligner: POS moves, the
            # unclipped bounds do not
            (507, "7S93M", (500, 599)),
            (500, "93M7S", (500, 599)),
            (505, "5S90M5S", (500, 599)),
            # Hard clips count too, and stack with soft clips
            (505, "5H95M", (500, 599)),
            (510, "5H5S90M", (500, 599)),
            # Deletions and skips consume reference bases; insertions do not
            (500, "50M10D50M", (500, 609)),
            (500, "50M10N50M", (500, 609)),
            (500, "50M10I50M", (500, 599)),
            # A single base
            (500, "1M", (500, 500)),
        ],
    )
    def test_counts_clipped_bases_as_aligned(
        self, ref_start: int, cigar: str, expected: tuple[int, int]
    ) -> None:
        assert process_viral_bowtie2_sam.unclipped_bounds(ref_start, cigar) == expected

    def test_returns_na_for_an_unmapped_record(self) -> None:
        """An unmapped mate carries POS 0 and CIGAR "*", so it has no bounds."""
        assert process_viral_bowtie2_sam.unclipped_bounds(-1, "*") == ("NA", "NA")


class TestUnclippedColumns:
    """Test that the unclipped bounds reach the output columns, in the right slots."""

    GENOME = "NC_000001.1"

    def _run(self, tmp_path: Path, sam_lines: list[str]) -> dict[str, str]:
        """Process the given SAM lines and return the single output row."""
        sam_input = tmp_path / "in.sam.gz"
        with gzip.open(sam_input, "wt") as f:
            f.write("".join(line + "\n" for line in sam_lines))
        genbank_metadata = tmp_path / "genbank_metadata.tsv.gz"
        with gzip.open(genbank_metadata, "wt") as f:
            f.write("genome_id\ttaxid\tspecies_taxid\n")
            f.write(f"{self.GENOME}\t10298\t10298\n")
        virus_db = tmp_path / "virus_db.tsv.gz"
        with gzip.open(virus_db, "wt") as f:
            f.write("taxid\n10298\n")
        output = tmp_path / "out.tsv.gz"
        metadata = process_viral_bowtie2_sam.read_genbank_metadata(
            str(genbank_metadata)
        )
        viral_taxids = process_viral_bowtie2_sam.get_viral_taxids(str(virus_db))
        with gzip.open(sam_input, "rt") as inf, gzip.open(output, "wt") as outf:
            process_viral_bowtie2_sam.process_paired_sam(
                inf, outf, metadata, viral_taxids
            )
        with gzip.open(output, "rt") as f:
            header = f.readline().rstrip("\n").split("\t")
            row = f.readline().rstrip("\n").split("\t")
        return dict(zip(header, row, strict=True))

    def _alignment(
        self, flag: int, pos: int, cigar: str, length: int, pair_status: str = "CP"
    ) -> str:
        seq, qual = "A" * length, "I" * length
        return (
            f"read1\t{flag}\t{self.GENOME}\t{pos}\t42\t{cigar}\t=\t1\t300\t"
            f"{seq}\t{qual}\tAS:i:180\tXS:i:100\tNM:i:0\tYT:Z:{pair_status}"
        )

    def test_a_clipped_pair_reports_the_unclipped_span(self, tmp_path: Path) -> None:
        """Mate 1's leading clip moves its POS but not its unclipped start."""
        row = self._run(
            tmp_path,
            [
                self._alignment(99, 508, "7S93M", 100),
                self._alignment(147, 701, "93M7S", 100),
            ],
        )
        assert row["ref_start"] == "507"
        assert row["ref_start_unclipped"] == "500"
        assert row["ref_end_unclipped"] == "599"
        assert row["ref_start_rev"] == "700"
        assert row["ref_start_unclipped_rev"] == "700"
        assert row["ref_end_unclipped_rev"] == "799"

    def test_a_lone_aligned_mate_fills_only_its_own_slot(self, tmp_path: Path) -> None:
        """An unmapped mate has no CIGAR, so both of its columns are NA."""
        row = self._run(
            tmp_path,
            [
                self._alignment(73, 501, "10S90M", 100, pair_status="UP"),
                # Bowtie2 places an unaligned mate at its mate's coordinate, with
                # no CIGAR and no alignment score
                f"read1\t133\t{self.GENOME}\t501\t0\t*\t=\t501\t0\t"
                + "A" * 100
                + "\t"
                + "I" * 100
                + "\tYT:Z:UP",
            ],
        )
        assert row["ref_start_unclipped"] == "490"
        assert row["ref_end_unclipped"] == "589"
        assert row["ref_start_unclipped_rev"] == "NA"
        assert row["ref_end_unclipped_rev"] == "NA"
