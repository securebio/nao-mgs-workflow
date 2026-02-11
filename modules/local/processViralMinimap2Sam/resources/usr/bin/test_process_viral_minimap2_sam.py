#!/usr/bin/env python3

import gzip
import io
import math

import pytest
import process_viral_minimap2_sam


@pytest.fixture
def ref_data():
    """Minimal metadata and viral DB dicts matching what main() builds from TSVs."""
    genbank_metadata = {"genome1": ["12345", "12300"], "genome2": ["67890", "67800"]}
    viral_taxids = {"12345", "67890"}
    return genbank_metadata, viral_taxids


# ── read_fastq_record ──────────────────────────────────────────────────────


class TestReadFastqRecord:

    def test_basic_record(self):
        fh = io.StringIO("@readA\nACGT\n+\nFFFF\n")
        result = process_viral_minimap2_sam.read_fastq_record(fh)
        assert result == ("readA", "ACGT", "FFFF")

    def test_eof_returns_none(self):
        fh = io.StringIO("")
        assert process_viral_minimap2_sam.read_fastq_record(fh) is None

    @pytest.mark.parametrize(
        "header,expected_id",
        [
            ("@readA extra info\n", "readA"),
            ("@read_X\trunid=abc\n", "read_X"),
            ("@f47ac10b-58cc-4372-a567-0e02b2c3d479 runid=xyz\n",
             "f47ac10b-58cc-4372-a567-0e02b2c3d479"),
        ],
        ids=["space_delimited", "tab_delimited", "ont_uuid"],
    )
    def test_whitespace_delimited_headers(self, header, expected_id):
        fh = io.StringIO(header + "ACGT\n+\nFFFF\n")
        result = process_viral_minimap2_sam.read_fastq_record(fh)
        assert result is not None
        assert result[0] == expected_id


# ── extract_viral_taxid ────────────────────────────────────────────────────


class TestExtractViralTaxid:

    METADATA = {"g1": ["111", "222"]}

    @pytest.mark.parametrize(
        "viral,expected",
        [
            ({"111"}, "111"),          # taxid is viral → use it
            ({"222"}, "222"),          # species_taxid is viral → use it
            ({"111", "222"}, "111"),   # both viral → prefer taxid
            ({"999"}, "111"),          # neither viral → fall back to taxid
        ],
        ids=["taxid_viral", "species_viral", "both_prefers_taxid", "neither_fallback"],
    )
    def test_taxid_selection(self, viral, expected):
        assert process_viral_minimap2_sam.extract_viral_taxid("g1", self.METADATA, viral) == expected

    def test_missing_genome_raises(self):
        with pytest.raises(ValueError, match="No matching genome ID found: missing"):
            process_viral_minimap2_sam.extract_viral_taxid("missing", self.METADATA, {"111"})


# ── parse_sam_alignment ────────────────────────────────────────────────────


class TestParseSamAlignment:

    def _make_read(self, tmp_path, sam_line):
        """Create a pysam AlignedSegment from a SAM text line."""
        import pysam

        sam_path = tmp_path / "test.sam"
        sam_path.write_text(
            "@HD\tVN:1.6\n"
            "@SQ\tSN:genome1\tLN:10000\n"
            "@SQ\tSN:genome2\tLN:10000\n"
            + sam_line + "\n"
        )
        with pysam.AlignmentFile(str(sam_path), "r") as f:
            return next(iter(f))

    def test_supplementary_classification(self, tmp_path, ref_data):
        """Flag 2048 produces classification 'supplementary'."""
        genbank_metadata, viral_taxids = ref_data
        read = self._make_read(
            tmp_path,
            "readA\t2048\tgenome1\t100\t60\t10M\t*\t0\t0\tACGTACGTAC\tIIIIIIIIII\tNM:i:0\tAS:i:20",
        )
        result = process_viral_minimap2_sam.parse_sam_alignment(
            read, genbank_metadata, viral_taxids, "ACGTACGTAC", "IIIIIIIIII"
        )
        assert result["classification"] == "supplementary"

    def test_length_one_sequence(self, tmp_path, ref_data):
        """Length-1 sequence produces length_normalized_score of 0."""
        genbank_metadata, viral_taxids = ref_data
        read = self._make_read(
            tmp_path,
            "readA\t0\tgenome1\t100\t60\t1M\t*\t0\t0\tA\tI\tNM:i:0\tAS:i:5",
        )
        result = process_viral_minimap2_sam.parse_sam_alignment(
            read, genbank_metadata, viral_taxids, "A", "I"
        )
        assert result["length_normalized_score"] == 0
        assert result["query_len"] == 1

    def test_primary_forward_all_fields(self, tmp_path, ref_data):
        """Primary forward-strand alignment populates all expected fields."""
        genbank_metadata, viral_taxids = ref_data
        read = self._make_read(
            tmp_path,
            "readA\t0\tgenome1\t501\t42\t8M\t*\t0\t0\tACGTACGT\tHHHHHHHH\tNM:i:2\tAS:i:30",
        )
        result = process_viral_minimap2_sam.parse_sam_alignment(
            read, genbank_metadata, viral_taxids, "ACGTACGT", "FFFFFFFF"
        )
        assert set(result.keys()) == set(process_viral_minimap2_sam.HEADER_FIELDS)
        assert result["seq_id"] == "readA"
        assert result["genome_id"] == "genome1"
        assert result["taxid"] == "12345"
        assert result["ref_start"] == 500  # 0-based
        assert result["cigar"] == "8M"
        assert result["edit_distance"] == 2
        assert result["best_alignment_score"] == 30
        assert result["next_alignment_score"] == "NA"
        assert result["query_seq"] == "ACGTACGT"  # unmodified (forward)
        assert result["query_qual"] == "FFFFFFFF"  # from FASTQ, not SAM
        assert result["query_rc"] is False
        assert result["query_len"] == 8
        assert result["classification"] == "primary"
        assert result["length_normalized_score"] == pytest.approx(30 / math.log(8))

    def test_reverse_complement(self, tmp_path, ref_data):
        """Reverse-strand alignment (flag 16) reverse-complements seq and reverses qual."""
        genbank_metadata, viral_taxids = ref_data
        read = self._make_read(
            tmp_path,
            "readA\t16\tgenome1\t100\t60\t4M\t*\t0\t0\tACGT\tIIII\tNM:i:0\tAS:i:10",
        )
        result = process_viral_minimap2_sam.parse_sam_alignment(
            read, genbank_metadata, viral_taxids, "AACC", "FHIG"
        )
        assert result["query_seq"] == "GGTT"  # RC of AACC
        assert result["query_qual"] == "GIHF"  # reversed FHIG
        assert result["query_rc"] is True


# ── process_sam ────────────────────────────────────────────────────────────


class TestProcessSam:

    def test_empty_sam_produces_header_only_output(self, tmp_path, ref_data):
        """Empty SAM file produces output with header line only."""
        genbank_metadata, viral_taxids = ref_data

        sam_path = tmp_path / "empty.sam"
        sam_path.write_text(
            "@HD\tVN:1.6\tSO:queryname\n" "@SQ\tSN:genome1\tLN:10000\n"
        )

        fastq_path = tmp_path / "empty.fastq"
        fastq_path.write_text("")

        out_path = str(tmp_path / "output.tsv.gz")
        process_viral_minimap2_sam.process_sam(
            str(sam_path), out_path, genbank_metadata, viral_taxids, str(fastq_path)
        )

        with gzip.open(out_path, "rt") as f:
            lines = f.readlines()
        assert len(lines) == 1  # Header only

    def test_unmapped_reads_skipped(self, tmp_path, ref_data):
        """Unmapped reads (flag 4) are skipped in output."""
        genbank_metadata, viral_taxids = ref_data

        sam_path = tmp_path / "test.sam"
        sam_path.write_text(
            "@HD\tVN:1.6\n"
            "@SQ\tSN:genome1\tLN:10000\n"
            "read_mapped\t0\tgenome1\t101\t60\t10M\t*\t0\t0\tACGTACGTAC\tIIIIIIIIII\tNM:i:0\tAS:i:20\n"
            "read_unmapped\t4\t*\t0\t0\t*\t*\t0\t0\tTTTTTTTTTT\tFFFFFFFFFF\n"
        )

        fastq_path = tmp_path / "test.fastq"
        fastq_path.write_text(
            "@read_mapped\nACGTACGTAC\n+\nFFFFFFFFFF\n"
            "@read_unmapped\nTTTTTTTTTT\n+\nFFFFFFFFFF\n"
        )

        out_path = str(tmp_path / "output.tsv.gz")
        process_viral_minimap2_sam.process_sam(
            str(sam_path), out_path, genbank_metadata, viral_taxids, str(fastq_path)
        )

        with gzip.open(out_path, "rt") as f:
            lines = f.readlines()
        assert len(lines) == 2  # Header + 1 mapped read
        assert "read_mapped" in lines[1]
        assert "read_unmapped" not in "".join(lines)

    def test_multi_alignment_and_fastq_superset(self, tmp_path, ref_data):
        """Merge join holds FASTQ position for multi-alignment reads and skips FASTQ-only reads."""
        genbank_metadata, viral_taxids = ref_data

        # read_A: 2 alignments (primary + secondary). read_B: in FASTQ only. read_C: reverse strand.
        sam_path = tmp_path / "test.sam"
        sam_path.write_text(
            "@HD\tVN:1.6\tSO:queryname\n"
            "@SQ\tSN:genome1\tLN:10000\n"
            "@SQ\tSN:genome2\tLN:10000\n"
            "read_A\t0\tgenome1\t101\t60\t10M\t*\t0\t0\tACGTACGTAC\tIIIIIIIIII\tNM:i:0\tAS:i:20\n"
            "read_A\t256\tgenome2\t201\t30\t10M\t*\t0\t0\tACGTACGTAC\tIIIIIIIIII\tNM:i:1\tAS:i:15\n"
            "read_C\t16\tgenome1\t301\t60\t10M\t*\t0\t0\tGTACGTACGT\tIIIIIHHHHH\tNM:i:0\tAS:i:18\n"
        )
        fastq_path = tmp_path / "test.fastq"
        fastq_path.write_text(
            "@read_A\nACGTACGTAC\n+\nFFFFFFFFFF\n"
            "@read_B\nTTTTTTTTTT\n+\nGGGGGGGGGG\n"
            "@read_C\nACGTACGTAC\n+\nIIIIIHHHHH\n"
        )

        out_path = str(tmp_path / "output.tsv.gz")
        process_viral_minimap2_sam.process_sam(
            str(sam_path), out_path, genbank_metadata, viral_taxids, str(fastq_path)
        )

        with gzip.open(out_path, "rt") as f:
            lines = f.readlines()
        header = lines[0].strip().split("\t")
        rows = [dict(zip(header, line.strip().split("\t"))) for line in lines[1:]]

        # 3 output rows: read_A x2 (multi-alignment hold), read_C x1; read_B skipped
        seq_ids = [r["seq_id"] for r in rows]
        assert seq_ids == ["read_A", "read_A", "read_C"]
        assert rows[0]["classification"] == "primary"
        assert rows[1]["classification"] == "secondary"
        # Seq/qual come from FASTQ, not SAM
        assert rows[0]["query_seq"] == "ACGTACGTAC"
        assert rows[0]["query_qual"] == "FFFFFFFFFF"
        # read_C: RC applied
        assert rows[2]["query_seq"] == "GTACGTACGT"
        assert rows[2]["query_rc"] == "True"

    def test_sam_read_missing_from_fastq_raises_error(self, tmp_path, ref_data):
        """Raises ValueError when SAM contains a read not in FASTQ."""
        genbank_metadata, viral_taxids = ref_data

        sam_path = tmp_path / "test.sam"
        sam_path.write_text(
            "@HD\tVN:1.6\tSO:queryname\n"
            "@SQ\tSN:genome1\tLN:10000\n"
            "read_X\t0\tgenome1\t101\t60\t10M\t*\t0\t0\tACGTACGTAC\tIIIIIIIIII\tNM:i:0\tAS:i:20\n"
        )

        fastq_path = tmp_path / "test.fastq"
        fastq_path.write_text(
            "@read_A\n" "ACGTACGTAC\n" "+\n" "FFFFFFFFFF\n"
        )

        out_path = str(tmp_path / "output.tsv.gz")
        with pytest.raises(ValueError, match="read_X"):
            process_viral_minimap2_sam.process_sam(
                str(sam_path),
                out_path,
                genbank_metadata,
                viral_taxids,
                str(fastq_path),
            )

    def test_unsorted_gzipped_inputs_via_sort_helpers(self, tmp_path, ref_data):
        """Integration test: unsorted gzipped inputs are sorted then processed."""
        genbank_metadata, viral_taxids = ref_data

        from sort_sam import sort_sam
        from sort_fastq import sort_fastq

        # Unsorted gzipped SAM
        sam_gz = tmp_path / "unsorted.sam.gz"
        with gzip.open(str(sam_gz), "wt") as f:
            f.write(
                "@HD\tVN:1.6\n"
                "@SQ\tSN:genome1\tLN:10000\n"
                "read_C\t0\tgenome1\t301\t60\t5M\t*\t0\t0\tACGTA\tIIIII\tNM:i:0\tAS:i:10\n"
                "read_A\t0\tgenome1\t101\t60\t5M\t*\t0\t0\tACGTA\tIIIII\tNM:i:0\tAS:i:10\n"
            )

        # Unsorted gzipped FASTQ
        fastq_gz = tmp_path / "unsorted.fastq.gz"
        with gzip.open(str(fastq_gz), "wt") as f:
            f.write(
                "@read_C\nCCCCC\n+\nHHHHH\n"
                "@read_A\nAAAAA\n+\nFFFFF\n"
            )

        # Sort
        sorted_sam = str(tmp_path / "sorted.sam")
        sorted_fastq = str(tmp_path / "sorted.fastq")
        sort_sam(str(sam_gz), sorted_sam)
        sort_fastq(str(fastq_gz), sorted_fastq)

        # Process
        out_path = str(tmp_path / "output.tsv.gz")
        process_viral_minimap2_sam.process_sam(
            sorted_sam, out_path, genbank_metadata, viral_taxids, sorted_fastq
        )

        with gzip.open(out_path, "rt") as f:
            lines = f.readlines()

        assert len(lines) == 3  # header + 2 reads
        header = lines[0].strip().split("\t")
        rows = [dict(zip(header, line.strip().split("\t"))) for line in lines[1:]]
        assert rows[0]["seq_id"] == "read_A"
        assert rows[1]["seq_id"] == "read_C"
        # Verify clean seq/qual came from FASTQ, not SAM
        assert rows[0]["query_seq"] == "AAAAA"
        assert rows[1]["query_seq"] == "CCCCC"
