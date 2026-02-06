#!/usr/bin/env python3

import pytest
import gzip

import pandas as pd
import process_viral_minimap2_sam


@pytest.fixture
def ref_data(tmp_path):
    """Create minimal metadata and viral DB, loaded into the dicts process_sam expects."""
    meta_path = tmp_path / "metadata.tsv.gz"
    with gzip.open(meta_path, "wt") as f:
        f.write("genome_id\ttaxid\tspecies_taxid\n")
        f.write("genome1\t12345\t12300\n")
        f.write("genome2\t67890\t67800\n")

    vdb_path = tmp_path / "virus_db.tsv.gz"
    with gzip.open(vdb_path, "wt") as f:
        f.write("taxid\n")
        f.write("12345\n")
        f.write("67890\n")

    meta_db = pd.read_csv(str(meta_path), sep="\t", dtype=str)
    genbank_metadata = {
        gid: [tid, stid]
        for gid, tid, stid in zip(
            meta_db["genome_id"], meta_db["taxid"], meta_db["species_taxid"]
        )
    }
    virus_db = pd.read_csv(str(vdb_path), sep="\t", dtype=str)
    viral_taxids = set(virus_db["taxid"].values)

    return genbank_metadata, viral_taxids


class TestProcessViralMinimap2Sam:

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

    def test_normal_case_multi_alignment_and_reverse_complement(
        self, tmp_path, ref_data
    ):
        """Multi-alignment hold, FASTQ superset skipping, and reverse complement."""
        genbank_metadata, viral_taxids = ref_data

        # Sorted SAM: read_A has 2 alignments (primary + secondary flag 256),
        # read_C has 1 reverse-strand alignment (flag 16).
        # read_B is in FASTQ but not SAM (FASTQ is a superset).
        sam_path = tmp_path / "test.sam"
        sam_path.write_text(
            "@HD\tVN:1.6\tSO:queryname\n"
            "@SQ\tSN:genome1\tLN:10000\n"
            "@SQ\tSN:genome2\tLN:10000\n"
            "read_A\t0\tgenome1\t101\t60\t10M\t*\t0\t0\tACGTACGTAC\tIIIIIIIIII\tNM:i:0\tAS:i:20\n"
            "read_A\t256\tgenome2\t201\t30\t10M\t*\t0\t0\tACGTACGTAC\tIIIIIIIIII\tNM:i:1\tAS:i:15\n"
            "read_C\t16\tgenome1\t301\t60\t10M\t*\t0\t0\tGTACGTACGT\tIIIIIHHHHH\tNM:i:0\tAS:i:18\n"
        )

        # Sorted FASTQ with unmasked (clean) sequences. read_B not in SAM.
        fastq_path = tmp_path / "test.fastq"
        fastq_path.write_text(
            "@read_A\n"
            "ACGTACGTAC\n"
            "+\n"
            "FFFFFFFFFF\n"
            "@read_B\n"
            "TTTTTTTTTT\n"
            "+\n"
            "GGGGGGGGGG\n"
            "@read_C\n"
            "ACGTACGTAC\n"
            "+\n"
            "IIIIIHHHHH\n"
        )

        out_path = str(tmp_path / "output.tsv.gz")
        process_viral_minimap2_sam.process_sam(
            str(sam_path), out_path, genbank_metadata, viral_taxids, str(fastq_path)
        )

        with gzip.open(out_path, "rt") as f:
            lines = f.readlines()

        # Header + 3 data rows (2 for read_A, 1 for read_C)
        assert len(lines) == 4

        header = lines[0].strip().split("\t")
        rows = [dict(zip(header, line.strip().split("\t"))) for line in lines[1:]]

        # read_A: two alignments with unmodified forward seq/qual
        read_a_rows = [r for r in rows if r["seq_id"] == "read_A"]
        assert len(read_a_rows) == 2
        for r in read_a_rows:
            assert r["query_seq"] == "ACGTACGTAC"
            assert r["query_qual"] == "FFFFFFFFFF"
            assert r["query_rc"] == "False"
            assert r["query_len"] == "10"
        assert read_a_rows[0]["classification"] == "primary"
        assert read_a_rows[1]["classification"] == "secondary"
        # ref_start is 0-based (SAM POS 101 → 100, POS 201 → 200)
        assert read_a_rows[0]["ref_start"] == "100"
        assert read_a_rows[1]["ref_start"] == "200"
        assert read_a_rows[0]["taxid"] == "12345"
        assert read_a_rows[1]["taxid"] == "67890"

        # read_C: reverse strand — RC seq and reversed qual
        read_c_rows = [r for r in rows if r["seq_id"] == "read_C"]
        assert len(read_c_rows) == 1
        rc = read_c_rows[0]
        assert rc["query_seq"] == "GTACGTACGT"  # RC of ACGTACGTAC
        assert rc["query_qual"] == "HHHHHIIIII"  # reversed IIIIIHHHHH
        assert rc["query_rc"] == "True"
        assert rc["ref_start"] == "300"
        assert rc["classification"] == "primary"
        assert float(rc["length_normalized_score"]) > 0

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
