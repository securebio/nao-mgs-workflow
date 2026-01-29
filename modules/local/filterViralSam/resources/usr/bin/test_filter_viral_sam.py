#!/usr/bin/env python3
"""
Comprehensive test suite for filter_viral_sam.py module.

This file contains pytest-based unit and integration tests for the viral SAM filtering
functionality. Unlike other modules in this pipeline which use nf-test for testing,
the filter_viral_sam.py script contains complex Python logic that benefits from 
detailed unit testing of individual components.

When to run:
- During development of filter_viral_sam.py to verify correctness
- After making changes to SAM alignment parsing or filtering logic
- As part of debugging SAM filtering issues
- Before committing changes to the filterViralSam module

Run with: pytest test_filter_viral_sam.py -v

Test coverage includes:
- SAM alignment parsing and score calculation
- Score threshold filtering for paired and unpaired reads
- Secondary alignment handling across different reference genomes
- Synthetic mate creation for unpaired reads (UP status)
- Edge cases like empty inputs, missing tags, and header lines

The filter_viral_sam.py module implements memory-efficient filtering of viral
SAM alignments based on normalized alignment scores, with special handling for
paired reads and secondary alignments to different viral references.
"""

import pytest
import tempfile
import os
import sys
from pathlib import Path

from filter_viral_sam import filter_viral_sam, SamAlignment

# Add the script directory to path so we can import the module
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))

class TestSamAlignment:
    def test_parse_basic_sam_line(self):
        line = "read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGT\tIIIIIIII\tAS:i:45\tYT:Z:CP"
        alignment = SamAlignment.from_sam_line(line)
        
        assert alignment.qname == 'read1'
        assert alignment.flag == 99
        assert alignment.rname == 'chr1'
        assert alignment.pos == 100
        assert alignment.alignment_score == 45
        assert alignment.pair_status == 'CP'
    
    def test_parse_missing_tags(self):
        line = "read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGT\tIIIIIIII"
        alignment = SamAlignment.from_sam_line(line)
        
        assert alignment.alignment_score is None
        assert alignment.pair_status is None
    
    def test_parse_up_status(self):
        line = "read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGT\tIIIIIIII\tAS:i:30\tYT:Z:UP"
        alignment = SamAlignment.from_sam_line(line)
        
        assert alignment.pair_status == 'UP'

class TestCalculateNormalizedScore:
    def test_normal_calculation(self):
        import math
        line = "read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\t" + "A" * 100 + "\t" + "I" * 100 + "\tAS:i:50"
        alignment = SamAlignment.from_sam_line(line)
        alignment.calculate_normalized_score()
        expected = 50 / math.log(100)
        assert alignment.normalized_score is not None, "normalized_score should not be None"
        assert expected is not None, "expected should not be None"
        assert abs(alignment.normalized_score - expected) < 1e-10
    
    def test_none_score(self):
        line = "read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\t" + "A" * 100 + "\t" + "I" * 100
        alignment = SamAlignment.from_sam_line(line)
        alignment.calculate_normalized_score()
        assert alignment.normalized_score == 0
    
    def test_zero_length(self):
        line = "read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\t\t\tAS:i:50"
        alignment = SamAlignment.from_sam_line(line)
        alignment.calculate_normalized_score()
        assert alignment.normalized_score == 0

class TestCreateUnmappedMate:
    def test_create_mate_from_read1(self):
        line = "read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGT\tIIIIIIII\tAS:i:45\tYT:Z:UP"
        alignment = SamAlignment.from_sam_line(line)
        
        mate = alignment.create_unmapped_mate()
        
        # Should flip from read1 (64) to read2 (128)
        assert mate.flag & 128  # read2
        assert not (mate.flag & 64)  # not read1
        assert mate.flag & 4  # unmapped
        assert mate.cigar == '*'
    
    def test_create_mate_from_read2(self):
        line = "read1\t147\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGT\tIIIIIIII\tAS:i:45\tYT:Z:UP"
        alignment = SamAlignment.from_sam_line(line)
        
        mate = alignment.create_unmapped_mate()
        
        # Should flip from read2 (128) to read1 (64)
        assert mate.flag & 64  # read1
        assert not (mate.flag & 128)  # not read2
        assert mate.flag & 4  # unmapped

class TestFilterViralSam:
    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def create_temp_file(self, content, suffix='.txt'):
        fd, path = tempfile.mkstemp(dir=self.temp_dir, suffix=suffix)
        with os.fdopen(fd, 'w') as f:
            f.write(content)
        return path
    
    def test_filtered_read_keeping(self):
        # SAM content with one filtered read that should be kept
        sam_content = """filtered_read\t99\tchr2\t300\t60\t50M\t=\t400\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:45\tYT:Z:CP
filtered_read\t147\tchr2\t400\t60\t50M\t=\t300\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:43\tYT:Z:CP
read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYT:Z:CP
read1\t147\tchr1\t200\t60\t50M\t=\t100\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:48\tYT:Z:CP"""
        
        filtered_content = """@filtered_read/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@filtered_read/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII
@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 0.1)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should have both pairs: filtered_read (kept) and read1 (passed score threshold)
        assert len(output_lines) == 4
        assert any('filtered_read' in line for line in output_lines)
        assert any('read1' in line for line in output_lines)
    
    def test_score_threshold_pair_logic(self):
        
        # Should keep read1 but not read2
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:20\tYT:Z:CP
read1\t147\tchr1\t200\t60\t50M\t=\t100\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:35\tYT:Z:CP
read2\t99\tchr2\t300\t60\t50M\t=\t400\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:15\tYT:Z:CP
read2\t147\tchr2\t400\t60\t50M\t=\t300\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:18\tYT:Z:CP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII
@read2/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read2/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        # Threshold 10 - read1 pair should pass, read2 pair should fail
        filter_viral_sam(sam_file, filtered_file, output_file, 10.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should only have read1 pair (2 lines)
        assert len(output_lines) == 2
        assert all('read1' in line for line in output_lines)
        assert not any('read2' in line for line in output_lines)
    
    def test_single_read_above_threshold(self):
        # Single read above threshold should be kept
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYT:Z:UP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 10.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        assert len(output_lines) == 2
        assert 'read1' in output_lines[0]
    
    def test_single_read_below_threshold(self):
        # Single read below threshold should be filtered out
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:10\tYT:Z:CP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 10.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        assert len(output_lines) == 0
    
    def test_up_read_missing_mate_primary(self):
        # UP read without mate should get an unmapped mate added
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYT:Z:UP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 1.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should have 2 lines: original read + created unmapped mate
        assert len(output_lines) == 2

        print(output_lines)
        
        # Parse both lines
        read1_line = [line for line in output_lines if '\t99\t' in line][0]
        read2_line = [line for line in output_lines if '\t167\t' in line][0]

        read1_parsed = SamAlignment.from_sam_line(read1_line.strip())
        assert not (read1_parsed.flag & 4)            # should be mapped
        assert read1_parsed.cigar != '*'              # should not be unmapped

        read2_parsed = SamAlignment.from_sam_line(read2_line.strip())
        assert read2_parsed.flag & 4  # unmapped
        assert read2_parsed.cigar == '*'
    
    def test_secondary_alignments_with_score_filtering(self):
        # Secondary alignments (flag >= 256) should be processed separately by reference
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYS:i:48\tYT:Z:CP
read1\t147\tchr1\t200\t60\t50M\t=\t100\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:48\tYS:i:50\tYT:Z:CP
read1\t355\tchr2\t300\t60\t50M\t=\t400\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:45\tYS:i:15\tYT:Z:CP
read1\t403\tchr2\t400\t60\t50M\t=\t300\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:15\tYS:i:45\tYT:Z:CP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        # Threshold that allows primary but not secondary chr2
        filter_viral_sam(sam_file, filtered_file, output_file, 17.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should only have primary alignments (2 lines)
        assert len(output_lines) == 2
        primary_flags = [SamAlignment.from_sam_line(line.strip()).flag for line in output_lines]
        assert all(flag < 256 for flag in primary_flags)
    
    def test_empty_input(self):
        sam_content = ""
        filtered_content = ""
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 1.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        assert len(output_lines) == 0
    
    def test_skip_header_lines(self):
        # Headers should be skipped (though they shouldn't be present based on workflow)
        sam_content = """@HD\tVN:1.6\tSO:coordinate
@SQ\tSN:chr1\tLN:248956422
read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYT:Z:UP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""        

        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 1.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should only have the read line, headers ignored
        assert len(output_lines) == 2
        assert 'read1' in output_lines[0]
        assert all("@" not in line for line in output_lines)

    def test_multiple_secondary_alignment_pairs(self):
        # Test two pairs of secondary alignments to different references
        # with different score patterns
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYS:i:48\tYT:Z:CP
read1\t147\tchr1\t200\t60\t50M\t=\t100\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:48\tYS:i:50\tYT:Z:CP
read1\t355\tchr2\t300\t60\t50M\t=\t400\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:45\tYS:i:42\tYT:Z:CP
read1\t403\tchr2\t400\t60\t50M\t=\t300\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:42\tYS:i:45\tYT:Z:CP
read1\t355\tchr3\t500\t60\t50M\t=\t600\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:25\tYS:i:20\tYT:Z:CP
read1\t403\tchr3\t600\t60\t50M\t=\t500\t-150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:20\tYS:i:25\tYT:Z:CP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""        
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        # Threshold 15.0: 
        # - Primary (chr1): scores ~18.0 and ~17.3 - both pass
        # - Secondary chr2: scores ~16.2 and ~15.1 - chr2 pair passes (max > 15)
        # - Secondary chr3: scores ~9.0 and ~7.2 - chr3 pair fails (max < 15)
        filter_viral_sam(sam_file, filtered_file, output_file, 15.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should have primary pair (2) + chr2 secondary pair (2) = 4 lines
        assert len(output_lines) == 4
        
        # Check that we have the right references
        references = [SamAlignment.from_sam_line(line.strip()).rname for line in output_lines]
        assert 'chr1' in references  # Primary
        assert 'chr2' in references  # Secondary that passed
        assert 'chr3' not in references  # Secondary that failed
        
        # Check flag distribution: 2 primary (< 256), 2 secondary (>= 256)
        flags = [SamAlignment.from_sam_line(line.strip()).flag for line in output_lines]
        primary_flags = [f for f in flags if f < 256]
        secondary_flags = [f for f in flags if f >= 256]
        assert len(primary_flags) == 2
        assert len(secondary_flags) == 2

    def test_secondary_up_read_synthetic_mate_creation(self):
        # Test that synthetic mates are properly created and saved for secondary UP reads
        # This test specifically catches the bug where synthetic mates were created but not saved
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYT:Z:UP
read1\t355\tchr2\t300\t60\t50M\t=\t400\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:45\tYT:Z:UP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""        
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 10.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should have primary UP + synthetic mate (2) + secondary UP + synthetic mate (2) = 4 lines
        assert len(output_lines) == 4
        
        # Parse all alignments
        alignments = [SamAlignment.from_sam_line(line.strip()) for line in output_lines]
        
        # Check that we have both primary and secondary alignments
        primary_alignments = [a for a in alignments if a.flag < 256]
        secondary_alignments = [a for a in alignments if a.flag >= 256]
        
        assert len(primary_alignments) == 2  # Primary UP + synthetic mate
        assert len(secondary_alignments) == 2  # Secondary UP + synthetic mate
        
        # Check that secondary alignments include both mapped and unmapped
        secondary_chr2 = [a for a in secondary_alignments if a.rname == 'chr2']
        assert len(secondary_chr2) == 2
        
        # One should be mapped (original UP), one should be unmapped (synthetic mate)
        mapped_secondary = [a for a in secondary_chr2 if not (a.flag & 4)]
        unmapped_secondary = [a for a in secondary_chr2 if a.flag & 4]
        
        assert len(mapped_secondary) == 1
        assert len(unmapped_secondary) == 1
        
        # The unmapped one should have CIGAR '*'
        assert unmapped_secondary[0].cigar == '*'
        
        # Verify the synthetic mate has the correct flag structure
        # Original UP read was flag 355 (read1), synthetic mate should be read2 + unmapped
        original_up = mapped_secondary[0]
        synthetic_mate = unmapped_secondary[0]
        
        # Original should be read1 (flag & 64), synthetic should be read2 (flag & 128)
        if original_up.flag & 64:  # Original is read1
            assert synthetic_mate.flag & 128  # Synthetic is read2
            assert not (synthetic_mate.flag & 64)
        else:  # Original is read2
            assert synthetic_mate.flag & 64  # Synthetic is read1
            assert not (synthetic_mate.flag & 128)

    def test_primary_up_reads_different_references_consecutive_ordering(self):
        # Test that primary UP reads mapping to different references appear
        # consecutively and are properly ordered (read1 before read2)
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYT:Z:UP
read1\t147\tchr2\t300\t60\t50M\t=\t400\t150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:45\tYT:Z:UP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""        
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 10.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should have exactly 2 lines: the 2 original UP reads (no synthetic mates needed)
        assert len(output_lines) == 2
        
        # Parse both alignments
        alignments = [SamAlignment.from_sam_line(line.strip()) for line in output_lines]
        
        # Both should be primary alignments (flag < 256)
        assert all(a.flag < 256 for a in alignments)
        
        # Should have reads mapping to both chr1 and chr2
        references = [a.rname for a in alignments]
        assert 'chr1' in references
        assert 'chr2' in references
        
        # Verify flag-based ordering: read1 (flag 99 & 64) should come before read2 (flag 147 & 128)
        assert alignments[0].flag & 64  # First alignment is read1
        assert alignments[1].flag & 128  # Second alignment is read2
        assert not (alignments[0].flag & 128)  # First is not read2
        assert not (alignments[1].flag & 64)  # Second is not read1
        
        # Verify specific flag values and references
        assert alignments[0].flag == 99  # read1 flag
        assert alignments[0].rname == 'chr1'
        assert alignments[1].flag == 147  # read2 flag  
        assert alignments[1].rname == 'chr2'
        
        # Both should be UP reads
        assert alignments[0].pair_status == 'UP'
        assert alignments[1].pair_status == 'UP'

    def test_primary_up_reads_different_references_with_secondary(self):
        # Test similar scenario as above but with a secondary read added
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYT:Z:UP
read1\t147\tchr3\t300\t60\t50M\t=\t400\t150\tTGCATGCATGCATGCA\tIIIIIIIIIIIIIIII\tAS:i:45\tYT:Z:UP
read1\t355\tchr2\t500\t60\t50M\t=\t600\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:40\tYT:Z:UP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""        
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 10.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should have 4 lines: 2 primary reads + 1 secondary read + 1 synthetic mate for secondary
        assert len(output_lines) == 4
        
        # Parse all alignments
        alignments = [SamAlignment.from_sam_line(line.strip()) for line in output_lines]
        
        # Separate primary and secondary alignments
        primary_alignments = [a for a in alignments if a.flag < 256]
        secondary_alignments = [a for a in alignments if a.flag >= 256]
        
        # Should have 2 primary (original reads) and 2 secondary (original + synthetic mate)
        assert len(primary_alignments) == 2
        assert len(secondary_alignments) == 2
        
        # Primary alignments should map to chr1 and chr3
        primary_refs = [a.rname for a in primary_alignments]
        assert 'chr1' in primary_refs
        assert 'chr3' in primary_refs
        
        # Secondary alignments should map to chr2
        secondary_refs = [a.rname for a in secondary_alignments]
        assert all(ref == 'chr2' for ref in secondary_refs)
        
        # Check that secondary includes both mapped and unmapped (synthetic mate)
        secondary_mapped = [a for a in secondary_alignments if not (a.flag & 4)]
        secondary_unmapped = [a for a in secondary_alignments if a.flag & 4]
        
        assert len(secondary_mapped) == 1  # Original secondary read
        assert len(secondary_unmapped) == 1  # Synthetic mate
        
        # Synthetic mate should have CIGAR '*'
        assert secondary_unmapped[0].cigar == '*'
        
        # All reads should have the same qname
        assert all(a.qname == 'read1' for a in alignments)

    def test_unpaired_read_secondary_alignments_same_rname(self):
        # Test unpaired read with one read mapped, other not mapped, but two secondary alignments with same rname and flag
        # Both secondary reads are forward reads (same flag), so synthetic mates should be created for both
        sam_content = """read1\t99\tchr1\t100\t60\t50M\t=\t200\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:50\tYT:Z:UP
read1\t355\tchr2\t300\t60\t50M\t=\t400\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:45\tYT:Z:UP
read1\t355\tchr2\t500\t60\t50M\t=\t600\t150\tACGTACGTACGTACGT\tIIIIIIIIIIIIIIII\tAS:i:40\tYT:Z:UP"""
        
        filtered_content = """@read1/1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCA
+
IIIIIIIIIIIIIIII"""        
        
        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')
        
        filter_viral_sam(sam_file, filtered_file, output_file, 10.0)
        
        with open(output_file, 'r') as f:
            output_lines = f.readlines()
        
        # Should have 6 lines: 1 primary + 1 synthetic mate + 2 secondary + 2 synthetic mates for secondary
        assert len(output_lines) == 6
        
        # Parse all alignments
        alignments = [SamAlignment.from_sam_line(line.strip()) for line in output_lines]
        
        # Separate primary and secondary alignments
        primary_alignments = [a for a in alignments if a.flag < 256]
        secondary_alignments = [a for a in alignments if a.flag >= 256]
        
        # Should have 2 primary (original + synthetic mate) and 4 secondary (2 original + 2 synthetic mates)
        assert len(primary_alignments) == 2
        assert len(secondary_alignments) == 4
        
        # Primary alignments: one mapped to chr1, one synthetic unmapped mate
        primary_mapped = [a for a in primary_alignments if not (a.flag & 4)]
        primary_unmapped = [a for a in primary_alignments if a.flag & 4]
        
        assert len(primary_mapped) == 1
        assert len(primary_unmapped) == 1
        assert primary_mapped[0].rname == 'chr1'
        assert primary_unmapped[0].cigar == '*'
        
        # Secondary alignments: should all map to chr2
        assert all(a.rname == 'chr2' for a in secondary_alignments)
        
        # Should have 2 mapped (original reads) and 2 unmapped (synthetic mates)
        secondary_mapped = [a for a in secondary_alignments if not (a.flag & 4)]
        secondary_unmapped = [a for a in secondary_alignments if a.flag & 4]
        
        assert len(secondary_mapped) == 2  # Both original secondary reads
        assert len(secondary_unmapped) == 2  # Two synthetic mates
        
        # Both original secondary reads should be UP reads with flag 355
        assert all(a.pair_status == 'UP' for a in secondary_mapped)
        assert all(a.flag == 355 for a in secondary_mapped)
        
        # Both synthetic mates should have CIGAR '*' and be unmapped
        assert all(a.cigar == '*' for a in secondary_unmapped)
        
        # All reads should have the same qname
        assert all(a.qname == 'read1' for a in alignments)

    @pytest.mark.parametrize(
        "score1,score2,expected_cigar",
        [
            (100, 100, "1S49M"),  # Same scores: keep first alignment
            (100, 90, "1S49M"),   # First has higher score: keep first
            (90, 100, "10S40M"),  # Second has higher score: keep second
        ],
    )
    def test_up_secondary_alignments_same_coordinates_different_cigar(
        self, score1, score2, expected_cigar
    ):
        # Two unpaired (UP) secondary read1 alignments with same (rname, pos, pnext) but different CIGARs
        # The alignment with the higher score should be kept; if equal, the first one is kept
        sam_content = f"""read1\t97\tchr1\t1000\t0\t50M\t=\t900\t0\tACGTACGTACGTACGTACGTACGTACGTACGT\tIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\tAS:i:100\tYT:Z:UP
read1\t145\tchr1\t900\t255\t50M\t=\t1000\t0\tTGCATGCATGCATGCATGCATGCATGCATGCA\tIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\tAS:i:94\tYT:Z:UP
read1\t353\tchr1\t950\t255\t1S49M\t=\t900\t0\tACGTACGTACGTACGTACGTACGTACGTACGT\tIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\tAS:i:{score1}\tYT:Z:UP
read1\t353\tchr1\t950\t255\t10S40M\t=\t900\t0\tACGTACGTACGTACGTACGTACGTACGTACGT\tIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\tAS:i:{score2}\tYT:Z:UP"""

        filtered_content = """@read1/1
ACGTACGTACGTACGTACGTACGTACGTACGT
+
IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII
@read1/2
TGCATGCATGCATGCATGCATGCATGCATGCA
+
IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII"""

        sam_file = self.create_temp_file(sam_content, '.sam')
        filtered_file = self.create_temp_file(filtered_content, '.fastq')
        output_file = os.path.join(self.temp_dir, 'output.sam')

        filter_viral_sam(sam_file, filtered_file, output_file, 10.0)

        with open(output_file) as f:
            output_lines = f.readlines()

        # Parse all alignments
        alignments = [SamAlignment.from_sam_line(line.strip()) for line in output_lines]

        # Key assertion: No two consecutive alignments should both be read1 or both be read2
        # This is what process_viral_bowtie2_sam.py expects
        for i in range(len(alignments) - 1):
            curr_is_read1 = bool(alignments[i].flag & 64)
            next_is_read1 = bool(alignments[i + 1].flag & 64)
            assert curr_is_read1 != next_is_read1, (
                f"Consecutive alignments at positions {i} and {i+1} are both "
                f"{'read1' if curr_is_read1 else 'read2'}: "
                f"flags {alignments[i].flag} and {alignments[i + 1].flag}"
            )

        # Separate primary and secondary
        primary_alignments = [a for a in alignments if a.flag < 256]
        secondary_alignments = [a for a in alignments if a.flag >= 256]

        # Primary: should have 2 alignments (the original read1 and read2)
        assert len(primary_alignments) == 2

        # Secondary: should have 2 alignments (one read1 + one synthetic read2 mate)
        # The duplicate read1 with a different CIGAR is deduplicated
        assert len(secondary_alignments) == 2, (
            f"Expected 2 secondary alignments (1 read1 + 1 synthetic read2), "
            f"got {len(secondary_alignments)}"
        )

        # Verify secondary has one read1 and one read2
        secondary_read1 = [a for a in secondary_alignments if a.flag & 64]
        secondary_read2 = [a for a in secondary_alignments if a.flag & 128]
        assert len(secondary_read1) == 1, f"Expected 1 secondary read1, got {len(secondary_read1)}"
        assert len(secondary_read2) == 1, f"Expected 1 secondary read2, got {len(secondary_read2)}"

        # Verify the correct alignment was kept based on score
        assert secondary_read1[0].cigar == expected_cigar, (
            f"Expected CIGAR {expected_cigar}, got {secondary_read1[0].cigar}"
        )


class TestGrouping:
    def test_group_concordant_pairs_by_alignment_scores(self):
        # Test that secondary alignments for concordant paired reads with the same 
        # reference genome name (rname), alignment position (pos)
        # reference genome name of the mate (rnext) and template length (tlen) 
        # get grouped separately with their correct mate

        # Arrange
        from filter_viral_sam import group_other_alignments
        alignments = []
        # Secondary pair 1: TLEN ±147, AS:45/42
        # Note: forward and reverse reads have different CIGARs
        line1 = "LH00642:88:22WT2HLT4:4:2351:20319:9912\t339\tEU095850.1\t4575\t255\t19S61M67S\t=\t4575\t147\tACGT\tIIII\tAS:i:45\tYS:i:42\tYT:Z:CP"
        line2 = "LH00642:88:22WT2HLT4:4:2351:20319:9912\t403\tEU095850.1\t4575\t255\t67S61M19S\t=\t4575\t-147\tTGCA\tIIII\tAS:i:42\tYS:i:45\tYT:Z:CP"
        # Secondary pair 2: TLEN ±147, AS:43/40        
        line3 = "LH00642:88:22WT2HLT4:4:2351:20319:9912\t339\tEU095850.1\t4575\t255\t79S61M7S\t=\t4575\t147\tACGT\tIIII\tAS:i:43\tYS:i:40\tYT:Z:CP"
        line4 = "LH00642:88:22WT2HLT4:4:2351:20319:9912\t403\tEU095850.1\t4575\t255\t7S61M79S\t=\t4575\t-147\tTGCA\tIIII\tAS:i:40\tYS:i:43\tYT:Z:CP"
        # Secondary pair 3: TLEN ±207, AS:40/38
        line5 = "LH00642:88:22WT2HLT4:4:2351:20319:9912\t339\tEU095850.1\t4575\t255\t79S61M7S\t=\t4575\t207\tACGT\tIIII\tAS:i:40\tYS:i:38\tYT:Z:CP"
        line6 = "LH00642:88:22WT2HLT4:4:2351:20319:9912\t403\tEU095850.1\t4575\t255\t7S61M79S\t=\t4575\t-207\tTGCA\tIIII\tAS:i:38\tYS:i:40\tYT:Z:CP"
        for line in [line1, line2, line3, line4, line5, line6]:
            alignments.append(SamAlignment.from_sam_line(line))
        # Act
        groups = group_other_alignments(alignments)
        # Assert
        # Should create exactly 3 groups (3 different pairs)
        assert len(groups) == 3, f"Expected 3 groups, got {len(groups)}"
        # Each group should contain exactly 2 alignments (paired reads)
        for group_id, group_alignments in groups.items():
            assert len(group_alignments) == 2, f"Group {group_id} should have 2 alignments, got {len(group_alignments)}"
            # Each group should have one read1 (flag 339) and one read2 (flag 403)
            flags = sorted([a.flag for a in group_alignments])
            assert flags == [339, 403], f"Group {group_id} should have flags [339, 403], got {flags}"
            # Verify that AS and YS scores map correctly between mates
            # The AS score of read1 should equal the YS score of read2, and vice versa
            read1 = [a for a in group_alignments if a.flag == 339][0]
            read2 = [a for a in group_alignments if a.flag == 403][0]
            assert read1.alignment_score == read2.mate_alignment_score, \
                f"Group {group_id}: read1 AS ({read1.alignment_score}) != read2 YS ({read2.mate_alignment_score})"
            assert read2.alignment_score == read1.mate_alignment_score, \
                f"Group {group_id}: read2 AS ({read2.alignment_score}) != read1 YS ({read1.mate_alignment_score})"
            # Each group should have matching abs(TLEN) values
            tlens = [abs(a.tlen) for a in group_alignments]
            assert tlens[0] == tlens[1], f"Group {group_id} abs(TLEN) mismatch: {tlens}"

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
