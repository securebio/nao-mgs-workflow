#!/usr/bin/env python3

"""
Given a sorted (by read ID) fastq file of filtered reads, a sorted (by read ID) SAM file with no header,
and a minimum normalized alignment score threshold, filter the SAM file by the following steps:
Keep only the read ids from the SAM file that are also in the read ids from the fastq file, then apply score thresholding on those reads, and finally
add missing mates for unpaired reads.
"""

# =======================================================================
# Preamble
# =======================================================================

import math
import logging
import gzip
import bz2
import argparse
import datetime
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Iterator
from collections import defaultdict
from Bio import SeqIO


# Configure logging
class UTCFormatter(logging.Formatter):
    """
    Custom logging formatter that displays timestamps in UTC.

    Returns:
        Formatted log timestamps in UTC timezone
    """

    def formatTime(self, record, datefmt=None):
        """
        Format log timestamps in UTC timezone.

        Args:
            record: LogRecord object containing timestamp data
            datefmt: Optional date format string (unused)

        Returns:
            Formatted timestamp string in UTC
        """
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

# =======================================================================
# I/O functions
# =======================================================================


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for viral SAM filtering.

    Returns:
        argparse.Namespace: Parsed arguments containing input_sam, filtered_fastq, output_sam, and score_threshold
    """
    desc = (
        "Given a sorted (by read ID) fastq file of filtered reads, a sorted (by read ID) SAM file with no header, "
        "and a minimum normalized alignment score threshold, filter the SAM file by the following steps:"
        "Keep only the read ids from the SAM file that are also in the read ids from the fastq file, then apply score thresholding on those reads, and finally"
        "add missing mates for unpaired reads."
    )
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("input_sam", help="Input SAM file to filter")
    parser.add_argument(
        "filtered_fastq", help="FASTQ file containing filtered reads to keep"
    )
    parser.add_argument("output_sam", help="Output filtered SAM file")
    parser.add_argument(
        "score_threshold",
        type=float,
        help="Minimum normalized alignment score threshold",
    )
    return parser.parse_args()


def open_by_suffix(filename: str, mode: str = "r"):
    """
    Parse the suffix of a filename to determine the right open method
    to use, then open the file. Can handle .gz, .bz2, and uncompressed files.

    Args:
        filename (str): Path to file to open
        mode (str): File open mode (default "r")

    Returns:
        File handle appropriate for the file compression type
    """
    if filename.endswith(".gz"):
        return gzip.open(filename, mode + "t")
    elif filename.endswith(".bz2"):
        return bz2.BZ2File(filename, mode)
    else:
        return open(filename, mode)


# =======================================================================
# Dataclass
# =======================================================================


@dataclass
class SamAlignment:
    """
    Represents a single SAM alignment record with parsed fields and metadata.
    """

    qname: str  # query name/read id
    flag: int  # bitwise FLAG
    rname: str  # reference genome name
    pos: int  # alignment position
    mapq: int  # mapping quality
    cigar: str  # CIGAR string
    rnext: str  # reference genome name of the mate
    pnext: int  # alignment position of the mate
    tlen: int  # insert size
    seq: str  # sequence
    qual: str  # quality score for sequence
    pair_status: Optional[str]  # pair status
    alignment_score: Optional[int]  # alignment score
    mate_alignment_score: Optional[int]  # alignment score of the mate
    normalized_score: float | None  # normalized alignment score
    line: str  # original SAM line
    fields: list[str]

    @classmethod
    def from_sam_line(cls, line: str) -> "SamAlignment":
        """
        Parse a SAM line and create a SamAlignment object.

        Args:
            line: Raw SAM format line

        Returns:
            SamAlignment object with parsed fields and metadata
        """
        fields = line.strip().split("\t")
        pair_status = None
        alignment_score = None
        mate_alignment_score = None
        # Extract YT (pair status) and AS (alignment score) tags
        for field in fields[11:]:
            if field.startswith("YT:Z:"):
                pair_status = field.split(":")[2]
            elif field.startswith("AS:i:"):
                alignment_score = int(field.split(":")[2])
            elif field.startswith("YS:i:"):
                mate_alignment_score = int(field.split(":")[2])
        return cls(
            qname=fields[0],
            flag=int(fields[1]),
            rname=fields[2],
            pos=int(fields[3]),
            mapq=int(fields[4]),
            cigar=fields[5],
            rnext=fields[6],
            pnext=int(fields[7]),
            tlen=int(fields[8]),
            seq=fields[9],
            qual=fields[10],
            pair_status=pair_status,
            alignment_score=alignment_score,
            mate_alignment_score=mate_alignment_score,
            normalized_score=None,  # Will be calculated later
            line=line,
            fields=fields,
        )

    def calculate_normalized_score(self) -> None:
        """
        Calculate and set the normalized alignment score.

        Normalizes the alignment score by dividing by the log of sequence length.
        Sets normalized_score to 0.0 if alignment_score is None or sequence is empty.

        Returns:
            None (modifies self.normalized_score in place)
        """
        if self.alignment_score is None or len(self.seq) == 0:
            self.normalized_score = 0.0
        else:
            self.normalized_score = self.alignment_score / math.log(len(self.seq))

    def create_unmapped_mate(self) -> "SamAlignment":
        """
        Create a synthetic unmapped mate for a read in a SAM file.

        When Bowtie2 is run with secondary alignments enabled, its SAM output can cause issues for our downstream script, `process_bowtie_viral_sam.py`. This script processes the SAM file line by line, expecting that consecutive entries with the same `read_id` contain consistent metadata in key columns (such as alignment status and FLAG).

        The problem arises specifically when secondary alignments are present. In some cases, Bowtie2 will output an unmapped primary alignment for one mate, but then also output secondary alignments, all sharing the same `read_id`. This causes `process_bowtie_viral_sam.py` to encounter consecutive entries with the same `read_id`, but conflicting metadata—e.g., the first entry is marked as unmapped, however the second entry has the same `read_id`. This inconsistency leads the script to throw an error.

        To address this, we generate a synthetic unmapped mate entry whenever its absence would create metadata conflicts for entries with the same `read_id`. This synthesized entry uses information from the mapped mate, but key fields—most importantly the FLAG—are updated to accurately indicate an unmapped status. This ensures that every set of consecutive reads with the same `read_id` has consistent metadata, allowing the downstream script to operate without errors.

        Those interested in learning more about SAM format can refer to this, https://samtools.github.io/hts-specs/SAMv1.pdf.

        This is used for UP (unpaired) reads that need synthetic unmapped mates.
        The SAM flag manipulation:
        - XOR with 192 (0b11000000) flips read1/read2 bits (64|128)
        - OR with 4 (0b100) sets the unmapped flag
        - AND with ~8 (NOT 0b1000) clears the mate unmapped flag

        Returns:
            SamAlignment object representing the synthetic unmapped mate

        """
        fields = self.fields[:]
        old_flag = self.flag
        # Note: The FLAG column in the SAM file uses bits to indicate information about the alignment.
        # SAM flag bits 0x40 (64) and 0x80 (128) indicate read position within the template:
        # 0x40: first segment in template (typically read1 in paired-end)
        # 0x80: last segment in template (typically read2 in paired-end)
        # To create a synthetic unmapped mate, we flip these bits so the mate appears
        # as the complementary read in the pair (read1 ↔ read2).
        # We set the unmapped flag (0x4) and clear the mate unmapped flag (0x8)
        # to indicate this read is unmapped while its mate has a valid alignment.
        new_flag = (old_flag ^ 192) | 4  # XOR flips bits 6&7 (64|128), OR adds unmapped
        new_flag &= ~8  # Ensure mate unmapped flag is clear
        # Update fields for unmapped mate
        fields[1] = str(new_flag)
        fields[5] = "*"  # CIGAR = * (unmapped)
        fields[6] = "="  # RNEXT = same chromosome as mate
        fields[7] = self.fields[3]  # PNEXT = mate's position
        mate_line = "\t".join(fields)
        mate = SamAlignment.from_sam_line(mate_line)
        return mate


# =======================================================================
# Read ID specific functions
# =======================================================================


def group_unpaired_alignments(
    alignments: list[SamAlignment],
) -> Dict[int, list[SamAlignment]]:
    """
    Group unpaired (UP) alignments by position and mate information.

    For UP reads, groups alignments by unique combinations of query name, reference name,
    and position ranges. Handles unmapped, self-referential, and cross-reference mappings
    as individual groups.

    An intermediate dictionary is used to map pair keys to group IDs.

    After grouping, deduplicates within each group to keep at most one read1 and one
    read2 alignment. When multiple alignments exist for the same orientation (e.g., two
    read1s with different CIGAR strings but identical coordinates), the alignment with
    the highest alignment score is kept.

    Args:
        alignments (list[SamAlignment]): List of SamAlignment objects with pair_status "UP"

    Returns:
        Dict[int, list[SamAlignment]]: Dictionary mapping group IDs to lists of alignments in each group
    """
    by_group: Dict[int, list[SamAlignment]] = defaultdict(list)
    pair_key_to_group: dict[tuple, int] = {}
    group_idx = 0

    for alignment in alignments:
        # If an alignment maps to itself, it has no mate, hence we put it in its own group
        if alignment.pnext == 0 or alignment.pos == alignment.pnext:
            by_group[group_idx].append(alignment)
            group_idx += 1
            continue
        # If an alignment has a mate that maps to a different reference genome, we put it in its own group
        # NB: Technically, we should be grouping these together as well, but
        # this makes the grouping logic more complex with little benefit.
        # The issue arises from multi-mapping alignments where the mate for a secondary alignment maps to a primary alignment.
        if alignment.rnext != "=" and alignment.rname != alignment.rnext:
            by_group[group_idx].append(alignment)
            group_idx += 1
            continue
        # For all alignments where the mate exists and maps to the same reference genome, we group by position and reference genome name
        # Find a unique key that you can use to group alignments that have mates
        pos_min = min(alignment.pos, alignment.pnext)
        pos_max = max(alignment.pos, alignment.pnext)
        pair_key = (alignment.rname, pos_min, pos_max)
        # If the key hasn't been seen before, then this alignment is put into a new group, and receives a new group ID
        if pair_key not in pair_key_to_group:
            pair_key_to_group[pair_key] = group_idx
            group_idx += 1
        # Get the group ID for this key and add the alignment
        group_id = pair_key_to_group[pair_key]
        by_group[group_id].append(alignment)
    # Deduplicate within each group: keep at most one read1 and one read2.
    # This handles cases where multiple alignments with the same orientation
    # (e.g., two read1s with different CIGAR strings) get grouped together
    # due to having identical (rname, pos_min, pos_max).
    filtered_groups: dict[int, list[SamAlignment]] = {}
    for group_id, group_alignments in by_group.items():
        read1_alns = [a for a in group_alignments if a.flag & 64]
        read2_alns = [a for a in group_alignments if a.flag & 128]
        # Sort by alignment score (descending) to keep highest-scoring alignment
        read1_alns.sort(key=lambda a: a.alignment_score or 0, reverse=True)
        read2_alns.sort(key=lambda a: a.alignment_score or 0, reverse=True)
        result = []
        if read1_alns:
            result.append(read1_alns[0])
        if read2_alns:
            result.append(read2_alns[0])
        if len(read1_alns) > 1 or len(read2_alns) > 1:
            logger.debug(f"Group {group_id}: deduplicated {len(group_alignments)} to {len(result)} alignments")
        filtered_groups[group_id] = result
    return filtered_groups


def group_other_alignments(
    alignments: list[SamAlignment],
) -> Dict[int, list[SamAlignment]]:
    """
    Group other alignments (CP/DP) by position, reference, and template length.

    For CP (Concordant Pair) and DP (Discordant Pair) reads, groups alignments by
    unique combinations of reference names, position ranges, and template length.

    An intermediate dictionary is used to map pair keys to group IDs.

    Args:
        alignments (list[SamAlignment]): List of SamAlignment objects with pair_status "CP" or "DP"

    Returns:
        Dict[int, list[SamAlignment]]: Dictionary mapping group IDs to lists of alignments in each group
    """
    by_group: Dict[int, list[SamAlignment]] = defaultdict(list)
    pair_key_to_group: Dict[tuple, int] = {}
    group_idx = 0

    for alignment in alignments:
        # Find a unique key that you can use to group alignments that have mates
        # To do this we use rname, rnext, pos_min, pos_max, tlen, and alignment_score/mate_alignment_score
        # as not including all these fields has resulted in grouping errors
        pos_min = min(alignment.pos, alignment.pnext)
        pos_max = max(alignment.pos, alignment.pnext)
        abs_tlen = abs(alignment.tlen)
        # For paired alignments (CP/DP), both scores must be present
        assert alignment.alignment_score is not None and alignment.mate_alignment_score is not None, \
            f"Paired alignment missing scores: AS={alignment.alignment_score}, YS={alignment.mate_alignment_score}"
        score_key = tuple(sorted((alignment.alignment_score, alignment.mate_alignment_score)))
        pair_key = (alignment.rname, alignment.rnext, pos_min, pos_max, abs_tlen, score_key)
        # If the key hasn't been seen before, then this alignment is put into a new group, and recieves a new group ID
        if pair_key not in pair_key_to_group:
            pair_key_to_group[pair_key] = group_idx
            group_idx += 1
        # Get the group ID for this key and add the alignment
        group_id = pair_key_to_group[pair_key]
        by_group[group_id].append(alignment)

    # Ensure each group has exactly one forward and one reverse alignment;
    # If there is more than one alignment pair, we select the first forward and reverse alignment pair
    # since there is little value in adding multiple alignment pairs that have the same
    # rname, rnext, pos_min, pos_max, tlen, and alignment_score/mate_alignment_score
    filtered_groups: Dict[int, list[SamAlignment]] = {}
    for group_id, group_alignments in by_group.items():
        if len(group_alignments) != 2:
            read1 = [a for a in group_alignments if a.flag & 64]
            read2 = [a for a in group_alignments if a.flag & 128]
            assert len(read1) == len(read2), \
                f"Group {group_id}: unequal read1 ({len(read1)}) and read2 ({len(read2)}) alignments"
            filtered_groups[group_id] = [read1[0], read2[0]]
            logger.debug(f"Group {group_id}: selected first read1 and read2 from {len(group_alignments)} alignments")
        else:
            filtered_groups[group_id] = group_alignments
    return filtered_groups


def group_secondary_alignments(
    alignments: list[SamAlignment],
) -> Dict[int, list[SamAlignment]]:
    """
    Group alignments by mate pairs using specialized functions based on pair status.

    Delegates to specialized functions based on the pair status:
    - UP (unpaired) reads: groups by position/mate information
    - CP (concordant pair) and DP (discordant pair) reads: groups by position, reference, template length, and alignment/mate alignment scores

    Args:
        alignments (list[SamAlignment]): List of SamAlignment objects to group

    Returns:
        Dict[int, list[SamAlignment]]: Dictionary mapping group IDs to lists of alignments in each group
    """
    # If there are no secondary alignments, return an empty dictionary
    if not alignments:
        return {}
    # Check pair status - all secondary alignments within a read id must have the same pair status
    pair_status = alignments[0].pair_status
    # Check that all secondary alignments have the same pair status
    assert all(a.pair_status == pair_status for a in alignments), (
        "All secondary alignments for a single read ID must share the same "
        f"pair_status. Found: {set(a.pair_status for a in alignments)}"
    )
    if pair_status == "UP":
        return group_unpaired_alignments(alignments)
    else:
        # Handle CP (Concordant Pair) and DP (Discordant Pair) reads
        return group_other_alignments(alignments)


def group_and_apply_score_filter(
    alignments: list[SamAlignment], threshold: float, secondary: bool = False
) -> Dict[int, list[SamAlignment]]:
    """
    Group alignments separately based on their alignment status (primary or secondary),
    then apply the score threshold to each group, based on the max of the bowtie2 length normalized score
    for all alignments in a group.

    Each group is defined by a dictionary where the key is a unique identifier for the group,
    and the value is a list of alignments in the group.

    Args:
        alignments (list[SamAlignment]): List of SamAlignment objects to filter
        threshold (float): Minimum normalized score threshold
        secondary (bool): Whether these are secondary alignments (affects grouping logic)

    Returns:
        Dict[int, list[SamAlignment]]: Dictionary of group IDs to alignment lists that pass the threshold
    """
    if secondary:
        # By default, secondary alignments must not always have mates for each read, hence we must
        # group reads without mates separately from those with mates
        groups = group_secondary_alignments(alignments)
        # Once the groups have been formed, we apply the score threshold and only keep
        # groups that pass
        kept_groups = {}
        for group_id, group_alignments in groups.items():
            max_score = max(
                a.normalized_score
                for a in group_alignments
                if a.normalized_score is not None
            )
            if max_score >= threshold:
                kept_groups[group_id] = group_alignments
            else:
                logger.debug("Discarding group %s", group_id)
        return kept_groups
    else:
        # By default there is at most 1 primary alignment per read in a read pair
        # (i.e. there is at most 1 group), so we can put all primary alignments
        # into a single group, and apply the threshold to the whole group
        max_score = max(
            a.normalized_score for a in alignments if a.normalized_score is not None
        )
        if max_score >= threshold:
            return {0: alignments}  # Single group for primary
        else:
            return {}


def add_synthetic_mates_by_groups(
    groups: Dict[int, list[SamAlignment]],
) -> Dict[int, list[SamAlignment]]:
    """
    Add synthetic mate reads based on provided alignment groups.

    For each group, checks if both read1 and read2 are present. If either is missing
    and the existing alignment has pair_status "UP" (unpaired), creates a synthetic
    unmapped mate to complete the pair.

    Args:
        groups (Dict[int, list[SamAlignment]]): Dictionary mapping group IDs to lists of alignments

    Returns:
        Dict[int, list[SamAlignment]]: Dictionary mapping group IDs to lists of alignments including synthetic mates
    """
    result = {}

    for group_id, group_alignments in groups.items():
        # Check if we have both read1 and read2 in this group
        has_read1 = any(a.flag & 64 for a in group_alignments)
        has_read2 = any(a.flag & 128 for a in group_alignments)
        # Start with existing alignments
        group_result = group_alignments[:]
        # Only create synthetic mates if we're missing either read1 or read2
        if not (has_read1 and has_read2):
            for alignment in group_alignments:
                # Only create synthetic mates for unpaired alignments
                assert alignment.pair_status == "UP" 
                if alignment.pair_status == "UP":
                    unmapped_mate = alignment.create_unmapped_mate()
                    group_result.append(unmapped_mate)
        # Sort this group by flag
        result[group_id] = sorted(group_result, key=lambda x: x.flag)
    return result


def process_all_alignments_for_read_id(
    alignments: list[SamAlignment], score_threshold: float
) -> list[SamAlignment]:
    """
    Process all alignments for a single read id. This includes:

      - Separating primary and secondary alignments
      - Calculating normalized scores for all alignments
      - Grouping alignments then filtering by score threshold
      - Adding synthetic mates (as process_viral_bowtie2_sam.py requires alignments to follow a specific format, where two consecutive alignments cannot be in the same orientation for the same read id)

    Args:
        alignments (list[SamAlignment]): All alignments for one read name
        score_threshold (float): Minimum normalized score threshold

    Returns:
        list[SamAlignment]: All alignments that pass the score threshold for this read id
    """
    # Separate primary and secondary alignments (flag < 256 vs >= 256)
    primary = [a for a in alignments if a.flag < 256]
    # Each read in a pair can have at most 1 primary alignment
    assert len(primary) <= 2
    # There is no limit (other than the one set in Bowtie2) on the number of secondary alignments that a read can have
    secondary = [a for a in alignments if a.flag >= 256]
    # Calculate the bowtie2 length normalized score for all alignments
    for alignment in primary + secondary:
        alignment.calculate_normalized_score()
    # Group the primary alignment(s) together, and return back the group as a dictionary
    # if the max bowtie2 normalized score of that group is greater than the specified score threshold
    primary_groups = group_and_apply_score_filter(primary, score_threshold)
    # If the primary alignment group is empty, then we have filtered out the alignments due to the score threshold
    # and therefore will not consider any secondary alignments, so we return an empty list
    if primary_groups == {}:
        logger.debug(
            "Filtering primary group; no alignments pass threshold, so we throw out the secondary alignments."
        )
        return []
    # Group the secondary alignment(s) together, and return back the group as a dictionary,
    # if the bowtie2 normalized score is greater than the specified score threshold
    secondary_groups = group_and_apply_score_filter(
        secondary, score_threshold, secondary=True
    )
    # Add missing mates using the groups (returns sorted groups)
    primary_with_mates = add_synthetic_mates_by_groups(primary_groups)
    secondary_with_mates = add_synthetic_mates_by_groups(secondary_groups)
    # Combine all groups back into a single list of SamAlignment objects
    result = []
    for group_alignments in primary_with_mates.values():
        result.extend(group_alignments)
    for group_alignments in secondary_with_mates.values():
        result.extend(group_alignments)

    return result


# =======================================================================
# Functions for streaming in data as iterators
# =======================================================================


def stream_filtered_fastq(fastq_file: str) -> Iterator[str]:
    """
    Stream read ids one at a time from sorted FASTQ file.
    Assumes FASTQ is sorted by read ID and handles paired reads correctly.

    For interleaved reads (read1/read2), yields the base read ID only once to avoid
    duplicates when comparing with SAM query names.

    Args:
        fastq_file (str): Path to FASTQ file (can be gzipped)

    Yields:
        str: Read IDs (without @ prefix and /1, /2 suffixes) in sorted order

    Raises:
        ValueError: If FASTQ file is not sorted by read ID
    """
    previous_read_id = None
    last_read_id = None

    try:
        with open_by_suffix(fastq_file) as handle:
            for record in SeqIO.parse(handle, "fastq"):
                # Remove /1, /2 suffixes and space-separated parts if present
                read_id = record.id.split()[0].split("/")[0]
                # Only yield if this is a new read ID (handles paired reads)
                if read_id != previous_read_id:
                    # Check if FASTQ file is sorted before yielding
                    if last_read_id is not None and read_id < last_read_id:
                        msg = f"FASTQ file not sorted by read ID at {read_id}, after {last_read_id}" 
                        logger.error(msg)
                        raise ValueError(msg)
                    yield read_id
                    last_read_id = read_id
                    previous_read_id = read_id
    except Exception as e:
        logger.error(f"Error reading FASTQ file {fastq_file}: {e}")
        raise


def stream_sam_by_qname(sam_file: str) -> Iterator[Tuple[str, list[SamAlignment]]]:
    """
    Stream SAM file and yield groups of alignments by query name (also known as read id).

    This approach assumes the SAM file is sorted by query name.

    Args:
        sam_file (str): Path to SAM file

    Yields:
        Tuple[str, list[SamAlignment]]: Tuples of (qname, list_of_alignments)

    Raises:
        ValueError: If SAM file is not sorted by query name
    """
    current_qname = None
    current_alignments = []
    last_qname = None

    with open_by_suffix(sam_file) as f:
        for line in f:
            if isinstance(line, bytes):
                line = line.decode()
            if line.startswith("@"):
                continue  # Skip header lines
            alignment = SamAlignment.from_sam_line(line)
            if current_qname is None:
                current_qname = alignment.qname
                current_alignments = [alignment]
            elif alignment.qname == current_qname:
                current_alignments.append(alignment)
            else:
                # Check if SAM file is sorted before yielding
                if last_qname is not None and current_qname < last_qname:
                    msg = f"SAM file not sorted! {current_qname} appeared after {last_qname}"
                    logger.error(msg)
                    raise ValueError(msg)
                # New query name, yield previous group
                yield current_qname, current_alignments
                last_qname = current_qname
                current_qname = alignment.qname
                current_alignments = [alignment]
        # Yield final group with final sort check
        if current_qname is not None:
            if last_qname is not None and current_qname < last_qname:
                msg = f"SAM file not sorted by query name at {current_qname}, after {last_qname}"
                logger.error(msg)
                raise ValueError(msg)
            yield current_qname, current_alignments


# =======================================================================
# Main functions
# =======================================================================


def filter_viral_sam(
    input_sam: str, filtered_fastq: str, output_sam: str, score_threshold: float
) -> None:
    """
    Filter viral SAM file using a streaming approach.

    Processing steps:
    1. Stream the read ids from the FASTQ file and read ids/alignments from the SAM file simultaneously using two separate iterators
    2. Use two-pointer merge to only keep read ids in the SAM file that are also in the read ids from the FASTQ file
    3. Discard alignments that don't pass the score threshold
    4. Create synthetic mates for alignments where necessary
    5. Write output immediately without accumulation

    Assumes both filtered FASTQ and SAM files are sorted by query ID.

    Args:
        input_sam (str): Input SAM file path (sorted by query name)
        filtered_fastq (str): FASTQ file containing filtered reads to keep (sorted by read ID)
        output_sam (str): Output filtered SAM file path
        score_threshold (float): Minimum normalized alignment score threshold
    """
    logger.info("Initializing iterators for the FASTQ and SAM file")
    # Create iterators for streaming both files
    filtered_read_ids_iter = stream_filtered_fastq(filtered_fastq)
    sam_iter = stream_sam_by_qname(input_sam)
    # Grab the first read id from the fastq file
    curr_read_id = next(filtered_read_ids_iter, None)
    # Initalizing counters and flags
    alignments_processed = alignments_kept = read_ids_processed = read_ids_skipped = 0
    # Flag used to determine whether the current read id in the FASTQ file exists in SAM file
    # If this flag is set to true, then the read id exists in the SAM file
    # If this flag is set to false, then the read id doesn't exist in the SAM file
    # This flag resets to false everytime we advance to a new read id in the FASTQ file
    fastq_read_id_in_sam = False
    logger.info(f"Processing SAM file with score threshold {score_threshold}")
    # The structure of this loop follows the two-pointer merge algorithm
    # however, the key nuance is that the read ids from the FASTQ file are subset of the read ids from the SAM file
    with open_by_suffix(output_sam, "w") as outf:
        # Iterate over the SAM file, processing all alignments for each read id
        for curr_align_read_id, alignments in sam_iter:
            # Keep track of number of alignments processed and read ids
            alignments_processed += len(alignments)
            read_ids_processed += 1
            # If the read id from the FASTQ file is alphabetically before the read id from the SAM file,
            # the read id has either already been processed or it does not exist in the SAM file
            if curr_read_id is not None and curr_read_id < curr_align_read_id:
                # If the read id in the FASTQ file exists in the SAM file, move to the next read id in the FASTQ file
                if fastq_read_id_in_sam:
                    logger.debug(f"Advancing past filtered ID: {curr_read_id}")
                    curr_read_id = next(filtered_read_ids_iter, None)
                    fastq_read_id_in_sam = False
                # If the read id in the FASTQ file does not exist in the SAM file, throw an error
                else:
                    msg = f"The read id {curr_read_id} from the FASTQ file does not exist in the SAM file, but should given that the read ids in the FASTQ file are a subset of the read ids in the SAM file"

                    logger.error(msg)
                    raise ValueError(msg)
            # If the read id from the FASTQ file is alphabetically greater than the read id from the SAM file, then
            # the read id from the SAM file is not in the FASTQ file and we should move onto the next read id in the SAM file
            if curr_read_id is not None and curr_read_id > curr_align_read_id:
                logger.debug(
                    f"The current read id from the SAM file, {curr_align_read_id}, is not in the FASTQ file, so skipping it"
                )
                read_ids_skipped += 1
                continue
            # If we've gone through all read ids in the FASTQ file, then curr_read_id will be None, which means
            # we've processed all read ids of interest and can discard the remaining SAM alignments
            if curr_read_id is None:
                logger.info(
                    "All filtered reads processed, skipping the remaining alignments"
                )
                break
            # If we've made it this far, this means that the FASTQ read id equals the SAM read id
            # so we can process the alignments for this read id
            # Mark the read id as having been found in the SAM file
            fastq_read_id_in_sam = True
            logger.info(f"Starting {curr_align_read_id}")
            # Process the alignment, returning a list of alignments that pass the score threshold
            kept_alignments = process_all_alignments_for_read_id(
                alignments, score_threshold
            )
            # Keep track of number of alignments kept
            alignments_kept += len(kept_alignments)
            # Write alignments to the file
            for alignment in kept_alignments:
                outf.write(alignment.line)
                if not alignment.line.endswith("\n"):
                    outf.write("\n")
    # Log some summary statistics
    logger.info(
        f"Processed {read_ids_processed} read ids, {alignments_processed} alignments."
        f"Skipped {read_ids_skipped} read ids, kept {alignments_kept} alignments"
    )


def main() -> None:
    """
    Main entry point for viral SAM filtering script.

    Parses command-line arguments and executes the filtering pipeline.

    Returns:
        None
    """
    logger.info("Initializing script.")
    logger.info("Parsing arguments.")
    args = parse_args()
    logger.info("Starting viral SAM filtering")
    filter_viral_sam(
        args.input_sam, args.filtered_fastq, args.output_sam, args.score_threshold
    )


if __name__ == "__main__":
    main()
