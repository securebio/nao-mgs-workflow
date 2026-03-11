#!/usr/bin/env python

"""
Given a sorted Bowtie2 SAM file, extract information about the alignments and write to a TSV.
"""

#=======================================================================
# Import modules
#=======================================================================

# Import modules
import logging
from datetime import datetime, timezone
import sys
import re
import argparse
import pandas as pd
import time
import gzip
import math
from Bio import Seq
import io
from typing import IO, Sequence, cast

# Type alias for SAM field dictionaries that contain mixed value types
type FieldValue = str | int | bool | float | None
type FieldDict = dict[str, FieldValue]

#=======================================================================
# Configure constants
#=======================================================================

SAM_HEADERS_PAIRED = [
    "seq_id",
    "genome_id", "genome_id_all",
    "taxid", "taxid_all",
    "fragment_length",
    "genome_id_fwd", "genome_id_rev",
    "taxid_fwd", "taxid_rev",
    "best_alignment_score", "best_alignment_score_rev",
    "next_alignment_score", "next_alignment_score_rev",
    "edit_distance", "edit_distance_rev",
    "ref_start", "ref_start_rev",
    "map_qual", "map_qual_rev",
    "cigar", "cigar_rev",
    "query_len", "query_len_rev",
    "query_seq", "query_seq_rev",
    "query_rc", "query_rc_rev",
    "query_qual", "query_qual_rev",
    "length_normalized_score_fwd", 
    "length_normalized_score_rev",
    "length_normalized_score",
    "pair_status",
    "classification"
]

SAM_HEADERS_UNPAIRED = [
    "seq_id",
    "genome_id", "taxid",
    "best_alignment_score", "next_alignment_score",
    "edit_distance", "ref_start",
    "map_qual", "cigar",
    "query_len", "query_seq",
    "query_rc", "query_qual",
    "length_normalized_score",
    "classification"
]

#=======================================================================
# Configure logging
#=======================================================================

class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter('[%(asctime)s] %(message)s')
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#=======================================================================
# I/O functions
#=======================================================================

def open_by_suffix(filename: str, mode: str = "r") -> IO[str]:
    """
    Open a file with the appropriate compression, as inferred from the filename suffix.
    Args:
        filename (str): Path to file.
        mode (str): Mode to open file in.
    Returns:
        IO[str]: File object.
    """
    logger.debug(f"\tOpening file object: {filename}")
    logger.debug(f"\tOpening mode: {mode}")
    logger.debug(f"\tGZIP mode: {filename.endswith('.gz')}")
    if filename.endswith('.gz'):
        return cast(IO[str], gzip.open(filename, mode + 't'))
    else:
        return open(filename, mode)

def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description="Process a sorted Bowtie2 SAM file into a TSV with additional information.")
    parser.add_argument("-s", "--sam", type=lambda f: open_by_suffix(f, "r"), default=sys.stdin,
                        help="Path to Bowtie2 SAM file (default: stdin).")
    parser.add_argument("-m", "--metadata", required=True,
                        help="Path to Genbank download metadata file containing genomeID and taxid information.")
    parser.add_argument("-v", "--viral_db", required=True,
                        help="Path to TSV containing viral taxonomic information.")
    parser.add_argument("-o", "--output", type=lambda f: open_by_suffix(f, "w"), default=sys.stdout,
                        help="Output path for processed data frame (default: stdout).")
    parser.add_argument("--paired", action=argparse.BooleanOptionalAction, default=True,
                        help="Processed SAM file as containing paired read alignments (default: True).")
    args = parser.parse_args()
    return args

def read_genbank_metadata(path: str) -> dict[str, tuple[str, str]]:
    """
    Read Genbank metadata file and return a dictionary mapping genome IDs to taxids.
    Args:
        path: Path to Genbank metadata file.
    Returns:
        dict[str, tuple[str, str]]: Dictionary mapping genome IDs to taxids.
    """
    meta_db = pd.read_csv(path, sep="\t", dtype=str)
    gid_taxid_dict = {genome_id: (taxid, species_taxid)
                      for genome_id, taxid, species_taxid in
                      zip(meta_db["genome_id"],meta_db["taxid"],
                          meta_db["species_taxid"])}
    return gid_taxid_dict

def get_viral_taxids(path: str) -> set[str]:
    """
    Read viral DB file and return a set of viral taxids.
    Args:
        path: Path to viral DB file.
    Returns:
        set[str]: Set of viral taxids.
    """
    virus_db = pd.read_csv(path, sep="\t", dtype=str)
    return set(virus_db["taxid"].values)

def join_line(fields: Sequence[FieldValue]) -> str:
    """
    Convert a list of arguments into a TSV string for output.
    Args:
        fields (list): List of fields to join.
    Returns:
        str: Joined fields.
    """
    return("\t".join(map(str, fields)) + "\n")

def write_sam_headers(out_file: IO[str], paired: bool) -> None:
    """
    Write header line to new TSV.
    Args:
        out_file (IO[str]): Output file to write to.
        paired (bool): Whether the SAM file is paired.
    """
    headers = SAM_HEADERS_PAIRED if paired else SAM_HEADERS_UNPAIRED
    logger.debug(f"Headers: {headers}")
    header_line = join_line(headers)
    logger.debug(f"Writing header line: {header_line}")
    out_file.write(header_line)
    return None

def get_next_alignment(sam_file: IO[str]) -> str | None:
    """Iterate through SAM file lines until gets an alignment line, then returns."""
    while True:
        l = next(sam_file, "EOF") # Get next line
        if not l: continue # Skip empty lines
        if l.startswith("@"): continue # Skip header lines
        if l == "EOF": return None
        return(l)

#=======================================================================
# Single-alignment processing functions
#=======================================================================

def check_flag(flag_sum: int|str,
               flag_dict: dict[str, bool],
               flag_name: str,
               flag_value: int) -> tuple[int, dict[str, bool]]:
    """
    Check if a flag sum includes a specific flag and return adjusted flag sum.
    Args:
        flag_sum (int|str): Flag sum to check. Must be parsable as an integer.
        flag_dict (dict[str, bool]): Dictionary to store flag results.
        flag_name (str): Flag name to store in flag_dict.
        flag_value (int): Flag value to check for.
    Returns:
        tuple[int, dict[str, bool]]: Adjusted flag sum and flag dictionary.
    """
    flag_sum = int(flag_sum)
    if flag_sum >= flag_value:
        flag_dict[flag_name] = True
        flag_sum -= flag_value
    else:
        flag_dict[flag_name] = False
    return flag_sum, flag_dict

def process_sam_flags(flag_sum: int|str) -> dict[str, bool]:
    """
    Extract individual flags from flag sum.
    Args:
        flag_sum (int|str): Flag sum to check. Must be parsable as an integer.
    Returns:
        dict[str, bool]: Dictionary of flag results.
    """
    flag_dict: dict[str, bool] = {}
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "is_supplementary", 2048)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "is_secondary", 256)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "is_mate_2", 128)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "is_mate_1", 64)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "mate_aligned_reverse", 32)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "aligned_reverse", 16)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "no_paired_alignments", 8)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "no_single_alignments", 4)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "proper_paired_alignment", 2)
    flag_sum, flag_dict = check_flag(flag_sum, flag_dict, "in_pair", 1)
    return(flag_dict)

def extract_option(opt_list: list[str],
                   query_value: str,
                   default: str|None = None) -> str|None:
    """
    Extract specific optional field from list of optional fields.
    Args:
        opt_list (list[str]): List of optional fields.
        query_value (str): Value to query for.
        default (str|None): Default value to return if no match is found.
    Returns:
        str|None: Extracted value or default value.
    """
    fields = [f for f in opt_list if query_value in f]
    if len(fields) == 0: return(default)
    assert len(fields) == 1, f"Multiple fields found for {query_value}: {fields}"
    field = fields[0]
    pattern = "{}:.:(.*)".format(query_value)
    out_value: str = re.findall(pattern, field)[0]
    return(out_value)

def extract_optional_fields(opt_list: list[str]) -> dict[str, str | None]:
    """
    Extract relevant optional fields from Bowtie2 SAM alignment.
    Args:
        opt_list (list[str]): List of optional fields.
    Returns:
        dict[str, str | None]: Dictionary of extracted values.
    """
    out = {}
    out["alignment_score"] = extract_option(opt_list, "AS")
    out["next_best_alignment"] = extract_option(opt_list, "XS")
    out["edit_distance"] = extract_option(opt_list, "NM")
    out["pair_status"] = extract_option(opt_list, "YT")
    out["mate_alignment_score"] = extract_option(opt_list, "YS")
    return(out)

def extract_viral_taxid(genome_id: str,
                        genbank_metadata: dict[str, tuple[str, str]],
                        viral_taxids: set[str]) -> str:
    """
    Extract taxid from the appropriate field of Genbank metadata.
    Args:
        genome_id (str): Genome ID to extract taxid for.
        genbank_metadata (dict[str, tuple[str, str]]): Genbank metadata mapping genome IDs to taxids.
        viral_taxids (set[str]): Set of viral taxids.
    Returns:
        str: Taxid.
    """
    try:
        taxid, species_taxid = genbank_metadata[genome_id]
        if taxid in viral_taxids:
            return taxid
        if species_taxid in viral_taxids:
            return species_taxid
        return taxid
    except KeyError:
        msg = "No matching genome ID found: {}".format(genome_id)
        logger.error(msg)
        raise ValueError(msg)

def process_sam_alignment(sam_line: str,
                          genbank_metadata: dict[str, tuple[str, str]],
                          viral_taxids: set[str],
                          paired: bool) -> FieldDict:
    """
    Process a SAM alignment line into a dictionary of fields.
    Args:
        sam_line (str): SAM alignment line.
        genbank_metadata (dict[str, tuple[str, str]]): Genbank metadata mapping genome IDs to taxids.
        viral_taxids (set[str]): Set of viral taxids.
        paired (bool): Whether the SAM file is paired.
    Returns:
        FieldDict: Dictionary of fields.
    """
    # Parse SAM line into fields
    fields_in = sam_line.strip().split("\t")
    # Parse fields into output dictionary in order
    out: FieldDict = {}
    out["seq_id"] = fields_in[0]
    out.update(process_sam_flags(fields_in[1]))
    out["genome_id"] = fields_in[2]
    out["taxid"] = int(extract_viral_taxid(fields_in[2], genbank_metadata, viral_taxids))
    out["ref_start"] = int(fields_in[3]) - 1 # Convert from 1-indexing to 0-indexing
    out["map_qual"] = int(fields_in[4])
    out["cigar"] = fields_in[5]
    if paired:
        out["mate_genome_id"] = fields_in[6]
        out["mate_ref_start"] = int(fields_in[7]) - 1 # Convert as above
        out["fragment_length"] = abs(int(fields_in[8]))
    out["query_seq"] = fields_in[9]
    out["query_qual"] = fields_in[10]
    out.update(extract_optional_fields(fields_in[11:]))
    # Check if alignment is in reverse orientation, and reverse-complement if so
    out["query_rc"] = False
    if out["aligned_reverse"]:
        out["query_seq"] = str(Seq.Seq(str(out["query_seq"])).reverse_complement())
        out["query_qual"] = str(out["query_qual"])[::-1]
        out["query_rc"] = True
    out["query_len"] = len(str(out["query_seq"]))
    return(out)

#=======================================================================
# Line processing functions
#=======================================================================

def get_line(fields_dict: FieldDict,
             paired: bool) -> str:
    """
    Convert a dictionary of fields into an output line.
    Args:
        fields_dict (FieldDict): Dictionary of fields.
        paired (bool): Whether the SAM file is paired.
    Returns:
        str: Output line.
    """
    # First check all required fields are present
    headers = SAM_HEADERS_PAIRED if paired else SAM_HEADERS_UNPAIRED
    for header in headers:
        assert header in fields_dict, \
            f"Required field is missing from dictionary: {header}, {fields_dict}"
    # Then convert dictionary into list of fields
    fields = [fields_dict[header] for header in headers]
    # Then join fields into line
    line = join_line(fields)
    return(line)

def get_line_from_single(read_dict: FieldDict,
                         paired: bool) -> str:
    """
    Generate an output line from a single SAM alignment dictionary.
    Args:
        read_dict (FieldDict): Dictionary of fields.
        paired (bool): Whether the SAM file is paired.
    Returns:
        str: Output line.
    """
    # Calculate length-adjusted alignment score
    adj_score = float(str(read_dict["alignment_score"])) / math.log(float(str(read_dict["query_len"])))
    # Prepare dictionary for output
    out_dict: FieldDict = {
    "seq_id": read_dict["seq_id"],
    "genome_id": read_dict["genome_id"],
    "taxid": read_dict["taxid"],
    "length_normalized_score": adj_score,
    "pair_status": read_dict["pair_status"],
    "classification": (
        "supplementary"  if read_dict["is_supplementary"]
        else "secondary" if read_dict["is_secondary"]
        else "primary")
    }
    if paired:
        # Additional fields for paired SAM
        out_dict["genome_id_all"] = read_dict["genome_id"]
        out_dict["taxid_all"] = read_dict["taxid"]
        out_dict["pair_status"] = read_dict["pair_status"]
        out_dict["fragment_length"] = "NA"
        # Additional fields based on forward/reverse status
        if read_dict["is_mate_1"]:
            out_dict["genome_id_fwd"], out_dict["genome_id_rev"] = read_dict["genome_id"], "NA"
            out_dict["taxid_fwd"], out_dict["taxid_rev"] = read_dict["taxid"], "NA"
            out_dict["best_alignment_score"], out_dict["best_alignment_score_rev"] = read_dict["alignment_score"], "NA"
            out_dict["next_alignment_score"], out_dict["next_alignment_score_rev"] = read_dict["next_best_alignment"], "NA"
            out_dict["edit_distance"], out_dict["edit_distance_rev"] = read_dict["edit_distance"], "NA"
            out_dict["ref_start"], out_dict["ref_start_rev"] = read_dict["ref_start"], "NA"
            out_dict["map_qual"], out_dict["map_qual_rev"] = read_dict["map_qual"], "NA"
            out_dict["cigar"], out_dict["cigar_rev"] = read_dict["cigar"], "NA"
            out_dict["query_len"], out_dict["query_len_rev"] = read_dict["query_len"], "NA"
            out_dict["query_seq"], out_dict["query_seq_rev"] = read_dict["query_seq"], "NA"
            out_dict["query_rc"], out_dict["query_rc_rev"] = read_dict["query_rc"], "NA"
            out_dict["query_qual"], out_dict["query_qual_rev"] = read_dict["query_qual"], "NA"
            out_dict["length_normalized_score_fwd"], out_dict["length_normalized_score_rev"] = adj_score, "NA"
        else: # Is mate 2
            out_dict["genome_id_fwd"], out_dict["genome_id_rev"] = "NA", read_dict["genome_id"]
            out_dict["taxid_fwd"], out_dict["taxid_rev"] = "NA", read_dict["taxid"]
            out_dict["best_alignment_score"], out_dict["best_alignment_score_rev"] = "NA", read_dict["alignment_score"]
            out_dict["next_alignment_score"], out_dict["next_alignment_score_rev"] = "NA", read_dict["next_best_alignment"]
            out_dict["edit_distance"], out_dict["edit_distance_rev"] = "NA", read_dict["edit_distance"]
            out_dict["ref_start"], out_dict["ref_start_rev"] = "NA", read_dict["ref_start"]
            out_dict["map_qual"], out_dict["map_qual_rev"] = "NA", read_dict["map_qual"]
            out_dict["cigar"], out_dict["cigar_rev"] = "NA", read_dict["cigar"]
            out_dict["query_len"], out_dict["query_len_rev"] = "NA", read_dict["query_len"]
            out_dict["query_seq"], out_dict["query_seq_rev"] = "NA", read_dict["query_seq"]
            out_dict["query_rc"], out_dict["query_rc_rev"] = "NA", read_dict["query_rc"]
            out_dict["query_qual"], out_dict["query_qual_rev"] = "NA", read_dict["query_qual"]
            out_dict["length_normalized_score_fwd"], out_dict["length_normalized_score_rev"] = "NA", adj_score
    else: # Is unpaired
        out_dict["best_alignment_score"] = read_dict["alignment_score"]
        out_dict["next_alignment_score"] = read_dict["next_best_alignment"]
        out_dict["edit_distance"] = read_dict["edit_distance"]
        out_dict["ref_start"] = read_dict["ref_start"]
        out_dict["map_qual"] = read_dict["map_qual"]
        out_dict["cigar"] = read_dict["cigar"]
        out_dict["query_len"] = read_dict["query_len"]
        out_dict["query_seq"] = read_dict["query_seq"]
        out_dict["query_rc"] = read_dict["query_rc"]
        out_dict["query_qual"] = read_dict["query_qual"]
    return get_line(out_dict, paired)

def get_line_from_pair(dict_1: FieldDict,
                       dict_2: FieldDict) -> str:
    """
    Generate an output line from two SAM alignment dictionaries.
    Args:
        dict_1 (FieldDict): First SAM alignment dictionary.
        dict_2 (FieldDict): Second SAM alignment dictionary.
    Returns:
        str: Output line.
    """
    # Identify forward and reverse reads
    if dict_1["is_mate_1"] and dict_2["is_mate_1"]:
        msg = f"Both reads are forward reads: {dict_1['seq_id']}"
        logger.error(msg)
        raise ValueError(msg)
    if not dict_1["is_mate_1"] and not dict_2["is_mate_1"]:
        msg = f"Both reads are reverse reads: {dict_1['seq_id']}"
        logger.error(msg)
        raise ValueError(msg)
    fwd_dict = dict_1 if dict_1["is_mate_1"] else dict_2
    rev_dict = dict_1 if not dict_1["is_mate_1"] else dict_2
    # Calculate length-adjusted alignment scores
    try:
        adj_score_fwd = float(str(fwd_dict["alignment_score"])) / math.log(float(str(fwd_dict["query_len"])))
        adj_score_rev = float(str(rev_dict["alignment_score"])) / math.log(float(str(rev_dict["query_len"])))
        adj_score_max = max(adj_score_fwd, adj_score_rev)
        score_fwd_max = adj_score_fwd >= adj_score_rev
    except Exception as e:
        logger.error("Error calculating length-adjusted alignment scores")
        logger.error(f"Forward read: {fwd_dict}")
        logger.error(f"Reverse read: {rev_dict}")
        raise e
    # Calculate scalar values for conflicting alignments
    if fwd_dict["genome_id"] == rev_dict["genome_id"]:
        genome_id_best = fwd_dict["genome_id"]
        genome_id_all = fwd_dict["genome_id"]
        taxid_best = fwd_dict["taxid"]
        taxid_all = fwd_dict["taxid"]
        fragment_length = fwd_dict["fragment_length"]
    else:
        genome_id_best = fwd_dict["genome_id"] if score_fwd_max else rev_dict["genome_id"]
        genome_id_list = [str(fwd_dict["genome_id"]), str(rev_dict["genome_id"])]
        genome_id_all = "/".join(genome_id_list)
        fragment_length = "NA"
        if fwd_dict["taxid"] == rev_dict["taxid"]:
            taxid_best = fwd_dict["taxid"]
            taxid_all = fwd_dict["taxid"]
        else:
            taxid_best = fwd_dict["taxid"] if score_fwd_max else rev_dict["taxid"]
            taxid_list = [str(fwd_dict["taxid"]), str(rev_dict["taxid"])]
            taxid_all = "/".join(taxid_list)
    # Prepare dictionary for output
    out_dict: FieldDict = {
        "seq_id": fwd_dict["seq_id"],
        "genome_id": genome_id_best,
        "genome_id_all": genome_id_all,
        "taxid": taxid_best,
        "taxid_all": taxid_all,
        "fragment_length": fragment_length,
        "genome_id_fwd": fwd_dict["genome_id"],
        "genome_id_rev": rev_dict["genome_id"],
        "taxid_fwd": fwd_dict["taxid"],
        "taxid_rev": rev_dict["taxid"],
        "best_alignment_score": fwd_dict["alignment_score"],
        "best_alignment_score_rev": rev_dict["alignment_score"],
        "next_alignment_score": fwd_dict["next_best_alignment"],
        "next_alignment_score_rev": rev_dict["next_best_alignment"],
        "edit_distance": fwd_dict["edit_distance"],
        "edit_distance_rev": rev_dict["edit_distance"],
        "ref_start": fwd_dict["ref_start"],
        "ref_start_rev": rev_dict["ref_start"],
        "map_qual": fwd_dict["map_qual"],
        "map_qual_rev": rev_dict["map_qual"],
        "cigar": fwd_dict["cigar"],
        "cigar_rev": rev_dict["cigar"],
        "query_len": fwd_dict["query_len"],
        "query_len_rev": rev_dict["query_len"],
        "query_seq": fwd_dict["query_seq"],
        "query_seq_rev": rev_dict["query_seq"],
        "query_rc": fwd_dict["query_rc"],
        "query_rc_rev": rev_dict["query_rc"],
        "query_qual": fwd_dict["query_qual"],
        "query_qual_rev": rev_dict["query_qual"],
        "length_normalized_score_fwd": adj_score_fwd,
        "length_normalized_score_rev": adj_score_rev,
        "length_normalized_score": adj_score_max,
        "pair_status": fwd_dict["pair_status"],
        "classification": "secondary" if fwd_dict["is_secondary"] else "primary",
        }
    return get_line(out_dict, True)

#=======================================================================
# File processing functions
#=======================================================================

def check_pair_status(line_dict: FieldDict,
                      paired: bool) -> None:
    """
    Check if pair status is valid for SAM entry line.
    Args:
        line_dict (FieldDict): SAM alignment dictionary.
        paired (bool): Whether the SAM file is paired.
    """
    pair_status = line_dict["pair_status"]
    if pair_status not in ["UU", "UP", "CP", "DP"]:
        raise ValueError(f"Invalid pair status: {line_dict['seq_id']}, {pair_status}")
    elif paired and pair_status == "UU":
        raise ValueError(f"Unpaired read in paired SAM file: {line_dict['seq_id']}")
    elif not paired and pair_status != "UU":
        raise ValueError(f"Paired read in unpaired SAM file: {line_dict['seq_id']}")

def process_paired_sam(inf: IO[str],
                       outf: IO[str],
                       genbank_metadata: dict[str, tuple[str, str]],
                       viral_taxids: set[str]) -> None:
    """
    Process paired SAM file into a TSV.
    Args:
        inf (IO[str]): Input SAM file.
        outf (IO[str]): Output TSV file.
        genbank_metadata (dict[str, tuple[str, str]]): Genbank metadata mapping genome IDs to taxids.
        viral_taxids (set[str]): Set of viral taxids.
    """
    # Write headers
    write_sam_headers(outf, True)
    # Check if the SAM file is empty (check first line)
    fwd_line = get_next_alignment(inf)
    if fwd_line is None:
        msg = (
            "Input SAM file contains no alignments. "
            "Creating empty output with header only."
        )
        logger.warning(msg)
        return
    # Get the next alignment for paired processing
    rev_line = get_next_alignment(inf)
    while True:
        if fwd_line is None:
            if rev_line is not None: # Break if reverse line exists without forward line
                rev_dict = process_sam_alignment(rev_line, genbank_metadata, viral_taxids, True)
                msg = f"Invalid data: reverse line exists without forward line: {rev_dict['seq_id']}"
                logger.error(msg)
                raise ValueError(msg)
            break
        # Extract forward read information and check pair status
        fwd_dict = process_sam_alignment(fwd_line, genbank_metadata, viral_taxids, True)
        check_pair_status(fwd_dict, True)
        if rev_line is None: # If no reverse line, check pair status, then process forward line as unpaired
            if fwd_dict["pair_status"] != "UP":
                msg = f"Forward read is paired but reverse read is missing: {fwd_dict['seq_id']}"
                logger.error(msg)
                raise ValueError(msg)
            line = get_line_from_single(fwd_dict, True)
            outf.write(line)
            fwd_line = rev_line
            rev_line = get_next_alignment(inf)
            continue
        # Extract reverse read information and check pair status
        rev_dict = process_sam_alignment(rev_line, genbank_metadata, viral_taxids, True)
        check_pair_status(rev_dict, True)
        # Check for sorting
        if str(fwd_dict["seq_id"]) > str(rev_dict["seq_id"]):
            msg = f"Reads are not sorted: encountered {fwd_dict['seq_id']} before {rev_dict['seq_id']}"
            logger.error(msg)
            raise ValueError(msg)
        # Check if read IDs match
        if rev_dict["seq_id"] != fwd_dict["seq_id"]:
            # If IDs mismatch, forward read should be unpaired
            if fwd_dict["pair_status"] != "UP":
                msg = f"Forward read is paired but reverse read is missing: {fwd_dict['seq_id']}"
                logger.error(msg)
                raise ValueError(msg)
            line = get_line_from_single(fwd_dict, True)
            outf.write(line)
            fwd_line = rev_line
            rev_line = get_next_alignment(inf)
            continue
        # Check that pair statuses match
        if rev_dict["pair_status"] != fwd_dict["pair_status"]:
            msg = f"Pair status mismatch: {fwd_dict['seq_id']}, {fwd_dict['pair_status']}, {rev_dict['pair_status']}"
            raise ValueError(msg)
        # If either line is missing a valid alignment, process the other as solo, else process pair together
        if fwd_dict["alignment_score"] is None:
            line = get_line_from_single(rev_dict, True)
        elif rev_dict["alignment_score"] is None:
            line = get_line_from_single(fwd_dict, True)
        else:
            line = get_line_from_pair(fwd_dict, rev_dict)
        outf.write(line)
        fwd_line = get_next_alignment(inf)
        rev_line = get_next_alignment(inf)
        continue

def process_unpaired_sam(inf: IO[str],
                         outf: IO[str],
                         genbank_metadata: dict[str, tuple[str, str]],
                         viral_taxids: set[str]) -> None:
    """
    Process unpaired SAM file into a TSV.
    Args:
        inf (IO[str]): Input SAM file.
        outf (IO[str]): Output TSV file.
        genbank_metadata (dict[str, tuple[str, str]]): Genbank metadata mapping genome IDs to taxids.
        viral_taxids (set[str]): Set of viral taxids.
    """
    # Write headers
    logger.debug("Writing headers")
    write_sam_headers(outf, False)
    # Check if the SAM file is empty (check first line)
    input_line = get_next_alignment(inf)
    logger.debug(f"First line: {input_line}")
    if input_line is None:
        msg = (
            "Input SAM file contains no alignments. "
            "Creating empty output with header only."
        )
        logger.warning(msg)
        return
    # Get next alignment to check sorting
    next_line = get_next_alignment(inf)
    logger.debug(f"Next line: {next_line}")
    # Iterate over input lines individually until end of file reached
    while input_line is not None:
        read_dict = process_sam_alignment(input_line, genbank_metadata, viral_taxids, False)
        logger.debug(f"Read dict: {read_dict}")
        if next_line is not None: # Check sorting
            next_dict = process_sam_alignment(next_line, genbank_metadata, viral_taxids, False)
            if str(read_dict["seq_id"]) > str(next_dict["seq_id"]):
                msg = f"Reads are not sorted: encountered {read_dict['seq_id']} before {next_dict['seq_id']}"
                logger.error(msg)
                raise ValueError(msg)
        # Check pair status
        check_pair_status(read_dict, False)
        # Write line to output file
        output_line = get_line_from_single(read_dict, False)
        logger.debug(f"Output line: {output_line}")
        outf.write(output_line)
        # Get next alignment
        input_line = next_line
        next_line = None if input_line is None else get_next_alignment(inf)
    return

#=======================================================================
# Main function
#=======================================================================

def main() -> None:
    logger.info("Initializing script.")
    start_time = time.time()
    # Parse arguments
    logger.info("Parsing arguments.")
    args = parse_args()
    try:
        # Log parameters
        logger.info(f"SAM file path: {args.sam}")
        logger.info(f"Genbank metadata file path: {args.metadata}")
        logger.info(f"Viral DB file path: {args.viral_db}")
        logger.info(f"Output path: {args.output}")
        logger.info(f"Processing file as paired: {args.paired}")
        # Import metadata and viral DB
        logger.info("Importing Genbank metadata file...")
        gid_taxid_dict = read_genbank_metadata(args.metadata)
        logger.info("Importing viral DB file...")
        virus_taxa = get_viral_taxids(args.viral_db)
        logger.info(f"Imported {len(virus_taxa)} virus taxa.")
        # Process SAM
        logger.info("Processing SAM file...")
        sam_fn = process_paired_sam if args.paired else process_unpaired_sam
        sam_fn(args.sam, args.output, gid_taxid_dict, virus_taxa)
        logger.info("File processed successfully.")
    finally:
        args.sam.close()
        args.output.close()
        end_time = time.time()
        logger.info(f"Total time elapsed: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
