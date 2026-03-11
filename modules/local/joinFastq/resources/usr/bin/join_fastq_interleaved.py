#!/usr/bin/env python

# Import modules
import argparse
import time
import datetime
import gzip
from Bio import SeqIO
from Bio import Seq
import os
from typing import IO, cast

def print_log(message: str) -> None:
    print("[", datetime.datetime.now(), "]  ", message, sep="")

def open_by_suffix(filename: str, mode: str = "r", debug: bool = False) -> IO[str]:
    if debug:
        print_log(f"\tOpening file object: {filename}")
        print_log(f"\tOpening mode: {mode}")
        print_log(f"\tGZIP mode: {filename.endswith('.gz')}")
    if filename.endswith('.gz'):
        return cast(IO[str], gzip.open(filename, mode + 't'))
    else:
        return open(filename, mode)

def join_paired_reads(input_file: str, output_file: str, gap: str = "N", debug: bool = False) -> None:
    """Join non-overlapping paired-end reads from an interleaved FASTQ file."""
    with open_by_suffix(input_file, "r", debug) as inf, open_by_suffix(output_file, "w", debug) as outf:
        # Check if file is empty
        pos = inf.tell()
        first_line = inf.readline()
        if not first_line.strip():
            print_log(f"Warning: Input file {input_file} is empty. Creating empty output file.")
            # Output file is already opened and will be empty when the context manager closes
            return

        # Reset file position to beginning
        inf.seek(pos)
        
        if debug: print_log("\tInitiating parsing...")
        r0 = SeqIO.parse(inf, "fastq") # Read FASTQ
        if debug: print_log("\tOrganizing read pairs...")
        r = zip(r0,r0) # Join FASTQ pairs
        if debug: print_log("\tIterating over read pairs...")
        if debug: nread = 0
        for fwd,rev in r:
            if debug:
                nread += 1
                print_log("\t\tRead {}".format(nread))
            # Read in forward and reverse reads and generate joined sequence
            s1, q1 = str(fwd.seq), fwd.letter_annotations["phred_quality"]
            s2, q2 = str(rev.seq.reverse_complement()), rev.letter_annotations["phred_quality"][::-1]
            joined_seq = Seq.Seq(s1 + gap + s2)
            joined_qual = {"phred_quality": q1 + [0]*len(gap) + q2}
            # Modify the description to include "joined" without duplicating the ID
            description_parts = fwd.description.split(maxsplit=1)
            if len(description_parts) > 1 and description_parts[0] == fwd.id:
                new_description = f"joined {description_parts[1]}"
            else:
                new_description = f"joined {fwd.description}"
            # Collate and return joined read object
            joined_read = SeqIO.SeqRecord(joined_seq, id=fwd.id, name=fwd.name,
                                          description=new_description,
                                          letter_annotations=joined_qual)
            SeqIO.write(joined_read, outf, "fastq")

def main() -> None:
    # Parse arguments
    parser = argparse.ArgumentParser(description="Join non-overlapping interleaved read pairs into single sequences.")
    parser.add_argument("reads", help="Path to FASTQ file containing interleaved forward and reverse reads.")
    parser.add_argument("output_file", help="Path to output FASTQ file containing joined reads.")
    parser.add_argument("-g", "--gap", help="Gap sequence separating joined reads. (Default: N)", default="N", nargs=1)
    parser.add_argument("-d", "--debug", help="Print additional information for debugging. (Default: false)", action="store_true", default=False)
    args = parser.parse_args()
    reads_path = args.reads
    out_path = args.output_file
    gap = args.gap
    debug = args.debug
    # Start time tracking
    print_log("Starting process.")
    start_time = time.time()
    # Print parameters
    print_log("Input reads file (interleaved): {}".format(reads_path))
    print_log("Output reads file: {}".format(out_path))
    print_log("Joining string: {}".format(gap))
    print_log("Debug mode: {}".format(debug))
    # Run joining function
    print_log("Joining reads...")
    join_paired_reads(reads_path, out_path, gap, debug) # NB: Currently implemented serially. Will investigate paralellising if necessary.
    print_log("...done.")
    # Finish time tracking
    end_time = time.time()
    print_log("Total time elapsed: %.2f seconds" % (end_time - start_time))

if __name__ == "__main__":
    main()
