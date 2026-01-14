#!/usr/bin/env python3

"""
Usage:
   ./identify-conserved-regions.py \
       <downstream_dir> <cache_dir> <index_nodes> <index_genomes> \
       <index_metadata> <jobs>

Where:
    downstream_dir:
        directory containing *_validation_hits.tsv.gz files to process

    cache_dir:
        temporary directory we can use to store our work, to enable fast
        resumes during development. Can be deleted when you're done
        with this script.

    index_nodes: path to the relevant taxonomy-nodes.dmp

    index_genomes: path to the relevant virus-genomes-masked.fasta.gz

    index_metadata: path to the relevant virus-genome-metadata-gid.tsv.gz

    jobs: number of cores to use for heavy processing
"""

import os
import csv
import sys
import glob
import gzip
import subprocess
from collections import defaultdict
from Bio.SeqIO.FastaIO import SimpleFastaParser

ENTEROVIRUS_C_TAXID = 138950
POLIO_TAXIDS = [
    12080,  # Poliovirus 1
    12083,  # Poliovirus 2
    12086,  # Poliovirus 3
    909390, # Recombinant polioviruses
    12079,  # Unidentified poliovirus
    53259,  # Wild poliovirus type 3
]

def parse_taxonomy_file(index_nodes_fname):
    """Parse the taxonomy nodes file and build parent-child mappings."""
    children = defaultdict(set)  # parent_id -> {child_ids}

    with open(index_nodes_fname, 'r') as f:
        for line in f:
            parts = [p.strip() for p in line.split('|')]
            node_id = int(parts[0])
            parent_id = int(parts[1])

            children[parent_id].add(node_id)

    return children

def get_descendants(node_id, children, descendants):
    """Build list of all taxids at or below the provided level"""
    descendants.add(node_id)
    for child_id in children[node_id]:
        get_descendants(child_id, children, descendants)

def subset_to_enterovirus_c(
        downstream_in, enterovirus_c_cache_dir, index_nodes_fname, jobs):
    subset_script_fname = os.path.join(
        os.path.dirname(__file__), "..", "subset",
        "subset-validation-hits-by-clade.sh")

    subprocess.run([
        subset_script_fname, downstream_in, enterovirus_c_cache_dir,
        str(jobs), index_nodes_fname, str(ENTEROVIRUS_C_TAXID)],
                   check=True)

def collect_genomes(
        target_genomes, genome_cache_fname, index_genomes_fname):
    if not os.path.exists(genome_cache_fname):
        with gzip.open(index_genomes_fname, "rt") as inf, \
             gzip.open(genome_cache_fname, "wt") as outf:
            for seq_id, seq in SimpleFastaParser(inf):
                genome_id = seq_id.split()[0]
                if genome_id not in target_genomes:
                    continue
                outf.write(f">{seq_id}\n{seq}\n")

def get_clade_taxids(index_nodes_fname):
    children = parse_taxonomy_file(index_nodes_fname)

    enterovirus_c_clade_taxids = set()
    get_descendants(ENTEROVIRUS_C_TAXID, children, enterovirus_c_clade_taxids)

    polio_clade_taxids = set()
    for polio_taxid in POLIO_TAXIDS:
        get_descendants(polio_taxid, children, polio_clade_taxids)

    return enterovirus_c_clade_taxids, polio_clade_taxids

def taxids_to_genomes(taxids, index_metadata_fname):
    genomes = set()
    with gzip.open(index_metadata_fname, "rt") as inf:
        for row in csv.DictReader(inf, delimiter="\t"):
            if int(row["taxid"]) in taxids:
                genomes.add(row["genome_id"])
    return genomes

def build_bowtie2_index(index_fname, genomes_fname):
    # Check if index already exists by looking for the .1.bt2 file
    if os.path.exists(f"{index_fname}.1.bt2"):
        return

    # Build the bowtie2 index
    subprocess.check_call([
        "bowtie2-build",
        genomes_fname,
        index_fname
    ])

def bowtie2_align(bt_index, input_fastq_gz, jobs,
                  out_aligning, out_not_aligning):
    subprocess.check_call([
        "bowtie2",
        "-x", bt_index,
        "-U", input_fastq_gz,
        "--al-gz", out_aligning,
        "--un-gz", out_not_aligning,
        "-S", "/dev/null",  # we don't want the SAM file.
        "--quiet",
        "--very-sensitive-local",
        "--score-min", "L,0,0.4", # lower threshold
        "-N", "1",  # allow 1bp mismatch in initial seed
        "--threads", str(jobs),
    ])

def prepare_genomes_for_alignment(
        enterovirus_c_cache_dir, enterovirus_c_fastqs):
    """Extract sequences and qualities from validation_hits.tsv.gz files, and
    write a fastq.gz file if it doesn't already exist."""
    if os.path.exists(enterovirus_c_fastqs):
        return

    with gzip.open(enterovirus_c_fastqs, "wt") as outf:
        for fname in glob.glob(os.path.join(enterovirus_c_cache_dir, "*.gz")):
            group_hash = os.path.basename(fname).split("_")[0]
            with gzip.open(fname, "rt") as inf:
                for row in csv.DictReader(inf, delimiter="\t"):
                    outf.write(
                        f"@{group_hash} {row['seq_id']}/1\n" +
                        row["query_seq"] + "\n+\n" +
                        row["query_qual"] + "\n")
                    if "query_seq_rev" not in row:
                        continue  # ONT
                    outf.write(
                        f"@{group_hash} {row['seq_id']}/2\n" +
                        row["query_seq_rev"] + "\n+\n" +
                        row["query_qual_rev"] + "\n")

def start(downstream_in,
          work_dir,
          index_nodes_fname,
          index_genomes_fname,
          index_metadata_fname,
          jobs):

    enterovirus_c_cache_dir = os.path.join(work_dir, "enterovirus-c")
    genome_cache_polio_fname = os.path.join(work_dir, "polio.fasta.gz")
    genome_cache_non_polio_fname = os.path.join(work_dir, "non-polio.fasta.gz")
    enterovirus_c_fastqs = os.path.join(work_dir, "enterovirus-c.fastq.gz")
    likely_polio_fastqs = os.path.join(
        work_dir, "likely_polio.fastq.gz")
    likely_non_polio_fastqs = os.path.join(
        work_dir, "likely_non_polio.fastq.gz")
    unclear_fastqs = os.path.join(work_dir, "unclear.fastq.gz")
    polio_fastqs = os.path.join(work_dir, "polio.fastq.gz")

    subset_to_enterovirus_c(
        downstream_in, enterovirus_c_cache_dir, index_nodes_fname, jobs)
    enterovirus_c_clade_taxids, polio_clade_taxids = get_clade_taxids(
        index_nodes_fname)

    polio_genomes = taxids_to_genomes(polio_clade_taxids, index_metadata_fname)
    non_polio_genomes = taxids_to_genomes(
        enterovirus_c_clade_taxids - polio_clade_taxids, index_metadata_fname)

    collect_genomes(
        polio_genomes, genome_cache_polio_fname,
        index_genomes_fname)
    collect_genomes(
        non_polio_genomes, genome_cache_non_polio_fname,
        index_genomes_fname)

    polio_bt_index = os.path.join(work_dir, "bt-polio")
    non_polio_bt_index = os.path.join(work_dir, "bt-non-polio")

    build_bowtie2_index(polio_bt_index, genome_cache_polio_fname)
    build_bowtie2_index(non_polio_bt_index, genome_cache_non_polio_fname)

    prepare_genomes_for_alignment(
        enterovirus_c_cache_dir, enterovirus_c_fastqs)

    # 1. Align all genomes against the non-polio index and drop anything that
    #    matches, to remove reads that are likely hits to conserved sequences.
    bowtie2_align(
        non_polio_bt_index, enterovirus_c_fastqs, jobs,
        out_aligning=likely_non_polio_fastqs,
        out_not_aligning=likely_polio_fastqs)

    # 2. Align all remaining genomes against the polio index, and drop anything
    #    that doesn't match, just to be sure.
    bowtie2_align(
        polio_bt_index, likely_polio_fastqs, jobs,
        out_aligning=polio_fastqs,
        out_not_aligning=unclear_fastqs)

    print(f"Reads that are very likely actually polio are in {polio_fastqs}")

if __name__ == "__main__":
    start(*sys.argv[1:])
