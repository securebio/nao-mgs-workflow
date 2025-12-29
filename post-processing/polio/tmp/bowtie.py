def build_bowtie2_index(index_fname, genomes_fname):
    import os
    import subprocess

    # Check if index already exists by looking for the .1.bt2 file
    if os.path.exists(f"{index_fname}.1.bt2"):
        return

    # Build the bowtie2 index
    subprocess.check_call([
        "bowtie2-build",
        genomes_fname,
        index_fname
    ])

def bowtie2_align(bt_index, input_fastq_gz, out_aligning, out_not_aligning):
    import subprocess

    subprocess.check_call([
        "bowtie2",
        "-x", bt_index,
        "--interleaved", input_fastq_gz,
        "--al-conc-gz", out_aligning,
        "--un-conc-gz", out_not_aligning
    ])
