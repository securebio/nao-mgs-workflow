// Extract information from a paired Bowtie2 SAM file based on Genbank download metadata
// Updated version with testing and gzipped output
process PROCESS_VIRAL_BOWTIE2_SAM {
    label "pysam_biopython"
    label "single"
    input:
        tuple val(sample), path(sam)
        path genbank_metadata_path
        path viral_db_path
        val paired
    output:
        tuple val(sample), path("${sample}_bowtie2_sam_processed.tsv.gz"), emit: output
        tuple val(sample), path("${sample}_bowtie2_sam_in.tsv.gz"), emit: input
    script:
        """
        out=${sample}_bowtie2_sam_processed.tsv.gz
        meta=${genbank_metadata_path}
        db=${viral_db_path}
        paired=${paired ? "--paired" : "--no-paired"}
        cmd="process_viral_bowtie2_sam.py -m \${meta} -v \${db} -o \${out} \${paired}"
        # Sort input SAM and pass to script
        zcat ${sam} | \${cmd}
        # Link input to output for testing
        ln -s ${sam} ${sample}_bowtie2_sam_in.tsv.gz
        """
}
