// Process SAM file (add reference taxid, add clean read information, turn into TSV)
process PROCESS_VIRAL_MINIMAP2_SAM {
    label "pysam_biopython"
    label "single"
    input:
        tuple val(sample), path(virus_sam), path(clean_reads)
        path genbank_metadata_path
        path viral_db_path

    output:
        tuple val(sample), path("${sample}_minimap2_sam_processed.tsv.gz"), emit: output
        tuple val(sample), path("input_${virus_sam}"), emit: input
    script:
        """
        process_viral_minimap2_sam.py \
            -a ${virus_sam} -r ${clean_reads} \
            -m ${genbank_metadata_path} -v ${viral_db_path} \
            -o ${sample}_minimap2_sam_processed.tsv.gz

        ln -s ${virus_sam} input_${virus_sam}
        """
}
