// Extract MultiQC data into a more usable form (single/interleaved version)
process SUMMARIZE_MULTIQC {
    label "rpkg"
    label "single"
    input:
        tuple val(stage), val(sample), path(multiqc_data)
        val(single_end)
    output:
        tuple val(sample), path("${sample}_qc_basic_stats_${stage}.tsv.gz"), path("${sample}_qc_adapter_stats_${stage}.tsv.gz"), path("${sample}_qc_quality_base_stats_${stage}.tsv.gz"), path("${sample}_qc_quality_sequence_stats_${stage}.tsv.gz"), path("${sample}_qc_length_stats_${stage}.tsv.gz")
    shell:
        '''
        summarize-multiqc.R -i !{multiqc_data} -s !{stage} -S !{sample} -r !{single_end} -o ${PWD} -p !{sample}
        '''
}
