// Extract MultiQC data into a more usable form (single/interleaved version)
process SUMMARIZE_MULTIQC {
    label "rpkg"
    label "single"
    input:
        tuple val(stage), val(sample), path(multiqc_data)
        val(single_end)
    output:
        tuple val(sample), path("${stage}_${sample}_qc_basic_stats.tsv.gz"), path("${stage}_${sample}_qc_adapter_stats.tsv.gz"), path("${stage}_${sample}_qc_quality_base_stats.tsv.gz"), path("${stage}_${sample}_qc_quality_sequence_stats.tsv.gz"), path("${stage}_${sample}_qc_length_stats.tsv.gz")
    shell:
        '''
        summarize-multiqc.R -i !{multiqc_data} -s !{stage} -S !{sample} -r !{single_end} -o ${PWD}
        '''
}
