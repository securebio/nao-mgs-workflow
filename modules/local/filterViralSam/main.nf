// Consolidated viral SAM filtering: keep contaminant-free reads, applies pair-based score threshold filtering, adds missing mates for UP reads, and sorts output
process FILTER_VIRAL_SAM {
    label "biopython"
    label "single"
    input:
        tuple val(sample), path(sam), path(contaminant_free_reads)
        val(score_threshold)
    output:
        tuple val(sample), path("${sample}_viral_filtered.sam.gz"), emit: sam
        tuple val(sample), path("input_${sam}"), emit: input
    script:
        """
        outf="${sample}_viral_filtered.sam.gz"

        # Run the consolidated viral SAM filtering
        filter_viral_sam.py ${sam} ${contaminant_free_reads} \${outf} ${score_threshold}
        # Link input to output for testing
        ln -s ${sam} input_${sam}
        """
}
