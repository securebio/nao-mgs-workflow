// Extract sequence IDs from a FASTA or FASTQ file
process EXTRACT_FASTN_IDS {
    label "seqkit"
    label "single"
    label "testing_only" // Process is currently only used for testing
    input:
        tuple val(sample), path(fastn)
    output:
        tuple val(sample), path("${sample}_ids.txt"), emit: output
        tuple val(sample), path("input_${fastn}"), emit: input
    script:
        """
        seqkit seq -ni ${fastn} | sort -u > ${sample}_ids.txt
        ln -s ${fastn} input_${fastn}
        """
}
