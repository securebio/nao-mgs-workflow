// Summarize high-level domain abundance directly from a Kraken2 report
process KRAKEN_DOMAIN_SUMMARY {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(report)
    output:
        tuple val(sample), path("${sample}.bracken.gz"), emit: output
        tuple val(sample), path("input_${report}"), emit: input
    script:
        """
        kraken_domain_summary.py ${report} ${sample}.bracken.gz
        # Link input for testing
        ln -s ${report} input_${report}
        """
}
