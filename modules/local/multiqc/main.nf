process MULTIQC_LABELED {
    label "single"
    label "MultiQC"
    tag "id=${sample},stage=${stage_label}"
    input:
        val(stage_label)
        tuple val(sample), path("*")
    output:
        path("multiqc_report.html"), emit: report
        tuple val(stage_label), val(sample), path("multiqc_data"), emit: data
    script:
        // Publish the 100 most frequent overrepresented sequences, ranked by
        // total occurrences.
        """
        multiqc --cl-config 'fastqc_config: { top_overrepresented_sequences: 100, top_overrepresented_sequences_by: total }' .
        """
}
