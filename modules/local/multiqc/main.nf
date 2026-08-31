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
        // Publish the 100 most frequent overrepresented sequences rather than
        // MultiQC's default 20. Rank by total occurrences: the default ranks by
        // number of reports containing the sequence, which is always 1 here.
        """
        multiqc --cl-config 'fastqc_config: { top_overrepresented_sequences: 100, top_overrepresented_sequences_by: total }' .
        """
}
