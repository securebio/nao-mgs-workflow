process MULTIQC_LABELED {
    label "single"
    label "MultiQC"
    input:
        val(stage_label)
        tuple val(sample), path("*")
    output:
        path("multiqc_report.html"), emit: report
        tuple val(stage_label), val(sample), path("multiqc_data"), emit: data
    script:
        """
        multiqc .
        """
}
