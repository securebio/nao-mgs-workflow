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
        // FastQC's "Top overrepresented sequences" table defaults to the 20 most
        // frequent sequences; raise it to 100, which is what QC consumers of
        // {sample}_qc_overrepresented_{stage}.tsv.gz ask for. Sequences below
        // FastQC's own 0.1%-of-reads threshold are never reported regardless.
        """
        multiqc --cl-config 'fastqc_config: { top_overrepresented_sequences: 100 }' .
        """
}
