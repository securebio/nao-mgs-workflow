// Join post-hoc validation results onto a full viral hits TSV, appending a
// validation_status column recording whether each read was aligned, produced no
// alignment, or was not selected for validation. No result is propagated between reads.
process ANNOTATE_VALIDATION_STATUS {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(hits), path(validation), path(sampled)
        val(key_column) // Column to join on (e.g. "seq_id")
        val(status_column) // Name of the status column to append
    output:
        tuple val(sample), path("annotated_${hits}"), emit: output
        tuple val(sample), path("input_${hits}"), path("input_${validation}"), path("input_${sampled}"), emit: input
    script:
        """
        annotate_validation_status.py -i ${hits} -v ${validation} -s ${sampled} \\
            -o annotated_${hits} -k ${key_column} -c ${status_column}
        # Link input files to output for testing
        ln -s ${hits} input_${hits}
        ln -s ${validation} input_${validation}
        ln -s ${sampled} input_${sampled}
        """
}
