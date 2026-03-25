// Rename fields in a TSV header
process REHEAD_TSV {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv)
        val(old_fields)
        val(new_fields)
    output:
        tuple val(sample), path("renamed_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        rehead_tsv.py ${tsv} ${old_fields} ${new_fields} renamed_${tsv}
        # Link input to output for testing
        ln -s ${tsv} input_${tsv}
        """
}
