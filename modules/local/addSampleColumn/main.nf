// Add a column to a TSV with sample ID
process ADD_SAMPLE_COLUMN {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(tsv)
        val(sample_column)
        val(label)
    output:
        tuple val(sample), path("labeled_${label}_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        add_sample_column.py ${tsv} ${sample} ${sample_column} labeled_${label}_${tsv}
        # Link input files for testing
        ln -s ${tsv} input_${tsv}
        """
}
