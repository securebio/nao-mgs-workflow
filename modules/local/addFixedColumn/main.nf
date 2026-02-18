// Add a column to a TSV with a specified name and value
// Input is a tuple of (sample, tsv_file) to preserve sample ID through the workflow
process ADD_FIXED_COLUMN {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv)
        val(column)
        val(value)
        val(label)
    output:
        tuple val(sample), path("labeled_${label}_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    shell:
        '''
        add_fixed_column.py !{tsv} !{column} !{value} labeled_!{label}_!{tsv}
        # Link input files for testing
        ln -s !{tsv} input_!{tsv}
        '''
}
