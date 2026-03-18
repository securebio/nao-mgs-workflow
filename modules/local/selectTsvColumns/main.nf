/*
Given a TSV column with an initial header line,
return a new TSV containing either:
1. Only the specified columns, with all others dropped ("keep" mode)
2. All columns except the specified ones, with only the latter dropped ("drop" mode)
*/

process SELECT_TSV_COLUMNS {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv) // Input TSV
        val(fields) // Comma-separated list of fields to select
        val(mode) // "keep" or "drop"
    output:
        tuple val(sample), path("selected_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        select_tsv_columns.py -i ${tsv} -o selected_${tsv} -f "${fields}" -m ${mode}
        # Link input to output for testing
        ln -s ${tsv} input_${tsv}
        """
}
