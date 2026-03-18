/*
Given a sorted TSV file with an initial header line, check that
every line in the file has a unique value of the specified column.
If not, throw an error.
*/

process CHECK_TSV_DUPLICATES {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv) // Input TSV
        val(field) // Field to check for duplicates
    output:
        tuple val(sample), path("checked_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        check_tsv_duplicates.py -i ${tsv} -f "${field}" -o checked_${tsv}
        # Link input to output for testing
        ln -s ${tsv} input_${tsv}
        """
}
