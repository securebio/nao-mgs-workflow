// Generic process to filter TSV files by column value, keeping rows that match specified criteria.
// Takes column name, value, and keep_matching boolean as parameters, making it reusable across different filtering scenarios.
process FILTER_TSV_COLUMN_BY_VALUE {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(input_tsv)
        val(column_name)
        val(filter_value)
        val(keep_matching)
    output:
        tuple val(sample), path("filter_${column_name}_${filter_value}_${input_tsv}"), emit: output
        tuple val(sample), path("input_${column_name}_${filter_value}_${input_tsv}"), emit: input
    script:
        keep_flag = keep_matching ? "--keep-matching" : "--no-keep-matching"
        """
        # Set strict error handling
        set -o pipefail
        out=filter_${column_name}_${filter_value}_${input_tsv}
        # Filter the TSV to keep only rows where column matches value
        filter_tsv_column_by_value.py -i ${input_tsv} -c "${column_name}" -v "${filter_value}" ${keep_flag} -o \${out}
        # Link input to output for testing
        ln -s ${input_tsv} input_${column_name}_${filter_value}_${input_tsv}
        """
}
