/*
* Add a new TSV column where values are conditionally selected from two source columns.
* Example: Create column D where D = column B when column A matches a value, otherwise D = column C.
*/
process ADD_CONDITIONAL_TSV_COLUMN {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv)
        val(params_map)
    output:
        tuple val(sample), path("added_${params_map.new_hdr}_${tsv}"), emit: tsv
    script:
        def outputFile = "added_${params_map.new_hdr}_${tsv}"
        """
        add_conditional_tsv_column.py \
            --input ${tsv} \
            --chk-col "${params_map.chk_col}" \
            --match-val "${params_map.match_val}" \
            --if-col "${params_map.if_col}" \
            --else-col "${params_map.else_col}" \
            --new-hdr "${params_map.new_hdr}" \
            --output ${outputFile}
        """
}
