// Filter out the samples from the grouping file that have 0 viral hits
process VALIDATE_GROUPING {
    label "python"
    label "single"
    input:
        tuple val(label), path(input_file), path(groups_file)
    output:
        tuple val(label), path(input_file), path("validated_${groups_file}"), emit: output
        tuple val(label), path("partial_group_${label}.tsv"), emit: partial_group_log
        tuple val(label), path("empty_group_${label}.tsv"), emit: empty_group_log
        tuple val(label), path("input_${input_file}"), path("input_${groups_file}"), emit: input
    script:
        def out_validated = "validated_${groups_file}"
        def out_partial = "partial_group_${label}.tsv"
        def out_empty = "empty_group_${label}.tsv"
        """
        validate_grouping.py ${input_file} ${groups_file} ${out_validated} ${out_partial} ${out_empty}
        ln -s ${input_file} input_${input_file}
        ln -s ${groups_file} input_${groups_file}
        """
}
