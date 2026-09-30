// Partition a TSV into multiple output TSVs based on a group column
process PARTITION_TSV {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(tsv)
        val(column)
    output:
        // Header-only input is passed on as partition_header_only_<input>: a positive signal of an
        // empty group, unlike a failed process, which emits nothing. Keep this a single glob: Fusion
        // drops task files matching no output pattern, and can't match a wildcard inside braces.
        tuple val(sample), path("partition_*_${tsv}", arity: "1..*"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        partition_tsv.py -i ${tsv} -c ${column}
        ln -s ${tsv} input_${tsv} # Link input to output for testing
        """
}
