// Partition a TSV into multiple output TSVs based on a group column
process PARTITION_TSV {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(tsv)
        val(column)
    output:
        // Header-only input produces no partitions: emit an empty list explicitly to disambiguate
        // from a failed process with an empty channel.
        tuple val(sample), path("partition_*_${tsv}", arity: "0..*"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        partition_tsv.py -i ${tsv} -c ${column}
        ln -s ${tsv} input_${tsv} # Link input to output for testing
        """
}
