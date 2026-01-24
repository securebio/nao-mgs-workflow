// Partition a TSV into multiple output TSVs based on a group column
process PARTITION_TSV {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv)
        val(column)
    output:
        tuple val(sample), path("partition_*_${tsv}"), emit: output, optional: true
        tuple val(sample), path("input_${tsv}"), emit: input
    shell:
        '''
        partition_tsv.py -i !{tsv} -c !{column}
        ln -s !{tsv} input_!{tsv} # Link input to output for testing
        '''
}
