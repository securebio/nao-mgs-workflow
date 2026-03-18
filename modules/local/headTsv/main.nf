// Add a header line to an unheaded TSV file
process HEAD_TSV {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv)
        val(headers)
        val(label)
    output:
        tuple val(sample), path("head_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        head_tsv.py ${tsv} ${headers} head_${tsv}
        # Link input to output for testing
        ln -s ${tsv} input_${tsv}
        """
}
