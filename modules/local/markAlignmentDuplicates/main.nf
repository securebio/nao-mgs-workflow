// Tool source: rust-tools/mark_duplicates/
process MARK_ALIGNMENT_DUPLICATES {
    label "large"
    label "rust_tools"
    input:
        tuple val(sample), path(tsv)
        val(fuzzy_match)
    output:
        tuple val(sample), path("${sample}_duplicate_reads.tsv.gz"), path("${sample}_duplicate_stats.tsv.gz"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    shell:
    '''
    mark_duplicates -i "!{tsv}" \\
        -o "!{sample}_duplicate_reads.tsv.gz" \\
        -m "!{sample}_duplicate_stats.tsv.gz" \\
        -d !{fuzzy_match} \\
        -n !{task.cpus}
    ln -s !{tsv} input_!{tsv} # Link output to input for testing
    '''
}
