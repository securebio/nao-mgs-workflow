// Tool source: rust-tools/mark_duplicates_similarity/
process MARK_SIMILARITY_DUPLICATES {
    label "single"
    label "rust_tools"
    input:
        tuple val(sample), path(tsv)
    output:
        tuple val(sample), path("similarity-duplicate-marked_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
    """
    mark_duplicates_similarity -i "${tsv}" -o "similarity-duplicate-marked_${tsv}"
    ln -s "${tsv}" "input_${tsv}"
    """
}
