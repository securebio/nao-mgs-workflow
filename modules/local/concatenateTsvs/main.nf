// Concatenate multiple TSVs (streamed version with Python)
process CONCATENATE_TSVS_LABELED {
    label "python"
    label "single"
    tag "id=${label}"
    input:
        tuple val(label), path(tsvs)
        val(name)
    output:
        tuple val(label), path("${label}_${name}.tsv.gz"), emit: output
        tuple val(label), path("${label}_input_${tsvs[0]}"), emit: input
    script:
        """
        concatenate_tsvs.py -o ${label}_${name}.tsv.gz ${tsvs}
        ln -s ${tsvs[0]} ${label}_input_${tsvs[0]} # Link input to output for testing
        """
}
