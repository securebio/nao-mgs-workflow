// Concatenate multiple TSVs (streamed version with Python)
process CONCATENATE_TSVS {
    label "python"
    label "single"
    input:
        path(tsvs)
        val(name)
    output:
        path("${name}.tsv.gz"), emit: output
        path("input_${tsvs[0]}"), emit: input
    shell:
        '''
        concatenate_tsvs.py -o !{name}.tsv.gz !{tsvs}
        ln -s !{tsvs[0]} input_!{tsvs[0]} # Link input to output for testing
        '''
}

// Labeled version
process CONCATENATE_TSVS_LABELED {
    label "python"
    label "single"
    input:
        tuple val(label), path(tsvs)
        val(name)
    output:
        tuple val(label), path("${label}_${name}.tsv.gz"), emit: output
        tuple val(label), path("${label}_input_${tsvs[0]}"), emit: input
    shell:
        '''
        concatenate_tsvs.py -o !{label}_!{name}.tsv.gz !{tsvs}
        ln -s !{tsvs[0]} !{label}_input_!{tsvs[0]} # Link input to output for testing
        '''
}
