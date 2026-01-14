// Concatenate gzipped FASTA files
process CONCATENATE_FASTA_GZIPPED {
    label "single"
    label "coreutils"
    input:
        path(files)
        val(name)
    output:
        path("${name}.fasta.gz")
    shell:
        '''
        cat !{files} > !{name}.fasta.gz
        '''
}

