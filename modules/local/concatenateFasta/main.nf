// Concatenate gzipped FASTA files
process CONCATENATE_FASTA_GZIPPED {
    label "single"
    label "coreutils"
    tag "id=index"
    input:
        path(files)
        val(name)
    output:
        path("${name}.fasta.gz")
    script:
        """
        cat ${files} > ${name}.fasta.gz
        """
}

