// Download genome reference
process DOWNLOAD_GENOME {
    label "BBTools"
    label "single"
    input:
        tuple val(genome_url), val(name)
    output:
        path("${name}.fasta.gz"), emit: genome
    script:
        def path = "${name}.fasta"
        """
        if [[ "${genome_url}" == *.gz ]]; then
            wget "${genome_url}" -O ${path}.gz
        else
            wget "${genome_url}" -O ${path}
            gzip ${path}
        fi
        """
}
