// Download entire viral Genbank DB
process DOWNLOAD_VIRAL_NCBI {
    label "ncbi_genome_download"
    label "xsmall"
    input:
        val(ncbi_viral_params)
    output:
        path("ncbi_metadata.txt"), emit: metadata
        path("ncbi_genomes"), emit: genomes
    shell:
        '''
        par="--formats fasta --flat-output --verbose --parallel !{task.cpus}"
        io="--output-folder ncbi_genomes --metadata-table ncbi_metadata.txt"
        ncbi-genome-download !{ncbi_viral_params} ${par} ${io} viral
        '''
}
