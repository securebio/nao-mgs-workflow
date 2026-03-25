// Filter viral genome data from ncbi-genome-download based on infection status
process FILTER_VIRAL_GENBANK_METADATA {
    label "single"
    label "pandas"
    input:
        path(metadata_db)
        path(virus_db)
        val(host_taxa)
        val(name_pattern)
    output:
        path("${name_pattern}-metadata-filtered.tsv.gz"), emit: db
        path("${name_pattern}-accessions.csv"), emit: accession
        path("${name_pattern}-paths.csv"), emit: path
    script:
        """
        filter-viral-genbank-metadata.py ${metadata_db} ${virus_db} "${host_taxa}" ${name_pattern}-metadata-filtered.tsv.gz ${name_pattern}-accessions.csv ${name_pattern}-paths.csv
        """
}
