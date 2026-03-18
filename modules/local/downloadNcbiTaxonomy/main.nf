// Download NCBI taxonomy files
process DOWNLOAD_NCBI_TAXONOMY {
    label "BBTools"
    label "single"
    input:
        val(taxonomy_url)
    output:
        path("taxonomy.zip")
    script:
        def path = "taxonomy.zip"
        """
        wget ${taxonomy_url} -O ${path}
        """
}
