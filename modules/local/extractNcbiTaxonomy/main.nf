// Extract NCBI taxonomy archive and access nodes and names files
process EXTRACT_NCBI_TAXONOMY {
    label "unzip"
    label "single"
    input:
        path(taxonomy_zip)
    output:
        path("taxonomy"), emit: dir
        path("taxonomy-nodes.dmp"), emit: nodes
        path("taxonomy-names.dmp"), emit: names
    script:
        """
        unzip ${taxonomy_zip} -d taxonomy
        cp taxonomy/nodes.dmp taxonomy-nodes.dmp
        cp taxonomy/names.dmp taxonomy-names.dmp
        """
}
