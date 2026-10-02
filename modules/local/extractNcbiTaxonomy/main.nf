// Extract NCBI taxonomy archive and access its nodes.dmp, names.dmp, and merged.dmp files
process EXTRACT_NCBI_TAXONOMY {
    label "unzip"
    label "single"
    tag "id=index"
    input:
        path(taxonomy_zip)
    output:
        path("taxonomy"), emit: dir
        path("taxonomy-nodes.dmp"), emit: nodes
        path("taxonomy-names.dmp"), emit: names
        path("taxonomy-merged.dmp"), emit: merged
    script:
        """
        unzip ${taxonomy_zip} -d taxonomy
        cp taxonomy/nodes.dmp taxonomy-nodes.dmp
        cp taxonomy/names.dmp taxonomy-names.dmp
        cp taxonomy/merged.dmp taxonomy-merged.dmp
        """
}
