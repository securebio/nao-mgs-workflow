// Drop rows whose taxid falls under an excluded taxonomic clade from a viral
// genome metadata TSV (from the sequence-sourcing branch of
// ENUMERATE_VIRAL_ACCESSIONS). Used to exclude influenza (Orthomyxoviridae,
// e.g. taxid 11308): NCBI keeps flu on grouped genome assemblies, so the
// sequence branch would otherwise re-add thousands of ungrouped flu segments.
process FILTER_SEQUENCE_TAXA {
    label "single"
    label "pandas"
    tag "id=index"
    input:
        path(metadata)
        path(nodes_dmp)
        val(exclude_taxid)
    output:
        path("virus-genome-metadata-seqfiltered.tsv.gz"), emit: metadata
    script:
        """
        filter_sequence_taxa.py \\
            ${metadata} \\
            ${nodes_dmp} \\
            ${exclude_taxid} \\
            virus-genome-metadata-seqfiltered.tsv.gz
        """
}
