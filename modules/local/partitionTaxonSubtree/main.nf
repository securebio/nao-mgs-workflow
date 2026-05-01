// Partition the taxonomic subtree rooted at a given taxid into a set of
// disjoint subtree roots, each with at most `max_size` descendant taxa.
// Used to fan out viral genome downloads at uniform shard size.
process PARTITION_TAXON_SUBTREE {
    label "python"
    label "single"
    input:
        path(nodes_dmp)
        val(root_taxid)
        val(max_size)
    output:
        path("segment_taxids.txt"), emit: taxids
    script:
        """
        partition_taxon_subtree.py ${nodes_dmp} ${root_taxid} segment_taxids.txt --max-size ${max_size}
        """
}
