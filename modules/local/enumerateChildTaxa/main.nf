// Enumerate direct child taxa of a given parent taxid from NCBI taxonomy nodes.dmp
process ENUMERATE_CHILD_TAXA {
    label "python"
    label "single"
    input:
        path(nodes_dmp)
        val(parent_taxid)
    output:
        path("child_taxids.txt"), emit: taxids
    shell:
        '''
        enumerate_child_taxa.py !{nodes_dmp} !{parent_taxid} child_taxids.txt
        '''
}
