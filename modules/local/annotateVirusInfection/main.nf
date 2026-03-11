// Annotate virus taxonomic DB with infection status for target host taxa
process ANNOTATE_VIRUS_INFECTION {
    label "single"
    label "pandas"
    input:
        path(virus_db)
        path(host_db)
        path(infection_db)
        path(nodes_db)
        val(hard_exclude_taxids)
    output:
        path("total-virus-db-annotated.tsv.gz"), emit: db
    script:
        """
        annotate_viral_hosts.py ${virus_db} ${host_db} ${infection_db} ${nodes_db} "${hard_exclude_taxids}" total-virus-db-annotated.tsv.gz
        """
}
