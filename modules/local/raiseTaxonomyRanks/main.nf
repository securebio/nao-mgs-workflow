// Annotate a taxonomic DB with taxids of higher ranks in the taxonomy tree (e.g. species, genus, family)
process RAISE_TAXONOMY_RANKS {
    label "single"
    label "pandas"
    input:
        path(taxonomy_db)
        val(target_ranks)
    output:
        path("taxonomy-db-raised.tsv.gz"), emit: db
    shell:
        '''
        raise_taxonomy_ranks.py !{taxonomy_db} "!{target_ranks}" taxonomy-db-raised.tsv.gz
        '''
}
