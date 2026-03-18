// Filter genomes to exclude specific patterns in sequence headers
process FILTER_GENOME_FASTA {
    label "seqtk"
    label "single"
    input:
        path(collated_genomes)
        path(patterns_exclude)
        val(name_pattern)
    output:
        path("${name_pattern}.fasta.gz")
    script:
        """
        zcat ${collated_genomes} | grep "^>" | grep -vif ${patterns_exclude} | sed 's/>//' > names.txt
        seqtk subseq ${collated_genomes} names.txt | gzip -c > ${name_pattern}.fasta.gz
        """
}
