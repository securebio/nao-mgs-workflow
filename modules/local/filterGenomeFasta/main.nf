// Filter genomes to exclude specific patterns in sequence headers
process FILTER_GENOME_FASTA {
    label "seqtk"
    label "single"
    tag "id=index"
    input:
        path(collated_genomes)
        path(patterns_exclude)
        val(name_pattern)
    output:
        path("${name_pattern}.fasta.gz")
    script:
        """
        set -euo pipefail
        pigz -dc -p ${task.cpus} ${collated_genomes} | grep "^>" | grep -vif ${patterns_exclude} | sed 's/>//' > names.txt
        # `pigz -1`, not `gzip`: this is an intermediate consumed by
        # MASK_GENOME_FASTA, so compression time dominates and ratio does not.
        seqtk subseq ${collated_genomes} names.txt | pigz -1 -p ${task.cpus} > ${name_pattern}.fasta.gz
        """
}
