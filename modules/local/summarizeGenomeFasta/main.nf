// Summarise a genome FASTA as one row per record: genome_id, seq_length and a
// digest of the canonicalised sequence. Lets downstream steps group records by
// sequence identity without carrying sequence bytes through a sort or a join.
process SUMMARIZE_GENOME_FASTA {
    label "python"
    label "single"
    tag "id=index"
    input:
        path(genome_fasta)
        val(name_pattern)
    output:
        path("${name_pattern}-sequence-summary.tsv.gz"), emit: summary
        path("input_${genome_fasta}"), emit: input
    script:
        """
        summarize_genome_fasta.py \\
            ${genome_fasta} \\
            ${name_pattern}-sequence-summary.tsv.gz
        # Link input to output for testing
        ln -s ${genome_fasta} input_${genome_fasta}
        """
}
