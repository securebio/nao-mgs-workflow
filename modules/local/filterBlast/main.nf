// Process & filter BLAST output into TSV
// Streamed version (final filter only, earlier filtering handled by FILTER_TSV)
process FILTER_BLAST {
    label "python"
    label "single"
    input:
        tuple val(sample), path(blast_hits_sorted) // Must be sorted on query ID (ascending) and bitscore (descending)
        val(max_rank) // Maximum bitscore rank to keep
        val(min_frac) // Minimum bitscore to retain (as a fraction of the best bitscore for the query)
    output:
        tuple val(sample), path("${sample}_blast_filtered.tsv.gz"), emit: output
        tuple val(sample), path("${sample}_blast_in.tsv.gz"), emit: input
    script:
        """
        # Run script
        filter_blast.py -i ${blast_hits_sorted} -o ${sample}_blast_filtered.tsv.gz -r ${max_rank} -f ${min_frac}
        # Link input to output for testing
        ln -s ${blast_hits_sorted} ${sample}_blast_in.tsv.gz
        """
}
