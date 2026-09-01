// Reduce genome metadata joined onto the per-record sequence summary down to
// one published row per sequence in the genome DB.
process RECONCILE_GENOME_METADATA {
    label "python"
    label "single"
    tag "id=index"
    input:
        path(joined_metadata)
        val(name_pattern)
    output:
        path("${name_pattern}-metadata-gid.tsv.gz"), emit: metadata
    script:
        """
        reconcile_genome_metadata.py \\
            ${joined_metadata} \\
            ${name_pattern}-metadata-gid.tsv.gz
        """
}
