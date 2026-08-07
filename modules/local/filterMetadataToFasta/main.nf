// Filter genome metadata down to the sequences present in the genome FASTA.
process FILTER_METADATA_TO_FASTA {
    label "python"
    label "single"
    tag "id=index"
    input:
        path(genome_metadata)
        path(genome_fasta)
        val(name_pattern)
    output:
        path("${name_pattern}-metadata-gid.tsv.gz")
    script:
        """
        filter_metadata_to_fasta.py \\
            ${genome_metadata} \\
            ${genome_fasta} \\
            ${name_pattern}-metadata-gid.tsv.gz
        """
}
