// Filter genome metadata down to one row per sequence in the genome FASTA.
// The input is PREPARE_VIRAL_METADATA's `-metadata-gid-unfiltered` file; the
// output name must stay distinct from it or this process overwrites its input.
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
