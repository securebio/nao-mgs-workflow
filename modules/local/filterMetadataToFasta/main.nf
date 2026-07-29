// Filter genome metadata down to the sequences present in the genome FASTA.
// Runs on the final, published FASTA rather than re-deriving which sequences
// were removed, so the filter cannot drift from the steps that removed them and
// automatically covers any future step that drops sequences.
process FILTER_METADATA_TO_FASTA {
    label "python"
    label "single"
    tag "id=index"
    input:
        path(genome_fasta)
        // Staged under a different name because PREPARE_VIRAL_METADATA emits it
        // under exactly this process's output name, which would collide. The
        // staged name fixes the suffix, so the input must be gzipped.
        path(genome_metadata, stageAs: "input_metadata.tsv.gz")
        val(name_pattern)
    output:
        path("${name_pattern}-metadata-gid.tsv.gz")
    script:
        """
        filter_metadata_to_fasta.py \\
            input_metadata.tsv.gz \\
            ${genome_fasta} \\
            ${name_pattern}-metadata-gid.tsv.gz
        """
}
