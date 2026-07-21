// Filter viral genome metadata (from ENUMERATE_VIRAL_ACCESSIONS) by host
// infection status and assembly status, then chunk the kept accessions into
// fixed-size files for parallel download fan-out.
process FILTER_VIRAL_GENBANK_METADATA {
    label "single"
    label "pandas"
    tag "id=index"
    input:
        path(metadata_db)
        path(virus_db)
        val(host_taxa)
        val(chunk_size)
        val(name_pattern)
    output:
        // Uncompressed so the subworkflow can merge the per-branch filtered
        // metadata with `collectFile(keepHeader:...)` before PREPARE_VIRAL_METADATA
        // (which reads either form). Intermediate only; not a published output.
        path("${name_pattern}-metadata-filtered.tsv"), emit: db
        path("${name_pattern}-accession-chunks/chunk_*.txt"), emit: accession_chunks
    script:
        """
        filter_viral_genbank_metadata.py \\
            ${metadata_db} \\
            ${virus_db} \\
            "${host_taxa}" \\
            ${name_pattern}-metadata-filtered.tsv \\
            ${name_pattern}-accession-chunks \\
            ${chunk_size}
        """
}
