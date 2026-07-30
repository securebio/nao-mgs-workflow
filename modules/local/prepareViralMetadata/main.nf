// Prepare viral genome metadata: join the filtered assembly metadata with the
// per-chunk accession -> genome_id map, add species_taxid, and expand to one
// row per (assembly_accession, genome_id).
process PREPARE_VIRAL_METADATA {
    label "python"
    label "single_cpu_16GB_memory"
    tag "id=index"
    input:
        path(merged_metadata)
        path(virus_db)
        path(accession_map)
        val(name_pattern)
    output:
        path("${name_pattern}-metadata-gid-unfiltered.tsv.gz"), emit: metadata
    script:
        """
        prepare_viral_metadata.py \\
            ${merged_metadata} \\
            ${virus_db} \\
            ${accession_map} \\
            ${name_pattern}-metadata-gid-unfiltered.tsv.gz
        """
}
