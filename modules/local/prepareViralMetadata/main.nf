// Prepare viral genome metadata: join the filtered assembly metadata with the
// per-chunk accession -> genome_id map, add species_taxid, and expand to one
// row per (assembly_accession, genome_id). Replaces the previous
// PREPARE + ADD_GENBANK_GENOME_IDS pair: the genome_id linkage now comes from
// the map emitted by DOWNLOAD_VIRAL_GENOMES rather than from re-reading each
// downloaded genome file.
process PREPARE_VIRAL_METADATA {
    label "python"
    label "single_cpu_16GB_memory"
    tag "id=index"
    input:
        path(merged_metadata)
        path(virus_db)
        path(accession_map)
    output:
        path("virus-genome-metadata-gid.tsv.gz"), emit: metadata
    script:
        """
        prepare_viral_metadata.py \\
            ${merged_metadata} \\
            ${virus_db} \\
            ${accession_map} \\
            virus-genome-metadata-gid.tsv.gz
        """
}
