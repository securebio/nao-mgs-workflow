// Prepare viral genome metadata for downstream filtering and genome ID extraction
process PREPARE_VIRAL_METADATA {
    label "python"
    label "single_cpu_16GB_memory"
    input:
        path(merged_metadata)
        path(virus_db)
        path(genome_files)
    output:
        path("ncbi_metadata.txt"), emit: metadata
        path("ncbi_genomes"), emit: genomes
    script:
        """
        prepare_viral_metadata.py \\
            ${merged_metadata} \\
            ${virus_db} \\
            . \\
            ncbi_metadata.txt \\
            ncbi_genomes
        """
}
