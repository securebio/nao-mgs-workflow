// Prepare viral genome metadata for downstream filtering and genome ID extraction
process PREPARE_VIRAL_METADATA {
    label "pandas"
    label "single"
    input:
        path(merged_metadata)
        path(virus_db)
        path(genome_files)
    output:
        path("ncbi_metadata.txt"), emit: metadata
        path("ncbi_genomes"), emit: genomes
    script:
        """
        # Move genome files into a staging directory to avoid mixing with other staged files
        mkdir -p input_genomes
        find . -maxdepth 1 -name '*.fna.gz' -exec mv {} input_genomes/ \\;

        prepare-viral-metadata.py \\
            ${merged_metadata} \\
            ${virus_db} \\
            input_genomes \\
            ncbi_metadata.txt \\
            ncbi_genomes
        """
}
