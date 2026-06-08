// Add genome ID information to Genbank metadata table.
// Uses a local scratch directory on Batch profiles as defined in configs/profiles.config.
process ADD_GENBANK_GENOME_IDS {
    label "biopython"
    label "xsmall"
    label "use_scratch"
    tag "id=index"
    input:
        path(genbank_metadata)
        path(genbank_genomes)
        val(filename_prefix)
    output:
        path("${filename_prefix}-metadata-gid.tsv.gz"), emit: output
        path("input_${genbank_metadata}"), emit: input
    script:
        """
        add_genbank_genome_ids.py \\
            ${genbank_metadata} \\
            ${filename_prefix}-metadata-gid.tsv.gz \\
            --parallelism \$(( ${task.cpus} * 4 ))
        # Link input file for testing
        ln -s ${genbank_metadata} input_${genbank_metadata}
        """
}
