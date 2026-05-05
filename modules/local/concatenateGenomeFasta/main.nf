// Concatenate downloaded genomes from ncbi-genome-download according to a file of genome IDs
process CONCATENATE_GENOME_FASTA {
    label "single"
    label "seqkit"
    input:
        path(genome_dir)
        path(path_file)
    output:
        path("genomes.fasta.gz")
    script:
        """
        # Diagnostics
        echo "Genome directory contains" \$(ls ${genome_dir} | wc -l) "files, beginning with:"
        ls -1 ${genome_dir} | head
        echo "Filepath file contains" \$(cat ${path_file} | wc -l) "paths, beginning with:"
        head ${path_file}
        # Concatenate files listed by paths, then deduplicate by sequence ID.
        # Belt-and-suspenders against duplicate accessions reaching the final
        # FASTA: `bowtie2-build` accepts duplicate `>name` records but
        # `samtools view` rejects the resulting duplicate `@SQ` headers
        # with `[E::sam_hrecs_update_hashes] Duplicate entry`. Upstream
        # filtering (FILTER_VIRAL_GENBANK_METADATA drops superseded
        # assemblies) handles the common case; this guards against any
        # remaining sources of duplicate names. seqkit rmdup auto-detects
        # gzip on stdin and logs the duplicate count to stderr.
        if [[ ! -s ${path_file} ]]; then
            echo "No matching files found!"
            exit 1
        fi
        # seqkit rmdup logs `[INFO] N duplicated records removed` to stderr;
        # `-D` records the offending IDs so we can see what got dropped.
        xargs cat < ${path_file} \\
            | seqkit rmdup --by-name --threads ${task.cpus} \\
                -D genomes-duplicates.tsv -o genomes.fasta.gz
        if [[ -s genomes-duplicates.tsv ]]; then
            echo "Duplicate sequence IDs removed:"
            cat genomes-duplicates.tsv
        fi
        """
}
