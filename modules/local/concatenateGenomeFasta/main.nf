// Concatenate downloaded genomes from ncbi-genome-download according to a file of genome IDs
process CONCATENATE_GENOME_FASTA {
    label "single_cpu_16GB_memory"
    label "seqkit"
    input:
        path(genome_dir)
        path(path_file)
    output:
        path("genomes.fasta.gz")
    script:
        """
        set -euo pipefail
        # Diagnostics
        # `|| true` prevents SIGPIPE in cases where directory size exceeds kernel pipe buffer
        ls -1 ${genome_dir} | head || true
        if [[ ! -s ${path_file} ]]; then
            echo "No matching files found!"
            exit 1
        fi
        echo "Filepath file contains" \$(cat ${path_file} | wc -l) "paths, beginning with:"
        head ${path_file}
        # Concatenate files listed by paths, then deduplicate by sequence ID.
        # `bowtie2-build` accepts duplicate `>name` records but `samtools view`
        # rejects the resulting duplicate `@SQ` headers.
        # Upstream filtering in FILTER_VIRAL_GENBANK_METADATA to drop non-current
        # assemblies handles the common case; this guards against remaining duplicates.
        xargs cat < ${path_file} \\
            | seqkit rmdup --by-name --threads ${task.cpus} \\
                -D genomes-duplicates.tsv -o genomes.fasta.gz
        if [[ -s genomes-duplicates.tsv ]]; then
            echo "Duplicate sequence IDs removed:"
            cat genomes-duplicates.tsv
        fi
        echo "Output file contains" \$(zcat genomes.fasta.gz | grep -c '^>') "sequences."
        """
}
