// Concatenate the per-chunk combined genome FASTAs emitted by
// DOWNLOAD_VIRAL_GENOMES into a single deduplicated FASTA.
// Uses a local scratch directory on Batch profiles as defined in configs/profiles.config.
process CONCATENATE_GENOME_FASTA {
    label "xsmall"
    label "seqkit"
    label "use_scratch"
    tag "id=index"
    input:
        path(genome_fastas)
    output:
        path("genomes.fasta.gz")
    script:
        """
        set -euo pipefail
        # Diagnostics
        n=\$(ls -1 *.fna.gz 2>/dev/null | wc -l)
        echo "Concatenating \$n combined genome FASTA file(s):"
        ls -1 *.fna.gz 2>/dev/null | head || true
        if [ "\$n" -eq 0 ]; then
            echo "No genome FASTA files found!"
            exit 1
        fi
        # Concatenate in sorted filename order so `seqkit rmdup --by-name`
        # first-occurrence behaviour is deterministic across runs (`ls -1`
        # already sorts lexicographically).
        ls -1 *.fna.gz \\
            | xargs cat \\
            | seqkit rmdup --by-name --threads ${task.cpus} \\
                -D genomes-duplicates.tsv -o genomes.fasta.gz
        if [[ -s genomes-duplicates.tsv ]]; then
            echo "Duplicate sequence IDs removed:"
            cat genomes-duplicates.tsv
        fi
        echo "Output file contains" \$(zcat genomes.fasta.gz | grep -c '^>') "sequences."
        """
}
