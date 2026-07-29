// Concatenate the per-chunk combined genome FASTAs emitted by
// DOWNLOAD_VIRAL_GENOMES into a single FASTA. Deduplication happens
// downstream in DEDUP_GENOME_FASTA, after pattern-based exclusion.
// Uses a local scratch directory on Batch profiles as defined in configs/profiles.config.
process CONCATENATE_GENOME_FASTA {
    label "single"
    label "coreutils_gzip_gawk"
    label "use_scratch"
    tag "id=index"
    input:
        path(genome_fastas)
    output:
        path("genomes.fasta.gz")
    script:
        """
        set -euo pipefail
        # Write sorted matching filenames to file for deterministic concatenation
        find . -maxdepth 1 -name '*.fna.gz' | sort > genome_files.txt
        if [[ ! -s genome_files.txt ]]; then
            echo "No genome FASTA files found!"
            exit 1
        fi
        echo "Concatenating \$(wc -l < genome_files.txt) combined genome FASTA file(s):"
        head genome_files.txt
        # Concatenate in sorted filename order so the record order that decides
        # first-occurrence dedup downstream is deterministic across runs.
        # Concatenated gzip members are themselves a valid gzip stream.
        xargs -d '\\n' -a genome_files.txt cat > genomes.fasta.gz
        echo "Output file contains" \$(zcat genomes.fasta.gz | grep -c '^>') "sequences."
        """
}
