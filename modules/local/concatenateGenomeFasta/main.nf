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
        # Diagnostics. Use `find` (not a glob) so the no-match case yields an
        # empty list instead of tripping errexit before the guard below.
        # Sorted filename order, held in a file rather than a shell variable so
        # `xargs -d '\\n'` consumes it verbatim — no word splitting or quote
        # interpretation on the staged filenames.
        find . -maxdepth 1 -name '*.fna.gz' | sort > genome_files.txt
        if [[ ! -s genome_files.txt ]]; then
            echo "No genome FASTA files found!"
            exit 1
        fi
        echo "Concatenating \$(wc -l < genome_files.txt) combined genome FASTA file(s):"
        head genome_files.txt
        # Concatenate in sorted filename order so `seqkit rmdup --by-name`
        # first-occurrence behaviour is deterministic across runs.
        xargs -d '\\n' -a genome_files.txt cat \\
            | seqkit rmdup --by-name --threads ${task.cpus} \\
                -D genomes-duplicates.tsv -o genomes.fasta.gz
        if [[ -s genomes-duplicates.tsv ]]; then
            echo "Duplicate sequence IDs removed:"
            cat genomes-duplicates.tsv
        fi
        echo "Output file contains" \$(zcat genomes.fasta.gz | grep -c '^>') "sequences."
        """
}
