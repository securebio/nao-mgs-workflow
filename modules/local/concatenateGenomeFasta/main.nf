// Concatenate downloaded genomes from ncbi-genome-download according to a file of genome IDs.
// Uses a local scratch directory on Batch profiles as defined in configs/profiles.config.
process CONCATENATE_GENOME_FASTA {
    label "xsmall"
    label "seqkit"
    label "use_scratch"
    input:
        path(genome_dir)
        path(path_file)
    output:
        path("genomes.fasta.gz")
    script:
        """
        set -euo pipefail
        # Diagnostics
        echo "Genome directory contains" \$(ls ${genome_dir} | wc -l) "files, beginning with:"
        # `|| true` prevents SIGPIPE in cases where directory size exceeds kernel pipe buffer
        ls -1 ${genome_dir} | head || true
        if [[ ! -s ${path_file} ]]; then
            echo "No matching files found!"
            exit 1
        fi
        echo "Filepath file contains" \$(cat ${path_file} | wc -l) "paths, beginning with:"
        head ${path_file}
        # `-P 4*cpus` because fetches are I/O-bound (sleeping on socket reads).
        mkdir -p staged
        xargs -P \$(( ${task.cpus} * 4 )) -n 100 -a ${path_file} cp -t staged/
        # Cat in path-file order (not `find staged` filesystem order) so
        # `seqkit rmdup --by-name` first-occurrence behavior is deterministic.
        awk -F/ '{print "staged/" \$NF}' ${path_file} \\
            | xargs cat \\
            | seqkit rmdup --by-name --threads ${task.cpus} \\
                -D genomes-duplicates.tsv -o genomes.fasta.gz
        rm -rf staged
        if [[ -s genomes-duplicates.tsv ]]; then
            echo "Duplicate sequence IDs removed:"
            cat genomes-duplicates.tsv
        fi
        echo "Output file contains" \$(zcat genomes.fasta.gz | grep -c '^>') "sequences."
        """
}
