// Filter genomes to exclude specific patterns in sequence headers
process FILTER_GENOME_FASTA {
    label "seqkit"
    label "single"
    tag "id=index"
    input:
        path(collated_genomes)
        path(patterns_exclude)
        val(name_pattern)
    output:
        path("${name_pattern}.fasta.gz")
    script:
        """
        set -euo pipefail
        # `|| true` on both greps because grep exits 1 on no matches, which the
        # guards report with a message rather than aborting bare under `set -e`.
        zcat ${collated_genomes} | { grep "^>" || true; } > headers.txt
        if [[ ! -s headers.txt ]]; then
            echo "Input FASTA contains no sequence headers!"
            exit 1
        fi
        { grep -vif ${patterns_exclude} headers.txt || true; } | sed 's/^>//' > names.txt
        if [[ ! -s names.txt ]]; then
            echo "Every sequence header matched a pattern in ${patterns_exclude}!"
            exit 1
        fi
        # `seqkit grep -n` matches the full header. Selecting by ID instead
        # (e.g. `seqtk subseq`) would re-admit an excluded record whenever a
        # retained record shares its ID, since the two differ only in the
        # description the exclusion patterns match on.
        seqkit grep -n -f names.txt --threads ${task.cpus} \\
            -o ${name_pattern}.fasta.gz ${collated_genomes}
        """
}
