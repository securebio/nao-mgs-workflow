// Deduplicate the viral genome DB.
// Runs after FILTER_GENOME_FASTA so that pattern-excluded records are already
// gone. That ordering is a no-op for `--by-name`, which only collapses records
// with byte-identical headers -- such records necessarily match the exclusion
// patterns identically, so filtering before or after dedup selects the same
// set. It matters for the stricter dedup that replaces this pass later:
// deduplicating first would let an excluded record win against a clean twin,
// and the filter would then delete the winner, dropping a genome for which a
// legitimate record existed.
// Uses a local scratch directory on Batch profiles as defined in configs/profiles.config.
process DEDUP_GENOME_FASTA {
    label "xsmall"
    label "seqkit"
    label "use_scratch"
    tag "id=index"
    input:
        path(filtered_genomes)
        val(name_pattern)
    output:
        path("${name_pattern}.fasta.gz")
    script:
        """
        set -euo pipefail
        # `--by-name` keys on the full header rather than the sequence ID, so
        # two records sharing an ID but differing in their description both
        # survive. That leaves the duplicate-reference failure of #758 only
        # partly closed, since bowtie2-build and samtools key on the ID alone.
        # Closing it is a behavioural change and is handled separately; this
        # step reproduces the previous behaviour in its new position.
        seqkit rmdup --by-name --threads ${task.cpus} \\
            -D duplicate-names.tsv -o ${name_pattern}.fasta.gz ${filtered_genomes}
        if [[ -s duplicate-names.tsv ]]; then
            echo "Duplicate sequence names removed:"
            cat duplicate-names.tsv
        fi
        echo "Output file contains" \$(zcat ${name_pattern}.fasta.gz | grep -c '^>') "sequences."
        """
}
