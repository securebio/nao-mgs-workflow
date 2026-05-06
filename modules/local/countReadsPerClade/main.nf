// Count reads per clade using taxonomic LCA assignments
// Note that if any reads are assigned to taxids not in the taxonomy, these reads will not be counted
process COUNT_READS_PER_CLADE {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
    // (sample name, read table tsv)
    // read table must include columns: seq_id, prim_align_dup_exemplar, aligner_taxid_lca, group
    // may include other columns
    // `group` column of read table must equal `sample` at every row
    tuple val(sample), path(reads_tsv)
    // taxonomy database tsv
    // must include columns: taxid, parent_taxid. may include other columns
    path taxdb

    output:
    // output gzipped tsv with columns:
    // group, taxid, parent_taxid, reads_direct_total, reads_direct_dedup, reads_clade_total, reads_clade_dedup
    tuple val(sample), path("${sample}_clade_counts.tsv.gz"), emit: output
    tuple val(sample), path("input_${reads_tsv}"), emit: input

    script:
    """
    count_reads_per_clade.py --reads ${reads_tsv} --taxdb ${taxdb} --group ${sample} --output ${sample}_clade_counts.tsv.gz
    # Link input files for testing
    ln -s ${reads_tsv} input_${reads_tsv}
    """
}
