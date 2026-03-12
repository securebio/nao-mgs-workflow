// Given a sorted TSV containing group, taxid, and score columns,
// return a TSV with a single row per group containing the lowest common ancestor
// taxid across all taxids in the group.
process LCA_TSV {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv) // Sorted TSV with group, taxid, and score columns
        path(nodes_db) // TSV containing taxonomic structure (mapping taxids to parent taxids)
        path(names_db) // TSV containing taxonomic names (mapping taxids to names)
        val(params_map) // Map containing LCA parameters: group_field, taxid_field, score_field, taxid_artificial, prefix
    output:
        tuple val(sample), path("lca_${tsv}"), emit: output // LCA-summarized TSV
        tuple val(sample), path("input_${tsv}"), path("input_${nodes_db}"), path("input_${names_db}"), emit: input // Input files for testing
    script:
        // Set up and run Python script
        def io = "-i ${tsv} -o lca_${tsv} -d ${nodes_db} -n ${names_db}"
        def par = "-g ${params_map.group_field} -t ${params_map.taxid_field} -s ${params_map.score_field} -a ${params_map.taxid_artificial}" + (params_map.prefix ? " -p ${params_map.prefix}" : "")
        """
        lca_tsv.py ${io} ${par}
        # Link input files to output for testing
        ln -s ${tsv} input_${tsv}
        ln -s ${nodes_db} input_${nodes_db}
        ln -s ${names_db} input_${names_db}
        """
}