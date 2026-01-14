// Process tabular output from VSEARCH clustering into a unified output format
process PROCESS_VSEARCH_CLUSTER_OUTPUT_LIST {
    label "pandas"
    label "single"
    input:
        tuple val(sample), path(summaries)
        val n_clusters // Return representative IDs of the N largest clusters
        val output_prefix // Column name prefix for output DB
    output:
        tuple val(sample), path("${sample}_*_vsearch_tab.tsv.gz"), emit: output
        tuple val(sample), path("${sample}_*_vsearch_ids.txt"), emit: ids
        tuple val(sample), path("input_*"), emit: input
    script:
        def prefix_string = output_prefix == "" ? "" : "-p ${output_prefix}"
        """
        for summary in ${summaries}; do
            species=\$(basename \${summary} | grep -oP '${sample}_\\K\\d+(?=_)')
            if [ -z "\$species" ]; then
                >&2 echo "Error: Could not extract species from filename: \${summary}"
                exit 1
            fi
            out_db=${sample}_\${species}_vsearch_tab.tsv.gz
            out_id=${sample}_\${species}_vsearch_ids.txt
            par="-n ${n_clusters} ${prefix_string}"
            process_vsearch_cluster_output.py \${par} \${summary} \${out_db} \${out_id}
            ln -s \${summary} input_\${summary}
        done
        """
}
