// Add a column to a TSV with sample ID
process ADD_SAMPLE_COLUMN {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv)
        val(sample_column)
        val(label)
    output:
        tuple val(sample), path("labeled_${label}_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    shell:
        '''
        add_sample_column.py !{tsv} !{sample} !{sample_column} labeled_!{label}_!{tsv}
        # Link input files for testing
        ln -s !{tsv} input_!{tsv}
        '''
}

// Add group_species column to list of TSVs, extracting species from filename
process ADD_SAMPLE_COLUMN_LIST {
    label "python"
    label "single"
    input:
        tuple val(group), path(tsvs)
        val(sample_column)
        val(label)
    output:
        tuple val(group), path("labeled_${label}_*"), emit: output
        tuple val(group), path("input_*"), emit: input
    shell:
        '''
        for tsv_file in !{tsvs}; do
            # Extract species taxid from filename
            # Pattern: {group}_{species}_vsearch_tab.tsv or {group}_{species}_vsearch_tab.tsv.gz
            species=$(echo "$tsv_file" | sed -n 's/^!{group}_\\(.*\\)_vsearch_tab\\.tsv\\(\\.gz\\)\\?$/\\1/p')

            if [ -z "$species" ]; then
                echo "Error: Could not extract species from filename: $tsv_file"
                exit 1
            fi

            # Create group_species label
            group_species="!{group}_${species}"

            # Add column
            add_sample_column.py "$tsv_file" "$group_species" "!{sample_column}" "labeled_!{label}_${tsv_file}"

            # Link input for testing
            ln -s "$tsv_file" "input_${tsv_file}"
        done
        '''
}
