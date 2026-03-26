// Sort a TSV file by a specified column header
// Uses Python script to handle empty files properly
process SORT_TSV {
    label "python"
    label "single_cpu_16GB_memory"
    input:
        tuple val(sample), path(tsv)
        val(sort_field)
    output:
        tuple val(sample), path("sorted_${sort_field}_${tsv}"), emit: sorted
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        # Run the Python script to sort the TSV
        sort_tsv.py -m ${task.memory.toGiga()} ${tsv} ${sort_field} sorted_${sort_field}_${tsv}

        # Link input to output for testing
        ln -s ${tsv} input_${tsv}
        """
}
