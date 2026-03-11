/*
Given a TSV with two taxid columns, compute the vertical taxonomic distance
(number of parent-child steps) between the two taxids for each row. Rows
for which the two taxids are the same are given a distance of 0; rows for which
one taxid is not an ancestor of the other are given a distance of NA.

The input map process_params should specify the following fields:
- taxid_field_1: Column header for first taxid field
- taxid_field_2: Column header for second taxid field
- distance_field_1: Column header for first new distance field
- distance_field_2: Column header for second new distance field
*/

process COMPUTE_TAXID_DISTANCE {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv) // Sorted TSV with two taxid columns
        val(process_params) // Map specifying input taxid fields and output distance fields
        path(nodes_db) // TSV containing taxonomic structure (mapping taxids to parent taxids)
    output:
        tuple val(sample), path("distance_${tsv}"), emit: output // Distance-computed TSV
        tuple val(sample), path("input_${tsv}"), emit: input // Input file for testing
    script:
        """
        # Set up and run Python script
        io="-i ${tsv} -o distance_${tsv} -n ${nodes_db}"
        par="-t1 ${process_params.taxid_field_1} -t2 ${process_params.taxid_field_2} -d1 ${process_params.distance_field_1} -d2 ${process_params.distance_field_2}"
        compute_taxid_distance.py \${io} \${par}
        # Link input file to output for testing
        ln -s ${tsv} input_${tsv}
        """
}