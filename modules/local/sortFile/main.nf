// Sort a gzipped file by a user-specified key string
// TODO: Expand to handle plaintext files
process SORT_FILE {
    label "coreutils"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(file)
        val(sort_string)
        val(file_suffix)
    output:
        tuple val(sample), path("${sample}_sorted.${file_suffix}.gz"), emit: output
        tuple val(sample), path("${sample}_in.${file_suffix}.gz"), emit: input
    script:
        def out = "${sample}_sorted.${file_suffix}.gz"
        def in_file = "${sample}_in.${file_suffix}.gz"
        """
        set -euo pipefail
        # Run command
        pigz -dc -p ${task.cpus} ${file} | sort ${sort_string} | pigz -p ${task.cpus} > ${out}
        # Link input to output for testing
        ln -s ${file} ${in_file}
        """
}
