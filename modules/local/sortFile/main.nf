// Sort a gzipped file by a user-specified key string
// TODO: Expand to handle plaintext files
// Sort buffer, spill directory and spill compression are configured by
// lib/SortUtils.groovy.
process SORT_FILE {
    label "coreutils"
    label "sort_resources"
    tag "id=${sample}"
    input:
        tuple val(sample), path(input_file)
        val(sort_string)
        val(file_suffix)
    output:
        tuple val(sample), path("${sample}_sorted.${file_suffix}.gz"), emit: output
        tuple val(sample), path("${sample}_in.${file_suffix}.gz"), emit: input
    script:
        def out = "${sample}_sorted.${file_suffix}.gz"
        def in_file = "${sample}_in.${file_suffix}.gz"
        def sort_opts = SortUtils.options(task.cpus, task.memory)
        """
        set -euo pipefail
        ${SortUtils.prelude('"${TMPDIR:-/tmp}"')}
        # Run command
        pigz -dc -p ${task.cpus} ${input_file} | sort ${sort_string} ${sort_opts} | pigz -p ${task.cpus} > ${out}
        # Link input to output for testing
        ln -s ${input_file} ${in_file}
        """
}
