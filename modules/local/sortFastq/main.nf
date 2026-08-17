// Sort a gzipped or plaintext FASTQ file based on header sequences
// Sort buffer, spill directory and spill compression are configured by
// lib/SortUtils.groovy.
process SORT_FASTQ {
    label "coreutils"
    label "sort_resources"
    tag "id=${sample}"
    input:
        tuple val(sample), path(input_file) // Interleaved or single-end
    output:
        tuple val(sample), path("sorted_${input_file}"), emit: output
        tuple val(sample), path("input_${input_file}"), emit: input
    script:
        def extractCmd = input_file.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        def compressCmd = input_file.toString().endsWith(".gz") ? "pigz -p ${task.cpus}" : "cat"
        def sort_opts = SortUtils.options(task.cpus, task.memory)
        """
        set -euo pipefail
        ${SortUtils.prelude('"${TMPDIR:-/tmp}"')}
        ${extractCmd} ${input_file} | paste - - - - | sort -k1,1 ${sort_opts} | \\
            tr '\\t' '\\n' | ${compressCmd} > sorted_${input_file}
        # Link input to output for testing
        ln -s ${input_file} input_${input_file}
        """
}
