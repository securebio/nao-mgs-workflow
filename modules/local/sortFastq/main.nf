// Sort a gzipped or plaintext FASTQ file based on header sequences
// Sort buffer, spill directory and spill compression are configured by
// SortUtils; see lib/SortUtils.groovy for why an explicit buffer size is
// required when sort reads from a pipe.
process SORT_FASTQ {
    label "coreutils"
    label "sort_fastq_resources"
    tag "id=${sample}"
    input:
        tuple val(sample), path(fastq) // Interleaved or single-end
    output:
        tuple val(sample), path("sorted_${fastq}"), emit: output
        tuple val(sample), path("input_${fastq}"), emit: input
    script:
        def extractCmd = fastq.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        def compressCmd = fastq.toString().endsWith(".gz") ? "pigz -p ${task.cpus}" : "cat"
        def sort_opts = SortUtils.options(task.cpus, task.memory)
        """
        set -euo pipefail
        ${SortUtils.prelude('"${TMPDIR:-/tmp}"')}
        ${extractCmd} ${fastq} | paste - - - - | sort -k1,1 ${sort_opts} | \\
            tr '\\t' '\\n' | ${compressCmd} > sorted_${fastq}
        # Link input to output for testing
        ln -s ${fastq} input_${fastq}
        """
}
