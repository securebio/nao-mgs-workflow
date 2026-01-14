// Sort a gzipped or plaintext FASTQ file based on header sequences
process SORT_FASTQ {
    label "coreutils"
    label "single"
    input:
        tuple val(sample), path(fastq) // Interleaved or single-end
    output:
        tuple val(sample), path("sorted_${fastq}"), emit: output
        tuple val(sample), path("input_${fastq}"), emit: input
    script:
        def extractCmd = fastq.toString().endsWith(".gz") ? "zcat" : "cat"
        def compressCmd = fastq.toString().endsWith(".gz") ? "gzip" : "cat"
        """
        set -euo pipefail
        ${extractCmd} ${fastq} | paste - - - - | sort -k1,1 | \\
            tr '\\t' '\\n' | ${compressCmd} > sorted_${fastq}
        # Link input to output for testing
        ln -s ${fastq} input_${fastq}
        """
}
