// Sort a gzipped file by a user-specified key string
// TODO: Expand to handle plaintext files
process SORT_FILE {
    label "coreutils"
    label "single"
    input:
        tuple val(sample), path(file)
        val(sort_string)
        val(file_suffix)
    output:
        tuple val(sample), path("${sample}_sorted.${file_suffix}.gz"), emit: output
        tuple val(sample), path("${sample}_in.${file_suffix}.gz"), emit: input
    script:
        """
        set -euo pipefail
        out=${sample}_sorted.${file_suffix}.gz
        in=${sample}_in.${file_suffix}.gz
        # Run command
        zcat ${file} | sort ${sort_string} | gzip > \${out}
        # Link input to output for testing
        ln -s ${file} \${in}
        """
}
