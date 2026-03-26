// Replace a string in a file while retaining channel structure
process REPLACE_STRING {
    label "single"
    label "coreutils"
    label "testing_only" // Process is currently only used for testing
    input:
        tuple val(label), path(file)
        val(outname)
        val(old_string)
        val(new_string)
    output:
        tuple val(label), path("${label}_${outname}"), emit: output
        tuple val(label), path("${label}_input_${file}"), emit: input
    script:
        """
        sed 's/${old_string}/${new_string}/g' ${file} > ${label}_${outname}
        ln -s ${file} ${label}_input_${file}
        """
}

// Replace a string in a compressed file while retaining channel structure
process REPLACE_STRING_IN_COMPRESSED_FILE {
    label "single"
    label "coreutils"
    label "testing_only" // Process is currently only used for testing
    input:
        tuple val(label), path(file)
        val(outname)
        val(old_string)
        val(new_string)
    output:
        tuple val(label), path("${label}_${outname}"), emit: output
        tuple val(label), path("${label}_input_${file}"), emit: input
    script:
        """
        zcat ${file} | sed 's/${old_string}/${new_string}/g' | gzip > ${label}_${outname}
        ln -s ${file} ${label}_input_${file}
        """
}
