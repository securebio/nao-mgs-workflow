// Gzip a plaintext input file
process GZIP_FILE {
    label "single"
    label "coreutils"
    label "testing_only" // Process is currently only used for testing
    tag "id=${sample}"
    input:
        tuple val(sample), path(file)
    output:
        tuple val(sample), path("${file}.gz")
    script:
        """
        gzip -c ${file} > ${file}.gz
        """
}

// Gzip a file without sample annotation
process GZIP_FILE_BARE {
    label "single"
    label "coreutils"
    tag "id=util"
    input:
        path(file)
    output:
        path("${file}.gz")
    script:
        """
        gzip -c ${file} > ${file}.gz
        """
}
