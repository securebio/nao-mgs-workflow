// Copy a file while retaining channel structure
process COPY_FILE {
    label "single"
    label "coreutils"
    input:
        tuple val(sample), path(file)
        val(outname)
    output:
        tuple val(sample), path("${sample}_${outname}")
    script:
        """
        cp ${file} ${sample}_${outname}
        """
}

// Copy a file without sample annotation
// Note that null operation (when file and outname are the same) is supported
// because it is useful for allowing workflow input and logging files to be published
process COPY_FILE_BARE {
    label "single"
    label "coreutils"
    input:
        path(file)
        val(outname)
    output:
        path("${outname}")
    script:
        """
        if [ "${file}" != "${outname}" ]; then
            cp ${file} ${outname}
        fi
        """
}
