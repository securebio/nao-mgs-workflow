// Naively concatenate multiple files end-to-end
process CONCATENATE_FILES {
    label "single"
    label "coreutils"
    input:
        path(files)
        val(name)
        val(ext)
    output:
        path("${name}.${ext}"), emit: output
        path("${name}_in_0.${ext}"), emit: input
    script:
        """
        cat ${files} > ${name}.${ext}
        ln -s ${files[0]} ${name}_in_0.${ext} # Link input to output for testing
        """
}
