/*
Given a list of files, first check that they have matching extensions,
then concatenate them into a single file with the same extension.
*/

process CONCATENATE_FILES_BY_EXTENSION {
    label "single"
    label "coreutils"
    input:
        tuple val(label), path(files)
        val(name)
    output:
        tuple val(label), path("${label}_${name}.*"), emit: output
        tuple val(label), path("${label}_input_${files[0]}"), emit: input
    script:
        // Extract extension from first file, handling double extensions like .fasta.gz
        def first_file = files[0].toString()
        if (!first_file.contains('.')) {
            throw new Exception("Input file ${first_file} has no extension")
        }
        def extension = first_file.substring(first_file.indexOf('.') + 1)
        // Check that all files end with the identified extension
        def check_str = ".${extension}"
        def invalid_files = files.findAll { f -> !f.toString().endsWith(check_str) }
        if (invalid_files) {
            throw new Exception("Input files ${invalid_files} do not end with .${extension}")
        }
        """
        cat ${files} > ${label}_${name}.${extension}
        ln -s ${files[0]} ${label}_input_${files[0]} # Link input to output for testing
        """
}