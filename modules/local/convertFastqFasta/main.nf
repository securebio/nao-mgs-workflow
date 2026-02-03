// Convert FASTQ files (interleaved or single-end) into FASTA format

process CONVERT_FASTQ_FASTA {
    label "single"
    label "seqtk"
    input:
        tuple val(sample), path(fastqs)
    output:
        tuple val(sample), path("converted_*"), emit: output
        tuple val(sample), path("input_*"), emit: input
    script:
        // Extract extension from first file and check that all files have the same extension
        def first_file = fastqs[0].toString()
        if (!first_file.contains('.')) {
            throw new Exception("Input file ${first_file} has no extension")
        }
        def extension = first_file.substring(first_file.indexOf('.') + 1)
        def check_str = ".${extension}"
        def invalid_files = fastqs.findAll { !it.toString().endsWith(check_str) }
        if (invalid_files) {
            throw new Exception("Input files ${invalid_files} do not end with .${extension}")
        }
        def extractCmd = check_str.endsWith(".gz") ? "zcat" : "cat"
        def compressCmd = check_str.endsWith(".gz") ? "gzip" : "cat"
        def outputs = fastqs.collect { fastq -> "converted_${fastq}".replace(".fastq", ".fasta") }
        def inputs = fastqs.collect { fastq -> "input_${fastq}" }
        """
        fastqs_array=(${fastqs})
        outputs_array=(${outputs.join(" ")})
        inputs_array=(${inputs.join(" ")})
        for ((i=0; i<\${#fastqs_array[@]}; i++)); do
            fastq="\${fastqs_array[i]}"
            output="\${outputs_array[i]}"
            input="\${inputs_array[i]}"
            # Perform conversion
            ${extractCmd} \${fastq} | seqtk seq -a | ${compressCmd} > \${output}
            # Link input to output for testing
            ln -s \${fastq} \${input}
        done
        """
}
