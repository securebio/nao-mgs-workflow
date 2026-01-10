// Convert a single FASTQ file (interleaved or single-end) into FASTA format

process CONVERT_FASTQ_FASTA {
    label "single"
    label "seqtk"
    input:
        tuple val(sample), path(fastq)
    output:
        tuple val(sample), path("converted_${fastq}"), emit: output
        tuple val(sample), path("input_${fastq}"), emit: input
    script:
        def extractCmd = fastq.toString().endsWith(".gz") ? "zcat" : "cat"
        def compressCmd = fastq.toString().endsWith(".gz") ? "gzip" : "cat"
        """
        # Perform conversion
        ${extractCmd} ${fastq} | seqtk seq -a | ${compressCmd} > converted_${fastq}
        # Link input to output for testing
        ln -s ${fastq} input_${fastq}
        """
}

// Convert a single FASTQ file (interleaved or single-end) into FASTA format
// TODO: Expand to work on non-gzipped files

process CONVERT_FASTQ_FASTA_LIST {
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
        """
        for fastq in ${fastqs}; do
            species=\$(basename \${fastq} | grep -oP '${sample}_\\K\\d+(?=_)')
            if [ -z "\${species}" ]; then
                >&2 echo "Error: Could not extract species from filename: \${fastq}"
                exit 1
            fi
            output="converted_\${fastq}"
            # Perform conversion
            ${extractCmd} \${fastq} | seqtk seq -a | ${compressCmd} > \${output}
            # Link input to output for testing
            ln -s \${fastq} input_\${fastq}
        done
        """
}
