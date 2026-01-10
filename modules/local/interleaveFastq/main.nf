// Interleave paired FASTQ files into a single interleaved file

process INTERLEAVE_FASTQ {
    label "single"
    label "coreutils_gzip_gawk"
    input:
        tuple val(sample), path(reads)
    output:
        tuple val(sample), path("${sample}_interleaved.*"), emit: output
        tuple val(sample), path("input_*"), emit: input
    script:
        def extractCmd = reads[0].toString().endsWith(".gz") ? "zcat" : "cat"
        def compressCmd = reads[0].toString().endsWith(".gz") ? "gzip" : "cat"
        def in1 = reads[0]
        def in2 = reads[1]
        def out_suffix = reads[0].toString().endsWith(".gz") ? "fastq.gz" : "fastq"
        def out = "${sample}_interleaved.${out_suffix}"
        """
        # Perform interleaving
        paste <(${extractCmd} ${in1} | paste - - - - ) <(${extractCmd} ${in2} | paste - - - - ) | tr "\t" "\n" | ${compressCmd} > ${out}
        # Link input to output for testing
        ln -s ${in1} input_${in1}
        ln -s ${in2} input_${in2}
        """
}
