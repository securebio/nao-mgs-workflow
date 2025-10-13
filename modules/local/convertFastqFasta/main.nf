// Convert a single FASTQ file (interleaved or single-end) into FASTA format
// TODO: Expand to work on non-gzipped files

process CONVERT_FASTQ_FASTA {
    label "single"
    label "seqtk"
    input:
        tuple val(sample), path(fastq)
    output:
        tuple val(sample), path("${sample}_converted.fasta.gz"), emit: output
        tuple val(sample), path("${sample}_in.fastq.gz"), emit: input
    shell:
        '''
        # Perform conversion
        zcat !{fastq} | seqtk seq -a | gzip -c > !{sample}_converted.fasta.gz
        # Link input to output for testing
        ln -s !{fastq} !{sample}_in.fastq.gz
        '''
}

// Convert a single FASTQ file (interleaved or single-end) into FASTA format
// TODO: Expand to work on non-gzipped files

process CONVERT_FASTQ_FASTA_LIST {
    label "single"
    label "seqtk"
    input:
        tuple val(sample), path(fastqs)
    output:
        tuple val(sample), path("${sample}_*_converted.fasta.gz"), emit: output
        tuple val(sample), path("${sample}_*_in.fastq.gz"), emit: input
    shell:
        '''
        for fastq in downsampled_!{sample}_*_joined.fastq.gz; do
            species=$(basename ${fastq} | grep -oP '!{sample}_\\K\\d+(?=_)')
            output=!{sample}_${species}_converted.fasta.gz
            # Perform conversion
            zcat ${fastq} | seqtk seq -a | gzip -c > ${output}
            # Link input to output for testing
            ln -s ${fastq} !{sample}_${species}_in.fastq.gz
        done
        '''
}
