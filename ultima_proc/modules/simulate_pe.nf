// Generate simulated paired-end reads from single-end Ultima data.
// R1 = original read, R2 = reverse complement of R1.
// Uses seqtk seq -r for streaming reverse complement generation.

process SIMULATE_PE {
    label "seqtk"
    label "single_cpu_16GB_memory"
    publishDir "${params.output_dir}", mode: 'copy'
    input:
        tuple val(sample), path(fastq)
    output:
        tuple val(sample), path("${sample}_R1.fastq.gz"), path("${sample}_R2.fastq.gz"), emit: reads
    script:
        """
        # Filter to min length 1 to remove empty reads, then use as R1
        seqtk seq -L 1 ${fastq} | gzip -c > ${sample}_R1.fastq.gz
        # Generate R2 as reverse complement of filtered reads
        seqtk seq -L 1 -r -l 0 ${fastq} | gzip -c > ${sample}_R2.fastq.gz
        """
}
