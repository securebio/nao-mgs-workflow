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
        path("${sample}_simulate_pe.log"), emit: log
    script:
        """
        # Count original reads (fast line count / 4)
        total=\$(zcat ${fastq} | wc -l)
        total=\$((total / 4))
        # Filter empty reads to R1, counting kept reads in the same pass
        kept=\$(seqtk seq -L 1 ${fastq} | tee >(gzip -c > ${sample}_R1.fastq.gz) | awk 'NR % 4 == 1' | wc -l)
        dropped=\$((total - kept))
        # R2 = reverse complement of filtered R1
        seqtk seq -r -l 0 ${sample}_R1.fastq.gz | gzip -c > ${sample}_R2.fastq.gz
        # Write log
        echo "${sample}: \${total} total reads, \${kept} kept, \${dropped} dropped (empty)" > ${sample}_simulate_pe.log
        """
}
