// Mask low complexity FASTQ read regions with BBDuk's entropy masking.
// Only works on gzipped FASTQ files.
process MASK_FASTQ_READS {
    label "BBTools"
    label "large"
    tag "id=${sample}"
    input:
        tuple val(sample), path(reads)
        val(window_size)
	    val(entropy)
    output:
        tuple val(sample), path("${sample}_masked.fastq.gz"), emit: masked
        tuple val(sample), path("${sample}_in.fastq.gz"), emit: input
    script:
        // Cap the heap at 75% of task memory to leave room for JVM native memory and pigz
        def heap_gb = Math.max(1, (task.memory.toGiga() * 0.75) as int)
        """
        set -eou pipefail

        # Define input/output
        out=${sample}_masked.fastq.gz

        # Define parameters
        par="entropy=${entropy} entropywindow=${window_size} entropymask=t interleaved=f threads=${task.cpus} -Xmx${heap_gb}g"

        # Execute with streaming approach
        pigz -dc ${reads} | bbduk.sh in=stdin.fastq out=stdout.fastq \${par} | pigz -p ${task.cpus} > \${out}

        # Link input to output for testing
        ln -s ${reads} ${sample}_in.fastq.gz
        """
}
