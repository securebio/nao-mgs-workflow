// Mask low complexity FASTQ read regions. Only works on gzipped FASTQ files.
process MASK_FASTQ_READS {
    label "large"
    label "BBTools"
    input:
        tuple val(sample), path(reads)
        val(window_size)
	    val(entropy)
    output:
        tuple val(sample), path("${sample}_masked.fastq.gz"), emit: masked
        tuple val(sample), path("${sample}_in.fastq.gz"), emit: input
    shell:
        '''
        set -eou pipefail

        # Define input/output
        out=!{sample}_masked.fastq.gz

        # Define parameters
        par="window=!{window_size} entropy=!{entropy} -Xmx!{task.memory.toGiga()}g"

        # If input is empty, create empty gzipped output (bbmask errors on empty input)
        if [[ -z "$(zcat "!{reads}" | head)" ]]; then
            echo -n | gzip > ${out}
        else
            # Execute with streaming approach
            zcat -f !{reads} | bbmask.sh in=stdin.fastq out=stdout.fastq ${par} | gzip > ${out}
        
            # Check for empty output file without empty input
            if [[ -z "$(zcat "${out}" | head)" ]]; then
                echo "Error: Output file is empty."
                exit 1
            fi
        fi

        # Link input to output for testing
        ln -s !{reads} !{sample}_in.fastq.gz
        '''
}