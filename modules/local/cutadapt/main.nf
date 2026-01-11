process CUTADAPT {
    label "cutadapt"
    label "small"
    input:
        tuple val(sample), path(reads_interleaved)
        path(adapters)
	val(error_rate)
    output:
        tuple val(sample), path("${sample}_cutadapt.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_cutadapt_log.txt"), emit: log
        tuple val(sample), path("${sample}_cutadapt_in.fastq.gz"), emit: input
    shell:
        /* Explanation of cutadapt parameters:
        -b (-B for R2 read) to trim any adapter in the adapters file from either end of either read
        -j number of cpu cores
        -m to drop a read pair where either element of the pair is <20 bp after trimming
        -e maximum error rate (as a fraction between 0 and 1) to allow in the matching region 
            between an adapter and the read
        --action=trim to trim adapters and up/downstream sequence
        */
        '''
        output="!{sample}_cutadapt.fastq.gz"
        log="!{sample}_cutadapt_log.txt"
        par="-b file:!{adapters} -B file:!{adapters} -j !{task.cpus} -m 20 -e !{error_rate} --action=trim --interleaved"
        zcat !{reads_interleaved} | cutadapt ${par} - 2> ${log} | gzip -c > ${output}
        ln -s !{reads_interleaved} !{sample}_cutadapt_in.fastq.gz
        '''
}

