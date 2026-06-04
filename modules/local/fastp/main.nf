// Run FASTP on streamed data (either single-end or interleaved)
process FASTP {
    label "small"
    label "fastp"
    tag "id=${sample}"
    input:
        tuple val(sample), path(reads)
        path(adapters)
        val(interleaved)
    output:
        tuple val(sample), path("${sample}_fastp.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_fastp_failed.fastq.gz"), emit: failed
        tuple val(sample), path("${sample}_fastp.json"), emit: json
        tuple val(sample), path("${sample}_fastp.html"), emit: html
        tuple val(sample), path("${sample}_fastp_in.fastq.gz"), emit: input
    script:
        /* FASTP cleaning operations:
        * Sliding window trimming with quality threshold;
        * Removing poly-X tails;
        * Automatic adapter detection;
        * Base correction in overlapping paired-end reads;
        * Filter low complexity reads.
        */
        def extractCmd = reads.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        def op = "${sample}_fastp.fastq.gz"
        def of = "${sample}_fastp_failed.fastq.gz"
        def oj = "${sample}_fastp.json"
        def oh = "${sample}_fastp.html"
        def ad = adapters
        def io = "--failed_out ${of} --html ${oh} --json ${oj} --adapter_fasta ${ad} --stdin --stdout ${interleaved ? '--interleaved_in' : ''}"
        def par = "--cut_front --cut_tail --correction --detect_adapter_for_pe --trim_poly_x --cut_mean_quality 20 --average_qual 20 --qualified_quality_phred 20 --verbose --dont_eval_duplication --thread ${task.cpus} --low_complexity_filter --length_required 35"
        def of_trimmed = of - ~/.gz$/
        def op_trimmed = op - ~/.gz$/
        """
        # Execute
        # pigz -1 (fast) is fast enough not to back-pressure fastp through the
        # output pipe; default level -6 is markedly slower than fastp can
        # produce. Same pattern as the streamed MINIMAP2 module and NUCLEAZE.
        ${extractCmd} ${reads} | fastp ${io} ${par} | pigz -p ${task.cpus} -1 -c > ${op}
        # Handle empty output (fastp doesn't handle gzipping empty output properly)
        if [[ ! -s ${of} ]]; then
            mv ${of} ${of_trimmed}
            pigz -p ${task.cpus} ${of_trimmed}
        fi
        if [[ ! -s ${op} ]]; then
            mv ${op} ${op_trimmed}
            pigz -p ${task.cpus} ${op_trimmed}
        fi
        # Link input to output for testing
        ln -s ${reads} ${sample}_fastp_in.fastq.gz
        """
}

