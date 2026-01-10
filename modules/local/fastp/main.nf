// Run FASTP on streamed data (either single-end or interleaved)
process FASTP {
    label "small"
    label "fastp"
    input:
        tuple val(sample), path(reads)
        path(adapters)
        val(interleaved)
    output:
        tuple val(sample), path("${sample}_fastp.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_fastp_failed.fastq.gz"), emit: failed
        tuple val(sample), path("${sample}_fastp.{json,html}"), emit: log
        tuple val(sample), path("${sample}_fastp_in.fastq.gz"), emit: input
    script:
        /* Cleaning not done in CUTADAPT:
        * Higher quality threshold for sliding window trimming;
        * Removing poly-X tails;
        * Automatic adapter detection;
        * Base correction in overlapping paired-end reads;
        * Filter low complexity reads.
        */
        def extractCmd = reads.toString().endsWith(".gz") ? "zcat" : "cat"
        def op = "${sample}_fastp.fastq.gz"
        def of = "${sample}_fastp_failed.fastq.gz"
        def oj = "${sample}_fastp.json"
        def oh = "${sample}_fastp.html"
        def ad = adapters
        def io = "--failed_out ${of} --html ${oh} --json ${oj} --adapter_fasta ${ad} --stdin --stdout ${interleaved ? '--interleaved_in' : ''}"
        def par = "--cut_front --cut_tail --correction --detect_adapter_for_pe --trim_poly_x --cut_mean_quality 20 --average_qual 20 --qualified_quality_phred 20 --verbose --dont_eval_duplication --thread ${task.cpus} --low_complexity_filter"
        def of_trimmed = of - ~/.gz$/
        def op_trimmed = op - ~/.gz$/
        """
        # Execute
        ${extractCmd} ${reads} | fastp ${io} ${par} | gzip -c > ${op}
        # Handle empty output (fastp doesn't handle gzipping empty output properly)
        if [[ ! -s ${of} ]]; then
            mv ${of} ${of_trimmed}
            gzip ${of_trimmed}
        fi
        if [[ ! -s ${op} ]]; then
            mv ${op} ${op_trimmed}
            gzip ${op_trimmed}
        fi
        # Link input to output for testing
        ln -s ${reads} ${sample}_fastp_in.fastq.gz
        """
}