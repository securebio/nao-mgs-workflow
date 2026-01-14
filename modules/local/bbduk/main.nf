// Streamed version (interleaved or single-end input and output)
process BBDUK {
    label "small"
    label "BBTools"
    input:
        tuple val(sample), path(reads) // Interleaved or single-end
        path(contaminant_ref)
        val(params_map) // min_kmer_fraction, k, suffix, interleaved
    output:
        tuple val(sample), path("${sample}_${params_map.suffix}_bbduk_nomatch.fastq.gz"), emit: nomatch
        tuple val(sample), path("${sample}_${params_map.suffix}_bbduk_match.fastq.gz"), emit: match
        tuple val(sample), path("${sample}_${params_map.suffix}_bbduk.stats.txt"), emit: log
        tuple val(sample), path("input_${reads}"), emit: input
    script:
        def extractCmd = reads.toString().endsWith(".gz") ? "zcat" : "cat"
        def op = "${sample}_${params_map.suffix}_bbduk_nomatch.fastq.gz"
        def of = "${sample}_${params_map.suffix}_bbduk_match.fastq.gz"
        def stats = "${sample}_${params_map.suffix}_bbduk.stats.txt"
        def ref = "${contaminant_ref}"
        def il = params_map.interleaved ? 't' : 'f'
        def io = "in=stdin.fastq ref=${ref} out=${op} outm=${of} stats=${stats} interleaved=${il}"
        def par = "minkmerfraction=${params_map.min_kmer_fraction} k=${params_map.k} t=${task.cpus} -Xmx${task.memory.toGiga()}g"
        """
        ${extractCmd} ${reads} | bbduk.sh ${io} ${par}
        ln -s ${reads} input_${reads}
        """
}

// Streamed version of BBDUK_HITS that returns an interleaved file
// Uses minkmerhits instead of minkmerfraction
process BBDUK_HITS_INTERLEAVE {
    label "small"
    label "BBTools"
    input:
        tuple val(sample), path(reads)
        path(contaminant_ref)
        val(params_map) // min_kmer_hits, k, suffix
    output:
        tuple val(sample), path("input_{${reads[0]},${reads[1]}}"), emit: input
        tuple val(sample), path("${sample}_${params_map.suffix}_bbduk_pass.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_${params_map.suffix}_bbduk_fail.fastq.gz"), emit: fail
        tuple val(sample), path("${sample}_${params_map.suffix}_bbduk.stats.txt"), emit: log
    script:
        def extractCmd = reads.toString().endsWith(".gz") ? "zcat" : "cat"
        def in1 = "${reads[0]}"
        def in2 = "${reads[1]}"
        def op = "${sample}_${params_map.suffix}_bbduk_pass.fastq.gz"
        def of = "${sample}_${params_map.suffix}_bbduk_fail.fastq.gz"
        def stats = "${sample}_${params_map.suffix}_bbduk.stats.txt"
        def ref = "${contaminant_ref}"
        def io = "in=stdin.fastq ref=${ref} out=${op} outm=${of} stats=${stats}"
        def par = "minkmerhits=${params_map.min_kmer_hits} k=${params_map.k} interleaved=t t=${task.cpus} -Xmx${task.memory.toGiga()}g"
        """
        # Execute
        paste <(${extractCmd} ${in1} | paste - - - - ) <(${extractCmd} ${in2} | paste - - - -) | tr "\t" "\n" | bbduk.sh ${io} ${par}
        # Move inputs for testing
        ln -s ${in1} input_${in1}
        ln -s ${in2} input_${in2}
        """
}
