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
        def extractCmd = reads.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        def op = "${sample}_${params_map.suffix}_bbduk_nomatch.fastq.gz"
        def of = "${sample}_${params_map.suffix}_bbduk_match.fastq.gz"
        def stats = "${sample}_${params_map.suffix}_bbduk.stats.txt"
        def ref = "${contaminant_ref}"
        def il = params_map.interleaved ? 't' : 'f'
        def io = "in=stdin.fastq ref=${ref} out=${op} outm=${of} stats=${stats} interleaved=${il}"
        // pigz=t / unpigz=t makes bbduk.sh shell out to pigz for output
        // compression and (when applicable) input decompression instead of using
        // its single-threaded internal gzip.
        def par = "minkmerfraction=${params_map.min_kmer_fraction} k=${params_map.k} t=${task.cpus} pigz=t unpigz=t -Xmx${task.memory.toGiga()}g"
        """
        ${extractCmd} ${reads} | bbduk.sh ${io} ${par}
        ln -s ${reads} input_${reads}
        """
}
