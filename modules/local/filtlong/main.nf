process FILTLONG {
    label "small"
    label "filtlong"
    input:
        tuple val(sample), path(reads)
        val(min_length)
        val(max_length)
        val(min_mean_q)
    output:
        tuple val(sample), path("${sample}_filtlong.fastq.gz"), emit: reads
    script:
        // Filter reads based on min length, max length, and min mean quality
        """
        set -euo pipefail
        o=${sample}_filtlong.fastq.gz
        i=${reads[0]}
        filtlong --min_length ${min_length} --max_length ${max_length} --min_mean_q ${min_mean_q} --verbose \${i} | gzip > \${o}
        """
}
