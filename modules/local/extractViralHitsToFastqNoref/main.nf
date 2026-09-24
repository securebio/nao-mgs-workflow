process EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED_LIST {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(tsvs)
        val(drop_unpaired)
    output:
        tuple val(sample), path("${sample}_*_hits_out.fastq.gz"), emit: output
        tuple val(sample), path("${sample}_*_hits_in.tsv.gz"), emit: input
    script:
        """
        # Label outputs by position: a group with no hits passes one unpartitioned table
        i=0
        for tsv in ${tsvs}; do
            fastq_out=${sample}_\${i}_hits_out.fastq.gz
            extract_viral_hits.py ${drop_unpaired ? "-d" : ""} -i \${tsv} -o \${fastq_out}
            ln -s \${tsv} ${sample}_\${i}_hits_in.tsv.gz
            i=\$((i + 1))
        done
        """
}
