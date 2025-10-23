process EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsv) // Viral hits TSV
        val(drop_unpaired) // Boolean
    output:
        tuple val(sample), path("${sample}_hits_out.fastq.gz"), emit: output
        tuple val(sample), path("${sample}_hits_in.tsv.gz"), emit: input
    shell:
        '''
        extract_viral_hits.py !{drop_unpaired ? "-d" : ""} -i !{tsv} -o !{sample}_hits_out.fastq.gz
        # Link input files for testing
        ln -s !{tsv} !{sample}_hits_in.tsv.gz
        '''
}

process EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED_LIST {
    label "python"
    label "single"
    input:
        tuple val(sample), path(tsvs)
        val(drop_unpaired)
    output:
        tuple val(sample), path("${sample}_*_hits_out.fastq.gz"), emit: output
        tuple val(sample), path("${sample}_*_hits_in.tsv.gz"), emit: input
    shell:
        '''
        for tsv in !{tsvs}; do
            species=$(basename ${tsv} | grep -oP 'partition_\\K\\d+(?=_)')
            if [ -z "$species" ]; then
                >&2 echo "Error: Could not extract species from filename: ${tsv}"
                exit 1
            fi
            fastq_out=!{sample}_${species}_hits_out.fastq.gz
            extract_viral_hits.py !{drop_unpaired ? "-d" : ""} -i ${tsv} -o ${fastq_out}
            ln -s ${tsv} !{sample}_${species}_hits_in.tsv.gz
        done
        '''
}
