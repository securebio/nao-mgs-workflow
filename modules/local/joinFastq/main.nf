// Join and concatenate partially-merged interleaved FASTQ files into a single read file
// TODO: Consider replacing with a Rust script for speed
process JOIN_FASTQ {
    label "biopython"
    label "single"
    input:
        tuple val(sample), path(reads) // Merged single reads, then unmerged interleaved
        val(debug)
    output:
        tuple val(sample), path("${sample}_joined.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_joined_in_{merged,unmerged}.fastq.gz"), emit: input
    shell:
        '''
        # Prepare to join unmerged read pairs
        om=!{reads[0]}
        ou=!{reads[1]}
        oj=!{sample}_bbmerge_unmerged_joined.fastq.gz
        # Join unmerged read pairs
        join_fastq_interleaved.py ${ou} ${oj} !{ debug ? "--debug" : "" }
        # Concatenate single output file
        oo=!{sample}_joined.fastq.gz
        cat ${om} ${oj} > ${oo}
        # Link input reads for testing
        im=!{sample}_joined_in_merged.fastq.gz
        iu=!{sample}_joined_in_unmerged.fastq.gz
        ln -s ${om} ${im}
        ln -s ${ou} ${iu}
        '''
}

// Join and concatenate partially-merged interleaved FASTQ files into a single read file
// TODO: Consider replacing with a Rust script for speed
process JOIN_FASTQ_LIST {
    label "biopython"
    label "single"
    input:
        tuple val(sample), path(reads)
        val(debug)
    output:
        tuple val(sample), path("${sample}_*[0-9]_joined.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_*_joined_in_{merged,unmerged}.fastq.gz"), emit: input
    shell:
        '''
        for merged_file in !{sample}_*_bbmerge_merged.fastq.gz; do
            # Prepare to join unmerged read pairs
            species=$(basename ${merged_file} | grep -oP '!{sample}_\\K\\d+(?=_)')
            unmerged_file="!{sample}_${species}_bbmerge_unmerged.fastq.gz"
            output=!{sample}_${species}_joined.fastq.gz
            temp_joined=!{sample}_${species}_bbmerge_unmerged_joined.fastq.gz

            if [[ ! -f ${unmerged_file} ]]; then
                >&2 echo "Error: Matching unmerged file not found for ${merged_file}"
                exit 1
            fi

            # Join unmerged read pairs
            join_fastq_interleaved.py ${unmerged_file} ${temp_joined} !{ debug ? "--debug" : "" }
            # Concatenate single output file
            cat ${merged_file} ${temp_joined} > ${output}
            rm ${temp_joined}

            # Link input reads for testing
            ln -s ${merged_file} !{sample}_${species}_joined_in_merged.fastq.gz
            ln -s ${unmerged_file} !{sample}_${species}_joined_in_unmerged.fastq.gz
        done
        '''
}
