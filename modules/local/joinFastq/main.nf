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
        tuple val(sample), path(merged_reads), path(unmerged_reads)
        val(debug)
    output:
        tuple val(sample), path("${sample}_*[0-9]_joined.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_*_joined_in_{merged,unmerged}.fastq.gz"), emit: input
    shell:
        '''
        set -euo pipefail

        merged_array=(!{merged_reads})
        unmerged_array=(!{unmerged_reads})
 
        for i in "${!merged_array[@]}"; do
            merged_file="${merged_array[$i]}"
            unmerged_file="${unmerged_array[$i]}"

            species=$(basename ${merged_file} | grep -oP '!{sample}_\\K\\d+(?=_)')
            if [ -z "$species" ]; then
                >&2 echo "Error: Could not extract species from filename: ${merged_file}"
                exit 1
            fi

            output=!{sample}_${species}_joined.fastq.gz
            temp_joined=!{sample}_${species}_bbmerge_unmerged_joined.fastq.gz

            join_fastq_interleaved.py ${unmerged_file} ${temp_joined} !{ debug ? "--debug" : "" }

            cat ${merged_file} ${temp_joined} > ${output}
            rm ${temp_joined}

            ln -s ${merged_file} !{sample}_${species}_joined_in_merged.fastq.gz
            ln -s ${unmerged_file} !{sample}_${species}_joined_in_unmerged.fastq.gz
        done
        '''
}
