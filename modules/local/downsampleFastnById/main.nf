// Subset a FASTA or FASTQ file to specific IDs
process DOWNSAMPLE_FASTN_BY_ID_LIST {
    label "seqkit"
    label "single"
    input:
        tuple val(sample), path(fastn), path(ids)
    output:
        tuple val(sample), path("downsampled_*"), emit: output
        tuple val(sample), path("input_*"), emit: input
    shell:
        '''
        reads_array=(!{fastn})
        ids_array=(!{ids})

        for i in "${!reads_array[@]}"; do
            reads_file="${reads_array[$i]}"
            ids_file="${ids_array[$i]}"
            output=downsampled_${reads_file}

            seqkit grep -f ${ids_file} ${reads_file} | seqkit rmdup | { [[ "${reads_file}" == *.gz ]] && gzip -c || cat; } > ${output}

            ln -s ${reads_file} input_${reads_file}
            ln -s ${ids_file} input_${ids_file}
        done
        '''
}
