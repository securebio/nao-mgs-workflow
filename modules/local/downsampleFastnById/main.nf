// Subset a FASTA or FASTQ file to specific IDs
process DOWNSAMPLE_FASTN_BY_ID {
    label "seqkit"
    label "single"
    input:
        tuple val(sample), path(fastn), path(ids)
    output:
        tuple val(sample), path("downsampled_${fastn}"), emit: output
        tuple val(sample), path("input_${fastn}"), path("input_${ids}"),  emit: input
    shell:
        '''
        seqkit grep -f !{ids} !{fastn} | seqkit rmdup | !{fastn.toString().endsWith(".gz") ? 'gzip -c' : 'cat'} > downsampled_!{fastn}
        ln -s !{fastn} input_!{fastn}
        ln -s !{ids} input_!{ids}
        '''
}

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

            seqkit grep -f ${ids_file} ${reads_file} | seqkit rmdup | !{fastn.toString().endsWith(".gz") ? 'gzip -c' : 'cat'} > ${output}

            ln -s ${reads_file} input_${reads_file}
            ln -s ${ids_file} input_${ids_file}
        done
        '''
}
