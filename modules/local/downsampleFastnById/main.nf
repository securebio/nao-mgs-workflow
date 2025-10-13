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
        for reads_file in !{sample}_*_joined.{fastq,fasta}.gz; do
            [[ -e ${reads_file} ]] || continue
            species=$(basename ${reads_file} | grep -oP '!{sample}_\\K\\d+(?=_)')
            ids_file="!{sample}_${species}_vsearch_ids.txt"
            output=downsampled_${reads_file}

            if [[ ! -f ${ids_file} ]]; then
                >&2 echo "Error: Matching IDs file not found for ${reads_file}"
                exit 1
            fi

            seqkit grep -f ${ids_file} ${reads_file} | seqkit rmdup | !{fastn.toString().endsWith(".gz") ? 'gzip -c' : 'cat'} > ${output}

            ln -s ${reads_file} input_${reads_file}
            ln -s ${ids_file} input_${ids_file}
        done
        '''
}
