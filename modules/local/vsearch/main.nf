// Cluster merged FASTQ sequences with VSEARCH
process VSEARCH_CLUSTER_LIST {
    label "large"
    label "vsearch"
    input:
        tuple val(sample), path(reads) // Single-end or merged reads
        val(identity_threshold) // Minimum required identity (0.0-1.0) required for two sequences to cluster together
        val(identity_method) // Method for calculating identity (see VSEARCH documentation)
        val(min_seq_length) // Minimum sequence length required by VSEARCH
    output:
        tuple val(sample), path("${sample}_*_vsearch_reps.fasta.gz"), emit: reps
        tuple val(sample), path("${sample}_*_vsearch_summary.tsv.gz"), emit: summary
        tuple val(sample), path("${sample}_*_vsearch_log.txt"), emit: log
        tuple val(sample), path("input_*"), emit: input
    shell:
        '''
        for reads_file in !{reads}; do
            # Define paths and parameters
            species=$(basename ${reads_file} | grep -oP '!{sample}_\\K\\d+(?=_)')
            if [ -z "$species" ]; then
                >&2 echo "Error: Could not extract species from filename: ${reads_file}"
                exit 1
            fi
            or=!{sample}_${species}_vsearch_reps.fasta
            os=!{sample}_${species}_vsearch_summary.tsv
            log=!{sample}_${species}_vsearch_log.txt
            io="--log ${log} --centroids ${or} --uc ${os} --cluster_fast ${reads_file}"
            par="--threads !{task.cpus} --id !{identity_threshold} --iddef !{identity_method} --minseqlength !{min_seq_length}"
            # Add decompression if necessary
            par="${par}$([[ ${reads_file} == *.gz ]] && echo ' --gzip_decompress' || echo '')"
            # Execute
            vsearch ${par} ${io}
            # Gzip outputs
            gzip -c ${or} > ${or}.gz
            gzip -c ${os} > ${os}.gz
            # Link input to output for testing
            ln -s ${reads_file} input_${reads_file}
        done
        '''
}
