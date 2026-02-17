// Run Bowtie2 on streamed interleaved or single-end input and return mapped and unmapped reads
// NB: This handles non-concordant alignments correctly for this use case (including them with the aligned rather than unaligned reads), so we can skip some downstream processing steps
process BOWTIE2 {
    label "bowtie2_samtools"
    label "small"
    input:
        tuple val(sample), path(reads_interleaved)
        val(index_dir)
        val(params_map) // par_string, suffix, remove_sq, debug, interleaved, db_download_timeout
    output:
        tuple val(sample), path("${sample}_${params_map.suffix}_bowtie2_mapped.sam.gz"), emit: sam
        tuple val(sample), path("${sample}_${params_map.suffix}_bowtie2_mapped.fastq.gz"), emit: reads_mapped
        tuple val(sample), path("${sample}_${params_map.suffix}_bowtie2_unmapped.fastq.gz"), emit: reads_unmapped
        tuple val(sample), path("${sample}_${params_map.suffix}_bowtie2_in.fastq.gz"), emit: input
    shell:
        '''
        set -euo pipefail
        suffix="!{params_map.suffix}"
        # Download Bowtie2 index if not already present
        idx_local_path=\$(download_db.py "!{index_dir}" !{params_map.db_download_timeout})
        # Prepare inputs
        sam="!{sample}_${suffix}_bowtie2_mapped.sam.gz"
        al="!{sample}_${suffix}_bowtie2_mapped.fastq.gz"
        un="!{sample}_${suffix}_bowtie2_unmapped.fastq.gz"
        io="-x ${idx_local_path}/bt2_index !{params_map.interleaved ? "--interleaved" : ""} -"
        par="--threads !{task.cpus} --mm !{params_map.par_string}"
        # Set SAM flags based on whether data is paired-end or single-end
        # For paired-end: flag 12 = read unmapped (4) + mate unmapped (8)
        # For single-end: flag 4 = read unmapped
        unmapped_flag="!{params_map.interleaved ? "12" : "4"}"
        # Run pipeline
        # Outputs a SAM file for all reads, which is then partitioned based on alignment status
        #   - First branch (samtools view -u -f ${unmapped_flag} -) filters SAM to unmapped reads:
        #       For paired-end: both read and mate unmapped
        #       For single-end: read unmapped
        #   - Second branch (samtools view -u -G ${unmapped_flag}) filters SAM to mapped reads:
        #       For paired-end: at least one of read or mate mapped
        #       For single-end: read mapped
        #   - Third branch (samtools view -h -G ${unmapped_flag}) also filters SAM to mapped reads,
        #       optionally removes SQ header lines, then saves SAM
        # Debug statements allow saving of additional SAM files at different steps in the pipeline.
        zcat !{reads_interleaved} \\
            | bowtie2 ${par} ${io} \\
            | tee \\
                !{ params_map.debug ? ">(gzip -c > test_all.sam.gz)" : "" } \\
                >(samtools view -u -f ${unmapped_flag} - \\
                    !{ params_map.debug ? "| tee >(samtools view -h - | gzip -c > test_unmapped.sam.gz)" : "" } \\
                    | samtools fastq -1 /dev/stdout -2 /dev/stdout \\
                        -0 /dev/stdout -s /dev/stdout -N - \\
                    | sed '1~4 s/\\/\\([12]\\)\$/ \\1/' \\
                    | gzip -c > ${un}) \\
                >(samtools view -u -G ${unmapped_flag} - \\
                    !{ params_map.debug ? "| tee >(samtools view -h - | gzip -c > test_mapped.sam.gz)" : "" } \\
                    | samtools fastq -1 /dev/stdout -2 /dev/stdout \\
                        -0 /dev/stdout -s /dev/stdout -N - \\
                    | sed '1~4 s/\\/\\([12]\\)\$/ \\1/' \\
                    | gzip -c > ${al}) \\
            | samtools view -h -G ${unmapped_flag} - \\
            !{ params_map.remove_sq ? "| grep -v '^@SQ'" : "" } | gzip -c > ${sam}
        # Move input files for testing
        in2="!{sample}_${suffix}_bowtie2_in.fastq.gz"
        ln -s !{reads_interleaved} ${in2}
        '''
}

// Generate a Bowtie2 index from an input file
process BOWTIE2_INDEX {
    label "bowtie2_samtools"
    label "max"
    input:
        path(reference_fasta)
        val(outdir)
    output:
        path("${outdir}")
    shell:
        '''
        odir="!{outdir}"
        mkdir ${odir}
        bowtie2-build -f --threads !{task.cpus} !{reference_fasta} ${odir}/bt2_index
        #tar -czf bt2-human-index.tar.gz bt2_human_index
        '''
}
