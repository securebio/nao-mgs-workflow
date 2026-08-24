// Create a minimap2 index
process MINIMAP2_INDEX {
    label "max"
    label "minimap2_samtools"
    tag "id=index,name=${outdir}"
    input:
        path(reference_fasta)
        val(outdir)
    output:
        path("${outdir}"), emit: output
        path("input_${reference_fasta}"), emit: input

    script:
        def odir = outdir
        def preset = "lr:hq"
        """
        mkdir ${odir}
        minimap2 -x ${preset} -d ${odir}/mm2_index.mmi ${reference_fasta}

        # Link input to output for testing
        ln -s ${reference_fasta} input_${reference_fasta}
        """
}

// Run minimap2 on a single input FASTQ file and partition reads based on alignment status.
//
// Set split_index for a reference too large for one index block (`-I`, 8G by default).
// minimap2 re-reads the query once per block, so it must be a regular file rather than a
// pipe, and --split-prefix is needed to merge the per-block output. Both constraints fail
// silently if broken: a piped query under --split-prefix emits nothing, and a multi-part
// index without it emits one copy of each read per block.
process MINIMAP2 {
    label "large"
    label "minimap2_samtools"
    tag "id=${sample}"
    input:
        tuple val(sample), path(reads)
        val(index_dir)
        val(params_map) // suffix, remove_sq, alignment_params, db_download_timeout, split_index
    output:
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_mapped.sam.gz"), emit: sam
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_mapped.fastq.gz"), emit: reads_mapped
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_unmapped.fastq.gz"), emit: reads_unmapped
        tuple val(sample), path("input_${reads}"), emit: input
    script:
        def extractCmd = reads.toString().endsWith(".gz") ? "zcat" : "cat"
        def sam = "${sample}_${params_map.suffix}_minimap2_mapped.sam.gz"
        def al = "${sample}_${params_map.suffix}_minimap2_mapped.fastq.gz"
        def un = "${sample}_${params_map.suffix}_minimap2_unmapped.fastq.gz"
        def split_index = params_map.get("split_index", false)
        def idx = "\${idx_local_path}/mm2_index.mmi"
        def align = split_index
            ? "minimap2 -a -t ${task.cpus} ${params_map.alignment_params} ${idx} ${reads} --split-prefix mm2_split_"
            : "${extractCmd} ${reads} | minimap2 -a -t ${task.cpus} ${params_map.alignment_params} ${idx} /dev/fd/0"
        """
        set -euo pipefail
        # Download Minimap2 index if not already present
        idx_local_path=\$(download_db.py "${index_dir}" "${params_map.db_download_timeout}")
        # Partition the SAM stream by alignment status:
        #   - First branch (samtools view -u -f 4 -) filters SAM to unaligned reads and saves FASTQ
        #   - Second branch (samtools view -u -F 4 -) filters SAM to aligned reads and saves FASTQ
        #   - Third branch (samtools view -h -F 4 -) also filters SAM to aligned reads and saves SAM
        ${align} \\
            | tee \\
                >(samtools view -u -f 4 - \\
                    | samtools fastq - | gzip -c > ${un}) \\
                >(samtools view -u -F 4 - \\
                    | samtools fastq - | gzip -c > ${al}) \\
            | samtools view -h -F 4 - \\
            ${ params_map.remove_sq ? "| grep -v '^@SQ'" : "" } | gzip -c > ${sam}
        # Link input to output for testing
        ln -s ${reads} input_${reads}
        """
}
