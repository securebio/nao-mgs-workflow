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

// Run minimap2 on a single input FASTQ file and partition reads based on alignment status
process MINIMAP2 {
    label "large"
    label "minimap2_samtools"
    tag "id=${sample}"
    input:
        tuple val(sample), path(reads)
        val(index_dir)
        val(params_map) // suffix, remove_sq, alignment_params, db_download_timeout
    output:
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_mapped.sam.gz"), emit: sam
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_mapped.fastq.gz"), emit: reads_mapped
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_unmapped.fastq.gz"), emit: reads_unmapped
        tuple val(sample), path("input_${reads}"), emit: input
    script:
        def sam = "${sample}_${params_map.suffix}_minimap2_mapped.sam.gz"
        def al = "${sample}_${params_map.suffix}_minimap2_mapped.fastq.gz"
        def un = "${sample}_${params_map.suffix}_minimap2_unmapped.fastq.gz"
        // Each compressor gets the full allocation rather than a share of it.
        // The branches are unevenly loaded and rarely saturate at the same time,
        // so pigz threads idle when their branch is starved; splitting the
        // allocation just leaves cores unused. Measured: full allocation is
        // -8.7% cpu-hours against task.cpus/4 over eight real samples.
        def pigz_threads = task.cpus as int
        def isGz = reads.toString().endsWith(".gz")
        """
        set -eou pipefail
        # Download Minimap2 index if not already present
        idx_local_path=\$(download_db.py "${index_dir}" "${params_map.db_download_timeout}")
        tmpdir=\$(mktemp -d)
        trap 'rm -rf "\${tmpdir}"' EXIT
        PIDS=()
        # Named FIFOs, not `>(...)`: process substitution hides the subshell PID,
        # so the script can exit before a compressor writes its gzip trailer and
        # silently truncate the output. See modules/local/nucleaze/main.nf.
        mkfifo "\${tmpdir}/un.fifo" "\${tmpdir}/al.fifo"
        # Partition the SAM stream by alignment status:
        #   - unmapped branch (samtools view -u -f 4 -) saves unaligned reads as FASTQ
        #   - mapped branch (samtools view -u -F 4 -) saves aligned reads as FASTQ
        #   - main pipeline (samtools view -h -F 4 -) saves aligned reads as SAM
        # `view -u` emits uncompressed BAM into `fastq`, so neither needs -@; all
        # compression is done by pigz.
        ( samtools view -u -f 4 - < "\${tmpdir}/un.fifo" \\
            | samtools fastq - | pigz -p ${pigz_threads} -1 -c > ${un} ) & PIDS+=(\$!)
        ( samtools view -u -F 4 - < "\${tmpdir}/al.fifo" \\
            | samtools fastq - | pigz -p ${pigz_threads} -1 -c > ${al} ) & PIDS+=(\$!)
        # Ordinary gzip can't be inflated in parallel; pigz only adds helper
        # threads for read/write, so cap the decompressor at 2.
        ${isGz ? "pigz -dc -p 2" : "cat"} ${reads} \\
            | minimap2 -a -t ${task.cpus} ${params_map.alignment_params} \${idx_local_path}/mm2_index.mmi /dev/fd/0 \\
            | tee "\${tmpdir}/un.fifo" "\${tmpdir}/al.fifo" \\
            | samtools view -h -F 4 - \\
            ${ params_map.remove_sq ? "| grep -v '^@SQ'" : "" } | pigz -p ${pigz_threads} -1 -c > ${sam}
        # Let the branch compressors flush their gzip trailers before exiting,
        # otherwise the .gz outputs truncate. A failing branch trips errexit.
        for pid in "\${PIDS[@]}"; do wait "\${pid}"; done
        # Link input to output for testing
        ln -s ${reads} input_${reads}
        """
}

// Run minimap2 against a multi-part index and partition reads based on alignment status.
// A reference too large for one index block (see `-I`) is split across blocks, and
// minimap2 then re-reads the query once per block, so the query must be a regular file
// rather than a pipe and `--split-prefix` is required to merge the per-block output.
// Only the query is constrained: minimap2 still merges to stdout, so the partitioning
// fan-out is identical to MINIMAP2.
process MINIMAP2_SPLIT_INDEX {
    label "max"
    label "minimap2_samtools"
    tag "id=${sample}"
    input:
        tuple val(sample), path(reads)
        path(index_dir)
        val(params_map) // suffix, remove_sq, alignment_params
    output:
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_mapped.sam.gz"), emit: sam
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_mapped.fastq.gz"), emit: reads_mapped
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_unmapped.fastq.gz"), emit: reads_unmapped
        tuple val(sample), path("${sample}_${params_map.suffix}_minimap2_in.fastq.gz"), emit: input
    script:
        def suffix = params_map.suffix
        def idx = "${index_dir}/mm2_index.mmi"
        def sam = "${sample}_${suffix}_minimap2_mapped.sam.gz"
        def al = "${sample}_${suffix}_minimap2_mapped.fastq.gz"
        def un = "${sample}_${suffix}_minimap2_unmapped.fastq.gz"
        def in2 = "${sample}_${suffix}_minimap2_in.fastq.gz"
        // Four consumers run concurrently (minimap2 plus three compressors), so
        // split the allocation rather than giving each pigz every core.
        def pigz_threads = Math.max(1, (task.cpus as int).intdiv(4))
        """
        set -euo pipefail
        tmpdir=\$(mktemp -d)
        trap 'rm -rf "\${tmpdir}"' EXIT
        PIDS=()
        # Named FIFOs, not `>(...)`: process substitution hides the subshell PID,
        # so the script can exit before a compressor writes its gzip trailer and
        # silently truncate the output. See modules/local/nucleaze/main.nf.
        mkfifo "\${tmpdir}/un.fifo" "\${tmpdir}/al.fifo"
        # Partition the SAM stream by alignment status:
        #   - unmapped branch (samtools view -u -f 4 -) saves unaligned reads as FASTQ
        #   - mapped branch (samtools view -u -F 4 -) saves aligned reads as FASTQ
        #   - main pipeline (samtools view -h -F 4 -) saves aligned reads as SAM
        # `view -u` emits uncompressed BAM into `fastq`, so neither needs -@; all
        # compression is done by pigz.
        ( samtools view -u -f 4 - < "\${tmpdir}/un.fifo" \\
            | samtools fastq - | pigz -p ${pigz_threads} -1 -c > ${un} ) & PIDS+=(\$!)
        ( samtools view -u -F 4 - < "\${tmpdir}/al.fifo" \\
            | samtools fastq - | pigz -p ${pigz_threads} -1 -c > ${al} ) & PIDS+=(\$!)
        # --split-prefix scratch files stay in the task work directory, which is
        # sized for the run; \${tmpdir} only carries the FIFOs.
        minimap2 -a -t ${task.cpus} ${params_map.alignment_params} ${idx} ${reads} --split-prefix "mm2_split_" \\
            | tee "\${tmpdir}/un.fifo" "\${tmpdir}/al.fifo" \\
            | samtools view -h -F 4 - \\
            ${ params_map.remove_sq ? "| grep -v '^@SQ'" : "" } | pigz -p ${pigz_threads} -1 -c > ${sam}
        # Let the branch compressors flush their gzip trailers before exiting,
        # otherwise the .gz outputs truncate. A failing branch trips errexit.
        for pid in "\${PIDS[@]}"; do wait "\${pid}"; done
        # Link input to output for testing
        ln -s ${reads} ${in2}
        """
}
