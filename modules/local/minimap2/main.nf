// Generate a Minimap2 index from an input FASTA.
// minimap2 emits the index incrementally, which is slow against the Fusion-backed work
// directory, so the build runs on local scratch and the finished index is staged out in
// one pass.
process MINIMAP2_INDEX {
    label "max"
    label "minimap2_samtools"
    label "use_scratch"
    tag "id=index,name=${outdir}"
    input:
        path(reference_fasta)
        val(outdir)
    output:
        path("${outdir}"), emit: output

    script:
        def odir = outdir
        def preset = "lr:hq"
        """
        set -euo pipefail
        mkdir ${odir}
        minimap2 -x ${preset} -t ${task.cpus} -d ${odir}/mm2_index.mmi ${reference_fasta}
        """
}

// Run minimap2 on a single input FASTQ file and partition reads based on alignment status.
//
// Set split_index for a reference too large for one index block (`-I`, 8G by default).
// minimap2 re-reads the query once per block, so it must be a regular file rather than a
// pipe, and --split-prefix is needed to merge the per-block output.
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
        // Ordinary gzip can't be inflated in parallel; cap the decompressor at 2 threads.
        def extractCmd = reads.toString().endsWith(".gz") ? "pigz -dc -p 2" : "cat"
        def sam = "${sample}_${params_map.suffix}_minimap2_mapped.sam.gz"
        def al = "${sample}_${params_map.suffix}_minimap2_mapped.fastq.gz"
        def un = "${sample}_${params_map.suffix}_minimap2_unmapped.fastq.gz"
        def split_index = params_map.get("split_index", false)
        def idx = "\${idx_local_path}/mm2_index.mmi"
        def alignCmd = split_index
            ? "minimap2 -a -t ${task.cpus} ${params_map.alignment_params} ${idx} ${reads} --split-prefix mm2_split_ 2> minimap2.log"
            : "${extractCmd} ${reads} | minimap2 -a -t ${task.cpus} ${params_map.alignment_params} ${idx} /dev/fd/0 2> minimap2.log"
        """
        set -eou pipefail
        # Download Minimap2 index if not already present
        idx_local_path=\$(download_db.py "${index_dir}" "${params_map.db_download_timeout}")
        tmpdir=\$(mktemp -d)
        # Also surfaces minimap2's own log on every exit path, so a failure stays debuggable.
        trap 'cat minimap2.log >&2 2>/dev/null || true; rm -rf "\${tmpdir}"' EXIT
        PIDS=()
        # Named FIFOs so errors surface, based on modules/local/nucleaze/main.nf.
        mkfifo "\${tmpdir}/un.fifo" "\${tmpdir}/al.fifo"
        # Partition the SAM stream by alignment status:
        #   - First branch (samtools view -u -f 4 -) filters SAM to unaligned reads and saves FASTQ
        #   - Second branch (samtools view -u -F 4 -) filters SAM to aligned reads and saves FASTQ
        #   - Third branch (samtools view -h -F 4 -) also filters SAM to aligned reads and saves SAM
        # Each branch gets full threads, since they are usually unevenly loaded.
        ( samtools view -u -f 4 - < "\${tmpdir}/un.fifo" \\
            | samtools fastq - | pigz -p ${task.cpus} -1 -c > ${un} ) & PIDS+=(\$!)
        ( samtools view -u -F 4 - < "\${tmpdir}/al.fifo" \\
            | samtools fastq - | pigz -p ${task.cpus} -1 -c > ${al} ) & PIDS+=(\$!)
        ${alignCmd} \\
            | tee "\${tmpdir}/un.fifo" "\${tmpdir}/al.fifo" \\
            | samtools view -h -F 4 - \\
            ${ params_map.remove_sq ? "| grep -v '^@SQ'" : "" } | pigz -p ${task.cpus} -1 -c > ${sam}
        # Wait for the branch compressors to flush their gzip trailers, or the .gz
        # outputs truncate. A failing branch trips errexit.
        for pid in "\${PIDS[@]}"; do wait "\${pid}"; done
        # Fail rather than simply warn if --split-prefix is not set for a multi-part index.
        if grep -qF "For a multi-part index" minimap2.log; then
            echo "ERROR: ${index_dir} is a multi-part index; set split_index in params_map" >&2
            exit 1
        fi
        # Link input to output for testing
        ln -s ${reads} input_${reads}
        """
}
