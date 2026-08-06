// Nucleaze viral k-mer pre-screen on paired-end reads.
// Outputs interleaved gzipped FASTQs (match / nomatch); either side
// can be dropped via keep_match / keep_nomatch (both default true,
// at least one must be true).
process NUCLEAZE {
    label "small"
    label "rust_tools"
    tag "id=${sample}"
    input:
        tuple val(sample), path(reads)   // reads is [R1.fastq, R2.fastq]
        path(index)
        val(params_map)                  // k, minhits, suffix, keep_match?, keep_nomatch?
    output:
        tuple val(sample), path("input_{${reads[0]},${reads[1]}}"), emit: input
        tuple val(sample), path("${sample}_${params_map.suffix}_nucleaze_nomatch.fastq.gz"), emit: nomatch, optional: true
        tuple val(sample), path("${sample}_${params_map.suffix}_nucleaze_match.fastq.gz"), emit: match, optional: true
        tuple val(sample), path("${sample}_${params_map.suffix}_nucleaze.stats.txt"), emit: log
    script:
        def r1 = reads[0]
        def r2 = reads[1]
        def keep_match = params_map.get("keep_match", true)
        def keep_nomatch = params_map.get("keep_nomatch", true)
        if (!keep_match && !keep_nomatch) {
            throw new IllegalArgumentException(
                "NUCLEAZE: at least one of keep_match / keep_nomatch must be true"
            )
        }
        def nomatch_out = "${sample}_${params_map.suffix}_nucleaze_nomatch.fastq.gz"
        def match_out = "${sample}_${params_map.suffix}_nucleaze_match.fastq.gz"
        def stats = "${sample}_${params_map.suffix}_nucleaze.stats.txt"
        def r1ExtractCmd = r1.toString().endsWith(".gz") ? "zcat" : "cat"
        def r2ExtractCmd = r2.toString().endsWith(".gz") ? "zcat" : "cat"
        def keep_match_str = keep_match.toString()
        def keep_nomatch_str = keep_nomatch.toString()
        def empty_match_cmd   = keep_match   ? "gzip -c < /dev/null > ${match_out}"   : ""
        def empty_nomatch_cmd = keep_nomatch ? "gzip -c < /dev/null > ${nomatch_out}" : ""
        def nucleaze_args = "--binref ${index} --k ${params_map.k} --minhits ${params_map.minhits} --canonical --threads ${task.cpus}"
        """
        set -euo pipefail
        # nucleaze emits no files on empty input — synthesise empty gzips.
        r1_first=\$(${r1ExtractCmd} ${r1} | head -c 1 || true)
        r2_first=\$(${r2ExtractCmd} ${r2} | head -c 1 || true)
        if [[ -z "\${r1_first}" && -z "\${r2_first}" ]]; then
            >&2 echo "Warning: Both input read files are empty. Creating empty output files."
            ${empty_match_cmd}
            ${empty_nomatch_cmd}
            echo "No data - empty input files" > ${stats}
        else
            tmpdir=\$(mktemp -d)
            trap 'rm -rf "\${tmpdir}"' EXIT
            PIDS=()
            # Input: nucleaze's built-in gz decompression is single-threaded
            # and becomes the limiting stage; pre-decompress via pigz FIFO.
            # Named FIFOs (not `<(pigz ...)`) so an errexit in the decoder
            # surfaces via wait. Cap at 2 threads — ordinary gz can't be
            # inflated faster.
            in1=${r1}; in2=${r2}
            if [[ "${r1}" == *.gz ]]; then
                mkfifo "\${tmpdir}/in1.fifo"
                pigz -dc -p 2 < ${r1} > "\${tmpdir}/in1.fifo" & PIDS+=(\$!)
                in1="\${tmpdir}/in1.fifo"
            fi
            if [[ "${r2}" == *.gz ]]; then
                mkfifo "\${tmpdir}/in2.fifo"
                pigz -dc -p 2 < ${r2} > "\${tmpdir}/in2.fifo" & PIDS+=(\$!)
                in2="\${tmpdir}/in2.fifo"
            fi
            # Output: named FIFOs (not `>(pigz ...)`) — process substitution
            # hides the subshell PID, so the script can exit mid-trailer and
            # truncate the gzip. -1 keeps pigz from back-pressuring nucleaze.
            outm=/dev/null; outu=/dev/null
            if [[ "${keep_match_str}" == "true" ]]; then
                mkfifo "\${tmpdir}/match.fifo"
                pigz -p ${task.cpus} -1 < "\${tmpdir}/match.fifo" > ${match_out} & PIDS+=(\$!)
                outm="\${tmpdir}/match.fifo"
            fi
            if [[ "${keep_nomatch_str}" == "true" ]]; then
                mkfifo "\${tmpdir}/nomatch.fifo"
                pigz -p ${task.cpus} -1 < "\${tmpdir}/nomatch.fifo" > ${nomatch_out} & PIDS+=(\$!)
                outu="\${tmpdir}/nomatch.fifo"
            fi
            # Capture both pipe stages separately: nucleaze failure and tee
            # (stats-log write) failure need different handling.
            pipestatus=(0 0)
            nucleaze --in "\${in1}" --in2 "\${in2}" --outm "\${outm}" --outu "\${outu}" ${nucleaze_args} 2>&1 | tee ${stats} || pipestatus=("\${PIPESTATUS[@]}")
            nucleaze_status=\${pipestatus[0]}
            tee_status=\${pipestatus[1]}
            if [[ "\${nucleaze_status}" -ne 0 ]]; then
                # nucleaze exited before opening its FIFOs (e.g. an incompatible
                # --binref index dies during indexing). The pigz helpers are then
                # blocked in open() forever; tear them down so the task fails fast
                # instead of stalling. kill on an already-dead pid is harmless.
                kill "\${PIDS[@]}" 2>/dev/null || true
                wait "\${PIDS[@]}" 2>/dev/null || true
                exit "\${nucleaze_status}"
            fi
            # nucleaze succeeded: drain the pigz feeders and let the output
            # compressors flush their gzip trailers before exiting (otherwise
            # the .gz outputs truncate). A failing helper trips errexit.
            for pid in "\${PIDS[@]}"; do wait "\${pid}"; done
            # A failed tee means the stats log is incomplete; fail the task
            # rather than silently reporting success.
            [[ "\${tee_status}" -eq 0 ]] || exit "\${tee_status}"
        fi
        ln -s ${r1} input_${r1}
        ln -s ${r2} input_${r2}
        """
}
