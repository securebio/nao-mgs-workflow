// Nucleaze viral k-mer pre-screen on paired-end reads.
// Outputs interleaved gzipped FASTQs (match / nomatch); either side
// can be dropped via keep_match / keep_nomatch (both default true,
// at least one must be true).
process NUCLEAZE {
    label "small"
    label "rust_tools"
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
        def r1_gz = r1.toString().endsWith(".gz")
        def r2_gz = r2.toString().endsWith(".gz")
        def r1ExtractCmd = r1_gz ? "zcat" : "cat"
        def r2ExtractCmd = r2_gz ? "zcat" : "cat"
        def empty_match_cmd   = keep_match   ? "gzip -c < /dev/null > ${match_out}"   : ""
        def empty_nomatch_cmd = keep_nomatch ? "gzip -c < /dev/null > ${nomatch_out}" : ""
        def in1_path = r1_gz ? "\${tmpdir}/in1.fifo" : "${r1}"
        def in2_path = r2_gz ? "\${tmpdir}/in2.fifo" : "${r2}"
        def match_target   = keep_match   ? "\${tmpdir}/match.fifo"   : "/dev/null"
        def nomatch_target = keep_nomatch ? "\${tmpdir}/nomatch.fifo" : "/dev/null"
        def fifo_cmds = []
        def pigz_cmds = []
        def wait_cmds = []
        // Input pigz: nucleaze's built-in gz decompression is single-threaded
        // and becomes the limiting stage on gz inputs; pre-decompress via a
        // pigz FIFO. Cap at 2 threads — ordinary gz can't be inflated faster.
        if (r1_gz) {
            fifo_cmds << "mkfifo \"\${tmpdir}/in1.fifo\""
            pigz_cmds << "pigz -dc -p 2 < ${r1} > \"\${tmpdir}/in1.fifo\" & DEC1=\$!"
            wait_cmds << "wait \"\${DEC1}\""
        }
        if (r2_gz) {
            fifo_cmds << "mkfifo \"\${tmpdir}/in2.fifo\""
            pigz_cmds << "pigz -dc -p 2 < ${r2} > \"\${tmpdir}/in2.fifo\" & DEC2=\$!"
            wait_cmds << "wait \"\${DEC2}\""
        }
        // Output pigz: -1 (fast level) so pigz keeps up with nucleaze.
        if (keep_match) {
            fifo_cmds << "mkfifo \"\${tmpdir}/match.fifo\""
            pigz_cmds << "pigz -p ${task.cpus} -1 < \"\${tmpdir}/match.fifo\"   > ${match_out}   & PIGZ_M=\$!"
            wait_cmds << "wait \"\${PIGZ_M}\""
        }
        if (keep_nomatch) {
            fifo_cmds << "mkfifo \"\${tmpdir}/nomatch.fifo\""
            pigz_cmds << "pigz -p ${task.cpus} -1 < \"\${tmpdir}/nomatch.fifo\" > ${nomatch_out} & PIGZ_U=\$!"
            wait_cmds << "wait \"\${PIGZ_U}\""
        }
        def fifo_block = fifo_cmds.join("\n            ")
        def pigz_block = pigz_cmds.join("\n            ")
        def wait_block = wait_cmds.join("\n            ")
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
            # Named FIFOs (not process substitution):
            #   - Output: `>(pigz ...)` hides the subshell PID, so the script
            #     can exit mid-trailer and truncate the gzip.
            #   - Input: a pigz-decoder errexit is observable via wait rather
            #     than swallowed by the subshell.
            tmpdir=\$(mktemp -d)
            trap 'rm -rf "\${tmpdir}"' EXIT
            ${fifo_block}
            ${pigz_block}
            nucleaze \
                --binref ${index} \
                --in ${in1_path} \
                --in2 ${in2_path} \
                --outm ${match_target} \
                --outu ${nomatch_target} \
                --k ${params_map.k} \
                --minhits ${params_map.minhits} \
                --canonical \
                --threads ${task.cpus} \
                2>&1 | tee ${stats}
            ${wait_block}
        fi
        ln -s ${r1} input_${r1}
        ln -s ${r2} input_${r2}
        """
}
