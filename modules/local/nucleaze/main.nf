// K-mer screen paired-end reads against a pre-built nucleaze index.
// Outputs are interleaved gzipped FASTQs (match / nomatch).
//
// params_map.keep_match and params_map.keep_nomatch (booleans, default true)
// gate the corresponding output. Compressing the unwanted side is a large
// fraction of process wall time when one side dominates (e.g. nomatch in
// the viral-filtering pipeline), so dropping it is worth doing when the
// caller doesn't need it. At least one must be true.
process NUCLEAZE {
    label "small"
    label "rust_tools"
    input:
        tuple val(sample), path(reads)   // reads is [R1.fastq, R2.fastq]
        path(index)                      // Pre-built binary index
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
        def r1ExtractCmd = r1.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        def r2ExtractCmd = r2.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        // Emit unwanted-side empty files / FIFOs only when kept.
        def empty_match_cmd   = keep_match   ? "gzip -c < /dev/null > ${match_out}"   : ""
        def empty_nomatch_cmd = keep_nomatch ? "gzip -c < /dev/null > ${nomatch_out}" : ""
        def match_target   = keep_match   ? "\${tmpdir}/match.fifo"   : "/dev/null"
        def nomatch_target = keep_nomatch ? "\${tmpdir}/nomatch.fifo" : "/dev/null"
        // Build the mkfifo + pigz-worker + wait blocks only for kept sides.
        def fifo_cmds = []
        def pigz_cmds = []
        def wait_cmds = []
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
        // Omitting --outu2/--outm2 causes nucleaze to auto-interleave paired output
        """
        set -euo pipefail
        # nucleaze does not create output files when both R1 and R2 are empty,
        # so short-circuit that case by emitting empty outputs ourselves
        # (only for sides the caller is keeping).
        r1_first=\$(${r1ExtractCmd} ${r1} | head -c 1 || true)
        r2_first=\$(${r2ExtractCmd} ${r2} | head -c 1 || true)
        if [[ -z "\${r1_first}" && -z "\${r2_first}" ]]; then
            >&2 echo "Warning: Both input read files are empty. Creating empty output files."
            ${empty_match_cmd}
            ${empty_nomatch_cmd}
            echo "No data - empty input files" > ${stats}
        else
            # Stream nucleaze's outputs straight into pigz workers via named FIFOs,
            # so the heavy output FASTQ never lands uncompressed on disk and
            # compression overlaps fully with screening.
            #   - pigz -1 (fast level) keeps up with nucleaze's output rate; default
            #     -6 back-pressures nucleaze and inflates its processing time ~3x.
            #   - Named FIFOs + `wait \$PID` are required (rather than bash
            #     `>(pigz ...)` process substitution): `>(...)` does not expose its
            #     subshell PID, so the script can exit before pigz drains the pipe
            #     and silently truncate the gzip trailer.
            #   - Dropped sides bypass the FIFO entirely and point nucleaze at
            #     /dev/null, so we never pay compression cost for output the
            #     caller doesn't keep.
            tmpdir=\$(mktemp -d)
            trap 'rm -rf "\${tmpdir}"' EXIT
            ${fifo_block}
            ${pigz_block}
            nucleaze \
                --binref ${index} \
                --in ${r1} \
                --in2 ${r2} \
                --outm ${match_target} \
                --outu ${nomatch_target} \
                --k ${params_map.k} \
                --minhits ${params_map.minhits} \
                --canonical \
                --threads ${task.cpus} \
                2>&1 | tee ${stats}
            ${wait_block}
        fi
        # Symlink inputs so they are captured as process outputs for read-conservation checks
        ln -s ${r1} input_${r1}
        ln -s ${r2} input_${r2}
        """
}
