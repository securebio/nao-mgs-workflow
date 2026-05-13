// K-mer screen paired-end reads against a pre-built nucleaze index.
// Outputs are interleaved gzipped FASTQs (match / nomatch).
process NUCLEAZE {
    label "small"
    label "rust_tools"
    input:
        tuple val(sample), path(reads)   // reads is [R1.fastq, R2.fastq]
        path(index)                      // Pre-built binary index
        val(params_map)                  // k, minhits, suffix
    output:
        tuple val(sample), path("input_{${reads[0]},${reads[1]}}"), emit: input
        tuple val(sample), path("${sample}_${params_map.suffix}_nucleaze_nomatch.fastq.gz"), emit: nomatch
        tuple val(sample), path("${sample}_${params_map.suffix}_nucleaze_match.fastq.gz"), emit: match
        tuple val(sample), path("${sample}_${params_map.suffix}_nucleaze.stats.txt"), emit: log
    script:
        def r1 = reads[0]
        def r2 = reads[1]
        def nomatch_out = "${sample}_${params_map.suffix}_nucleaze_nomatch.fastq.gz"
        def match_out = "${sample}_${params_map.suffix}_nucleaze_match.fastq.gz"
        def stats = "${sample}_${params_map.suffix}_nucleaze.stats.txt"
        def r1ExtractCmd = r1.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        def r2ExtractCmd = r2.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        // Omitting --outu2/--outm2 causes nucleaze to auto-interleave paired output
        """
        set -euo pipefail
        # nucleaze does not create output files when both R1 and R2 are empty,
        # so short-circuit that case by emitting empty outputs ourselves.
        r1_first=\$(${r1ExtractCmd} ${r1} | head -c 1 || true)
        r2_first=\$(${r2ExtractCmd} ${r2} | head -c 1 || true)
        if [[ -z "\${r1_first}" && -z "\${r2_first}" ]]; then
            >&2 echo "Warning: Both input read files are empty. Creating empty output files."
            gzip -c < /dev/null > ${nomatch_out}
            gzip -c < /dev/null > ${match_out}
            echo "No data - empty input files" > ${stats}
        else
            # Stream nucleaze's outputs straight into pigz workers via named FIFOs,
            # so the heavy nomatch FASTQ never lands uncompressed on disk and
            # compression overlaps fully with screening.
            #   - pigz -1 (fast level) keeps up with nucleaze's output rate; default
            #     -6 back-pressures nucleaze and inflates its processing time ~3x.
            #   - Named FIFOs + `wait \$PID` are required (rather than bash
            #     `>(pigz ...)` process substitution): `>(...)` does not expose its
            #     subshell PID, so the script can exit before pigz drains the pipe
            #     and silently truncate the gzip trailer.
            tmpdir=\$(mktemp -d)
            trap 'rm -rf "\${tmpdir}"' EXIT
            mkfifo "\${tmpdir}/match.fifo" "\${tmpdir}/nomatch.fifo"
            pigz -p ${task.cpus} -1 < "\${tmpdir}/match.fifo"   > ${match_out}   & PIGZ_M=\$!
            pigz -p ${task.cpus} -1 < "\${tmpdir}/nomatch.fifo" > ${nomatch_out} & PIGZ_U=\$!
            nucleaze \
                --binref ${index} \
                --in ${r1} \
                --in2 ${r2} \
                --outm "\${tmpdir}/match.fifo" \
                --outu "\${tmpdir}/nomatch.fifo" \
                --k ${params_map.k} \
                --minhits ${params_map.minhits} \
                --canonical \
                --threads ${task.cpus} \
                2>&1 | tee ${stats}
            wait "\${PIGZ_M}"
            wait "\${PIGZ_U}"
        fi
        # Symlink inputs so they are captured as process outputs for read-conservation checks
        ln -s ${r1} input_${r1}
        ln -s ${r2} input_${r2}
        """
}
