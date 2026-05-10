// Subsample paired reads and interleave in a single combined pass.
// Sampling and interleaving run concurrently via FIFOs, so R1 and R2 are each
// read once. Uses pigz for parallel (de)compression and seqtk for sampling.
// Input read count is provided by an upstream COUNT_READS task, eliminating
// the count pass that the legacy module performed internally.
process SUBSET_READS_PAIRED_TARGET {
    label "seqtk"
    label "xsmall"
    input:
        tuple val(sample), path(reads), path(counts_tsv)
        val readTarget
        val randomSeed
    output:
        tuple val(sample), path("${sample}_interleaved.fastq.gz"), emit: output
        tuple val(sample), path("input_*"), emit: input
    script:
        def in1 = reads[0]
        def in2 = reads[1]
        def out = "${sample}_interleaved.fastq.gz"
        def extractCmd = in1.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        """
        set -euo pipefail
        # Read count comes from COUNT_READS (n_read_pairs column, second row)
        n_reads=\$(awk -F'\\t' 'NR==2 {print \$3}' ${counts_tsv})
        echo "Input read pairs: \${n_reads}"
        echo "Target read pairs: ${readTarget}"
        if (( \${n_reads} <= ${readTarget} )); then
            echo "Target larger than input; passing through all reads (interleaved)."
            paste <(${extractCmd} ${in1} | paste - - - -) \\
                  <(${extractCmd} ${in2} | paste - - - -) \\
              | tr '\\t' '\\n' \\
              | pigz -p ${task.cpus} -1 > ${out}
        else
            frac=\$(awk -v a=\${n_reads} -v b=${readTarget} 'BEGIN {r = b/a; print (r > 1) ? 1.0 : r}')
            echo "Read fraction for subsetting: \${frac}"
            rseed=${randomSeed == "" ? "\$RANDOM" : randomSeed}
            echo "Random seed: \${rseed}"
            # Sample R1 and R2 in parallel via FIFOs; interleave the outputs on the fly
            mkfifo r1.fq r2.fq
            ${extractCmd} ${in1} | seqtk sample -s \${rseed} - \${frac} > r1.fq &
            ${extractCmd} ${in2} | seqtk sample -s \${rseed} - \${frac} > r2.fq &
            paste <(paste - - - - < r1.fq) <(paste - - - - < r2.fq) \\
              | tr '\\t' '\\n' \\
              | pigz -p ${task.cpus} -1 > ${out}
            wait
        fi
        # Link input to output for testing
        ln -s ${in1} input_${in1}
        ln -s ${in2} input_${in2}
        """
}

// Subsample single-end reads. Input read count is provided by an upstream
// COUNT_READS task; uses pigz for parallel (de)compression.
process SUBSET_READS_SINGLE_TARGET {
    label "seqtk"
    label "xsmall"
    input:
        tuple val(sample), path(reads), path(counts_tsv)
        val readTarget
        val randomSeed
    output:
        tuple val(sample), path("subset_${reads}"), emit: output
        tuple val(sample), path("input_${reads}"), emit: input
    script:
        def in1 = reads
        def out1 = "subset_${reads}"
        def extractCmd = in1.toString().endsWith(".gz") ? "pigz -dc -p ${task.cpus}" : "cat"
        def compressCmd = in1.toString().endsWith(".gz") ? "pigz -p ${task.cpus} -1" : "cat"
        """
        set -euo pipefail
        # Read count comes from COUNT_READS (n_reads_single column, second row)
        n_reads=\$(awk -F'\\t' 'NR==2 {print \$2}' ${counts_tsv})
        echo "Input reads: \${n_reads}"
        echo "Target reads: ${readTarget}"
        if (( \${n_reads} <= ${readTarget} )); then
            echo "Target larger than input; returning all reads."
            cp ${in1} ${out1}
        else
            frac=\$(awk -v a=\${n_reads} -v b=${readTarget} 'BEGIN {r = b/a; print (r > 1) ? 1.0 : r}')
            echo "Read fraction for subsetting: \${frac}"
            rseed=${randomSeed == "" ? "\$RANDOM" : randomSeed}
            echo "Random seed: \${rseed}"
            ${extractCmd} ${in1} | seqtk sample -s \${rseed} - \${frac} | ${compressCmd} > ${out1}
        fi
        # Link input to output for testing
        ln -s ${in1} input_${in1}
        """
}
