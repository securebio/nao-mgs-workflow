// Subsample paired reads and write a single interleaved FASTQ. R1 and R2 are
// sampled in parallel through FIFOs and merged on the fly with `seqtk mergepe`.
// The read count comes from an upstream COUNT_READS task.
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
        // Split allocated cpus across the two parallel input pipelines.
        def pigz_per_side = Math.max(1, (task.cpus as int) / 2 as int)
        def extractCmd = in1.toString().endsWith(".gz") ? "pigz -dc -p ${pigz_per_side}" : "cat"
        """
        set -euo pipefail
        # n_read_pairs from COUNT_READS (column 3, second row)
        n_reads=\$(awk -F'\\t' 'NR==2 {print \$3}' ${counts_tsv})
        echo "Input read pairs: \${n_reads}"
        echo "Target read pairs: ${readTarget}"
        if (( \${n_reads} <= ${readTarget} )); then
            echo "Target larger than input; passing through all reads."
            mkfifo r1.fq r2.fq
            ${extractCmd} ${in1} > r1.fq &
            ${extractCmd} ${in2} > r2.fq &
            seqtk mergepe r1.fq r2.fq | pigz -p ${pigz_per_side} -1 > ${out}
            wait
        else
            frac=\$(awk -v a=\${n_reads} -v b=${readTarget} 'BEGIN {r = b/a; print (r > 1) ? 1.0 : r}')
            echo "Read fraction for subsetting: \${frac}"
            rseed=${randomSeed == "" ? "\$RANDOM" : randomSeed}
            echo "Random seed: \${rseed}"
            mkfifo r1.fq r2.fq
            ${extractCmd} ${in1} | seqtk sample -s \${rseed} - \${frac} > r1.fq &
            ${extractCmd} ${in2} | seqtk sample -s \${rseed} - \${frac} > r2.fq &
            seqtk mergepe r1.fq r2.fq | pigz -p ${pigz_per_side} -1 > ${out}
            wait
        fi
        # Link input to output for testing
        ln -s ${in1} input_${in1}
        ln -s ${in2} input_${in2}
        """
}

// Subsample single-end reads with seqtk. Read count comes from an upstream
// COUNT_READS task; pigz handles parallel (de)compression.
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
        # n_reads_single from COUNT_READS (column 2, second row)
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
