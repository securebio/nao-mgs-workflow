// Subsample reads with seqtk with an autocomputed read fraction (paired-end)
process SUBSET_READS_PAIRED_TARGET {
    label "seqtk"
    label "single"
    input:
        tuple val(sample), path(reads)
        val readTarget
        val randomSeed
    output:
        tuple val(sample), path("subset_*"), emit: output
        tuple val(sample), path("input_*"), emit: input
    script:
        def in1 = reads[0]
        def in2 = reads[1]
        def out1 = "subset_${reads[0]}"
        def out2 = "subset_${reads[1]}"
        def extractCmd = in1.toString().endsWith(".gz") ? "zcat" : "cat"
        def compressCmd = in1.toString().endsWith(".gz") ? "gzip" : "cat"
        """
        # Count reads and compute target fraction
        n_reads=\$(${extractCmd} ${in1} | wc -l | awk '{ print \$1/4 }')
        echo "Input reads: \${n_reads}"
        echo "Target reads: ${readTarget}"
        if (( \${n_reads} <= ${readTarget} )); then
            echo "Target larger than input; returning all reads."
            cp ${in1} ${out1}
            cp ${in2} ${out2}
        else
            frac=\$(awk -v a=\${n_reads} -v b=${readTarget} 'BEGIN {result = b/a; print (result > 1) ? 1.0 : result}')
            echo "Read fraction for subsetting: \${frac}"
            # Carry out subsetting
            rseed=${randomSeed == "" ? "\$RANDOM" : randomSeed}
            echo "Random seed: \${rseed}"
            seqtk sample -s \${rseed} ${in1} \${frac} | ${compressCmd} > ${out1}
            seqtk sample -s \${rseed} ${in2} \${frac} | ${compressCmd} > ${out2}
        fi
        # Count reads for validation
        echo "Output reads: \$(${extractCmd} ${out1} | wc -l | awk '{ print \$1/4 }')"
        # Link input to output for testing
        ln -s ${in1} input_${in1}
        ln -s ${in2} input_${in2}
        """
}

// Subsample reads with seqtk with an autocomputed read fraction (single-end)
process SUBSET_READS_SINGLE_TARGET {
    label "seqtk"
    label "single"
    input:
        tuple val(sample), path(reads)
        val readTarget
        val randomSeed
    output:
        tuple val(sample), path("subset_${reads}"), emit: output
        tuple val(sample), path("input_${reads}"), emit: input
    script:
        def in1 = reads
        def out1 = "subset_${reads}"
        def extractCmd = in1.toString().endsWith(".gz") ? "zcat" : "cat"
        def compressCmd = in1.toString().endsWith(".gz") ? "gzip" : "cat"
        """
        # Count reads and compute target fraction
        n_reads=\$(${extractCmd} ${in1} | wc -l | awk '{ print \$1/4 }')
        echo "Input reads: \${n_reads}"
        echo "Target reads: ${readTarget}"
        if (( \${n_reads} <= ${readTarget} )); then
            echo "Target larger than input; returning all reads."
            cp ${in1} ${out1}
        else
            frac=\$(awk -v a=\${n_reads} -v b=${readTarget} 'BEGIN {result = b/a; print (result > 1) ? 1.0 : result}')
            echo "Read fraction for subsetting: \${frac}"
            # Carry out subsetting
            rseed=${randomSeed == "" ? "\$RANDOM" : randomSeed}
            echo "Random seed: \${rseed}"
            seqtk sample -s \${rseed} ${in1} \${frac} | ${compressCmd} > ${out1}
        fi
        # Count reads for validation
        echo "Output reads: \$(${extractCmd} ${out1} | wc -l | awk '{ print \$1/4 }')"
        # Link input to output for testing
        ln -s ${in1} input_${in1}
        """
}
