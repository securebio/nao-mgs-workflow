// Subsample reads with seqtk
process SUBSET_READS_PAIRED {
    label "seqtk"
    label "single"
    input:
        tuple val(sample), path(reads)
        val readFraction
        val suffix
    output:
        tuple val(sample), path("${sample}_subset_{1,2}.${suffix}.gz")
    shell:
        '''
        # Define input/output
        in1=!{reads[0]}
        in2=!{reads[1]}
        out1=!{sample}_subset_1.!{suffix}.gz
        out2=!{sample}_subset_2.!{suffix}.gz
        # Count reads for validation
        echo "Input reads: $(zcat ${in1} | wc -l | awk '{ print $1/4 }')"
        # Carry out subsetting
        seed=${RANDOM}
        seqtk sample -s ${seed} ${in1} !{readFraction} | gzip -c > ${out1}
        seqtk sample -s ${seed} ${in2} !{readFraction} | gzip -c > ${out2}
        # Count reads for validation
        echo "Output reads: $(zcat ${out1} | wc -l | awk '{ print $1/4 }')"
        '''
}

// Subsample reads with seqtk (single-end)
process SUBSET_READS_SINGLE {
    label "seqtk"
    label "single"
    input:
        tuple val(sample), path(reads)
        val readFraction
        val suffix
    output:
        tuple val(sample), path("${sample}_subset.${suffix}.gz")
    shell:
        '''
        # Define input/output
        in=!{reads}
        out=!{sample}_subset.!{suffix}.gz
        # Count reads for validation
        echo "Input reads: $(zcat ${in} | wc -l | awk '{ print $1/4 }')"
        # Carry out subsetting
        seed=${RANDOM}
        seqtk sample -s ${seed} ${in} !{readFraction} | gzip -c > ${out}
        # Count reads for validation
        echo "Output reads: $(zcat ${out} | wc -l | awk '{ print $1/4 }')"
        '''
}

// Subsample reads with seqtk (no sample name)
process SUBSET_READS_PAIRED_MERGED {
    label "seqtk"
    label "single"
    input:
        path(reads)
        val readFraction
        val suffix
    output:
        path("reads_subset_{1,2}.${suffix}.gz")
    shell:
        '''
        # Define input/output
        in1=!{reads[0]}
        in2=!{reads[1]}
        out1=reads_subset_1.!{suffix}.gz
        out2=reads_subset_2.!{suffix}.gz
        # Count reads for validation
        echo "Input reads: $(zcat ${in1} | wc -l | awk '{ print $1/4 }')"
        # Carry out subsetting
        seed=${RANDOM}
        seqtk sample -s ${seed} ${in1} !{readFraction} | gzip -c > ${out1}
        seqtk sample -s ${seed} ${in2} !{readFraction} | gzip -c > ${out2}
        # Count reads for validation
        echo "Output reads: $(zcat ${out1} | wc -l | awk '{ print $1/4 }')"
        '''
}

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
        val suffix
        val randomSeed
    output:
        tuple val(sample), path("${sample}_subset.${suffix}.gz")
    shell:
        '''
        # Define input/output
        in=!{reads}
        out=!{sample}_subset.!{suffix}.gz
        # Count reads and compute target fraction
        n_reads=$(zcat ${in} | wc -l | awk '{ print $1/4 }')
        echo "Input reads: ${n_reads}"
        echo "Target reads: !{readTarget}"
        if (( ${n_reads} <= !{readTarget} )); then
            echo "Target larger than input; returning all reads."
            cp ${in} ${out}
        else
            frac=$(awk -v a=${n_reads} -v b=!{readTarget} 'BEGIN {result = b/a; print (result > 1) ? 1.0 : result}')
            echo "Read fraction for subsetting: ${frac}"
            # Carry out subsetting
            rseed=!{randomSeed == "" ? "\\$RANDOM" : randomSeed}
            echo "Random seed: ${rseed}"
            seqtk sample -s ${rseed} ${in} ${frac} | gzip -c > ${out}
        fi
        # Count reads for validation
        echo "Output reads: $(zcat ${out} | wc -l | awk '{ print $1/4 }')"
        '''
}
