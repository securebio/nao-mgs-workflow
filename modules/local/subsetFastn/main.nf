// Subsample single or interleaved FASTQ / FASTA based on read IDs
process SUBSET_FASTN {
    label "seqkit"
    label "single"
    label "testing_only" // Process is currently only used for testing
    input:
        tuple val(sample), path(fastn)
        val readFraction // NB: will round to 2 decimal places
        val randomSeed
    output:
        tuple val(sample), path("subset_${fastn}"), emit: output
        tuple val(sample), path("input_${fastn}"), emit: input
    script:
        def out = "subset_${fastn}"
        def outBase = out.replaceAll(/\.gz$/, "")
        def in_file = "input_${fastn}"
        def rfOutside = readFraction.toFloat() > 1 || readFraction.toFloat() < 0
        def rpc = Math.round(readFraction.toFloat() * 100)
        def rseed = randomSeed == "" ? '\$RANDOM' : randomSeed
        """
        # Check the read fraction is between 0 and 1 inclusive
        if [[ ${rfOutside} == "true" ]]; then
            echo "ERROR: Read fraction must be between 0 and 1 (got ${readFraction})."
            exit 1
        fi
        # Handle special cases where fraction equals 0 or 1
        if [[ ${readFraction.toFloat() == 1} == "true" ]]; then
            echo "Read fraction equals 1. Copying input to output without sampling."
            cp ${fastn} ${out}
            ln -s ${fastn} ${in_file}
            exit 0
        elif [[ ${readFraction.toFloat() == 0} == "true" ]]; then
            echo "Read fraction equals 0. Creating empty output file."
            if [[ ${fastn.toString().endsWith(".gz")} == "true" ]]; then
                touch ${outBase}
                gzip ${outBase}
            else
                touch ${out}
            fi
            ln -s ${fastn} ${in_file}
            exit 0
        fi
        # Get unique read IDs from input file
        seqkit seq -ni ${fastn} | sort -u > all_ids.txt
        echo "Input IDs: \$(cat all_ids.txt | wc -l)"
        # Subsample IDs
        echo "Target read percentage: ${rpc}%"
        echo "Random seed: ${rseed}"
        RANDOM=${rseed}
        while IFS= read -r line; do
            rand_val=\$((RANDOM % 100))
            threshold=${rpc}
            if (( rand_val < threshold )); then
                echo \${line} >> sample_ids.txt
            fi
        done < all_ids.txt
        echo "Output IDs: \$(cat sample_ids.txt | wc -l)"
        # Subsample sequences based on IDs
        seqkit grep -f sample_ids.txt ${fastn} | ${fastn.toString().endsWith(".gz") ? 'gzip -c' : 'cat'} > ${out}
        ln -s ${fastn} ${in_file}
        """
}
