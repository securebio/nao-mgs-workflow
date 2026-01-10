// Merge read pairs into a single sequence
process BBMERGE {
    label "BBTools"
    label "small"
    input:
        tuple val(sample), path(reads_interleaved)
    output:
        tuple val(sample), path("${sample}_bbmerge_{merged,unmerged}.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_bbmerge_{stats,log}.txt"), emit: log
        tuple val(sample), path("input_${reads_interleaved}"), emit: input
    script:
        def ou = "${sample}_bbmerge_unmerged.fastq.gz"
        def om = "${sample}_bbmerge_merged.fastq.gz"
        def stats = "${sample}_bbmerge_stats.txt"
        def log = "${sample}_bbmerge_log.txt"
        def io = "in=stdin.fastq out=${om} outu=${ou} ihist=${stats}"
        def par = "join interleaved t=${task.cpus} -Xmx${task.memory.toGiga()}g"
        def extractCmd = reads_interleaved.toString().endsWith(".gz") ? "zcat" : "cat"
        def in2 = "input_${reads_interleaved}"
        """
        set -euo pipefail
        # Check if input file is empty or has zero reads
        if [[ ! -s ${reads_interleaved} ]] || [[ \$(${extractCmd} ${reads_interleaved} | head -c1 | wc -c) -eq 0 ]]; then
            echo "Warning: Input file is empty or contains no reads. Creating empty output files."
            # Create empty output files
            touch empty.fastq
            gzip -c empty.fastq > ${ou}
            gzip -c empty.fastq > ${om}
            echo "No data - empty input file" > ${stats}
            echo "Warning: Empty input file" > ${log}
            rm empty.fastq
        else
            # Normal processing for non-empty files
            ${extractCmd} ${reads_interleaved} \\
                | bbmerge.sh ${io} ${par} &> ${log}
            # Check for empty output files due to errors (only if input was not empty)
            if [[ ! -s ${ou} ]] && [[ ! -s ${om} ]]; then
                >&2 echo "Error: Empty output files from non-empty input. BBMerge failed."
                exit 1
            fi
        fi
        # Link input reads for testing
        ln -s ${reads_interleaved} ${in2}
        """
}

// Merge read pairs into a single sequence
process BBMERGE_LIST {
    label "BBTools"
    label "small"
    input:
        tuple val(sample), path(reads_interleaved)
    output:
        tuple val(sample), path("${sample}_*_bbmerge_merged.fastq.gz"), path("${sample}_*_bbmerge_unmerged.fastq.gz"), emit: reads
        tuple val(sample), path("${sample}_*_bbmerge_{stats,log}.txt"), emit: log
        tuple val(sample), path("${sample}_*_bbmerge_in.fastq.gz"), emit: input
    script:
        def extractCmd = reads_interleaved[0].toString().endsWith(".gz") ? "zcat" : "cat"
        """
        set -euo pipefail
        for reads in ${reads_interleaved}; do
            # Prepare inputs and outputs
            species=\$(basename \${reads} | grep -oP '${sample}_\\K\\d+(?=_)')
            if [ -z "\${species}" ]; then
                >&2 echo "Error: Could not extract species from filename: \${reads}"
                exit 1
            fi
            ou=${sample}_\${species}_bbmerge_unmerged.fastq.gz
            om=${sample}_\${species}_bbmerge_merged.fastq.gz
            stats=${sample}_\${species}_bbmerge_stats.txt
            log=${sample}_\${species}_bbmerge_log.txt

            # Check if input file is empty or has zero reads
            if [[ ! -s \${reads} ]] || [[ \$(${extractCmd} \${reads} | head -c1 | wc -c) -eq 0 ]]; then
                echo "Warning: Input file is empty or contains no reads. Creating empty output files."
                # Create empty output files
                touch empty.fastq
                gzip -c empty.fastq > \${ou}
                gzip -c empty.fastq > \${om}
                echo "No data - empty input file" > \${stats}
                echo "Warning: Empty input file" > \${log}
                rm empty.fastq
            else
                # Normal processing for non-empty files
                io="in=stdin.fastq out=\${om} outu=\${ou} ihist=\${stats}"
                par="join interleaved t=${task.cpus} -Xmx${task.memory.toGiga()}g"
                # Execute
                ${extractCmd} \${reads} | bbmerge.sh \${io} \${par} &> \${log}

                # Check for empty output files due to errors (only if input was not empty)
                if [[ ! -s \${ou} ]] && [[ ! -s \${om} ]]; then
                    >&2 echo "Error: Empty output files from non-empty input. BBMerge failed."
                    exit 1
                fi
            fi

            # Link input reads for testing
            ln -s \${reads} ${sample}_\${species}_bbmerge_in.fastq.gz
        done
        """
}
