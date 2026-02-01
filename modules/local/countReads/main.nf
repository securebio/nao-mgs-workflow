process COUNT_READS {
    label "coreutils_gzip_gawk"
    label "single"
    input:
        tuple val(sample), path(reads)
        val(single_end)
    output:
        tuple val(sample), path("${sample}_read_counts.tsv"), emit: output
        tuple val(sample), path("${sample}_reads_in.fastq.gz"), emit: input
    script:
        def readFile = single_end ? reads : reads[0]
        def extractCmd = readFile.toString().endsWith(".gz") ? "zcat" : "cat"
        """
        set -eou pipefail
        READS=${readFile}
        # First check if file is empty (before trying to decompress)
        if [ ! -s \${READS} ]; then
            COUNT=0 # File is completely empty, set count to 0
        else
            # File has content - try to count lines
            # This will fail if file is corrupted
            LINECOUNT=\$(${extractCmd} \${READS} | wc -l)
            if [ \${LINECOUNT} -eq 0 ]; then
                COUNT=0 # File has content but no lines (e.g., gzip header only)
            else
                COUNT=\$(awk -v count=\${LINECOUNT} 'BEGIN {print count / 4}')
            fi
        fi
        # Convert raw count to single and paired counts
        COUNT_SINGLE=${single_end ? '${COUNT}' : '$(awk -v count=\${COUNT} \'BEGIN {print count * 2}\')'}
        COUNT_PAIR=${single_end ? 'NA' : '${COUNT}'}
        # Add header
        echo -e "sample\\tn_reads_single\\tn_read_pairs" > ${sample}_read_counts.tsv
        # Add sample and count
        echo -e "${sample}\\t\${COUNT_SINGLE}\\t\${COUNT_PAIR}" >> ${sample}_read_counts.tsv
        # Link output to input for tests
        ln -s \${READS} ${sample}_reads_in.fastq.gz
        """
    }
