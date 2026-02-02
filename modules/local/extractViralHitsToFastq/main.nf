// Given a gzipped TSV of viral hits, extract the seq_ids and use them to subseq an interleaved FASTQ file
process EXTRACT_VIRAL_HITS_TO_FASTQ {
    label "seqtk"
    label "single_cpu_16GB_memory"
    input:
        tuple val(sample), path(hits_tsv), path(fastq)
    output:
        tuple val(sample), path("${sample}_virus_hits_final.fastq.gz"), emit: fastq
        tuple val(sample), path("${sample}_virus_hits_ids.txt.gz"), emit: ids
        tuple val(sample), path("${sample}_virus_hits_in.tsv.gz"), emit: input_tsv
        tuple val(sample), path("${sample}_virus_reads_in.fastq.gz"), emit: input_fastq
    shell:
        '''
        set -euo pipefail
        
        # Check if the TSV file is empty (only whitespace or empty)
        if [ $(zcat !{hits_tsv} | wc -l) -eq 0 ]; then
            echo "Warning: Input TSV file is empty. Creating empty output files."
            touch empty.txt
            gzip -c empty.txt > !{sample}_virus_hits_ids.txt.gz
            gzip -c empty.txt > !{sample}_virus_hits_final.fastq.gz
            rm empty.txt
        else
            zcat !{hits_tsv} \\
                | awk -F'\t' '
                    BEGIN { colIndex = -1 }
                    NR == 1 {
                        for (i = 1; i <= NF; i++) { if ($i == "seq_id") { colIndex = i; break } }
                        if (colIndex == -1) {
                            print "ERROR: No column named 'seq_id' in header" > "/dev/stderr"
                            exit 1
                        } else {
                            print "'seq_id' column index: $colIndex" > "/dev/stderr"
                        }
                        next # Skip printing the header itself
                    }
                    { print $colIndex } # Print the seq_id column only from subsequent lines
                ' \\
                | tee >(gzip -c > !{sample}_virus_hits_ids.txt.gz) \\
                | seqtk subseq !{fastq} - \\
                | gzip -c > !{sample}_virus_hits_final.fastq.gz
        fi
        # Link input to output for testing
        ln -s !{hits_tsv} !{sample}_virus_hits_in.tsv.gz
        ln -s !{fastq} !{sample}_virus_reads_in.fastq.gz
        '''
}
