// Extract target reads, based on the shared read ids in another FASTQ file. Only works on gzipped FASTQ files.
process EXTRACT_SHARED_FASTQ_READS {
    label "seqtk"
    label "single"
    input:
        tuple val(sample), path(fastq_1), path(fastq_2)
    output:
        tuple val(sample), path("${sample}_shared.fastq.gz"), emit: output
        tuple val(sample), path("${sample}_fastq_1.fastq.gz"), path("${sample}_fastq_2.fastq.gz"), emit: input
    script:
        """
        set -euo pipefail

        # Get target read ids
        seqtk comp ${fastq_1} | cut -f 1 > fastq_1_ids.txt

        # Create FASTQ file with cleaned, mapped reads
        seqtk subseq ${fastq_2} fastq_1_ids.txt | gzip -c > "${sample}_shared.fastq.gz"

        # Link input to output for testing
        ln -s ${fastq_1} ${sample}_fastq_1.fastq.gz
        ln -s ${fastq_2} ${sample}_fastq_2.fastq.gz
        """
}
