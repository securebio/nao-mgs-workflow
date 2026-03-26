// Mask gzipped FASTA file with Dustmasker
process DUSTMASKER_FASTA_GZIPPED {
    label "BLAST"
    label "single"
    input:
        path(fasta_gzipped)
    output:
        path("masked.fasta.gz")
    script:
        """
        zcat -f ${fasta_gzipped} | dustmasker -out "masked.fasta" -outfmt fasta
        sed -i '/^>/!s/[a-z]/x/g' masked.fasta
        gzip masked.fasta
        """
}
