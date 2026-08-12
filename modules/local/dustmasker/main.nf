// Mask gzipped FASTA file with Dustmasker
process DUSTMASKER_FASTA_GZIPPED {
    label "BLAST"
    label "single"
    tag "id=index"
    input:
        path(fasta_gzipped)
        val(name_pattern)
    output:
        path("${name_pattern}-dustmasked.fasta.gz")
    script:
        """
        zcat -f ${fasta_gzipped} | dustmasker -out "${name_pattern}-dustmasked.fasta" -outfmt fasta
        sed -i '/^>/!s/[a-z]/x/g' ${name_pattern}-dustmasked.fasta
        gzip ${name_pattern}-dustmasked.fasta
        """
}
