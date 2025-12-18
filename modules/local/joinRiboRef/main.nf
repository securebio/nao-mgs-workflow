// Process & concatenate ribosomal references
process JOIN_RIBO_REF {
    label "BBTools"
    label "single"
    input:
        path(ssu_ref)
        path(lsu_ref)
    output:
        path("ribo-ref-concat.fasta.gz"), emit: ribo_ref
        tuple path("ssu_ref_input.*"), path("lsu_ref_input.*"), emit: input
    script:
        def ssuExtractCmd = ssu_ref.toString().endsWith(".gz") ? "zcat" : "cat"
        def lsuExtractCmd = lsu_ref.toString().endsWith(".gz") ? "zcat" : "cat"
        """
        # Add suffixes to reference headers
        ${ssuExtractCmd} ${ssu_ref} | awk -v suffix=ssu '
            /^>/ {
                pos = index(\$0, " ")
                print (pos > 0) ? substr(\$0,1,pos-1) "::" toupper(suffix) substr(\$0,pos) : \$0 "::" toupper(suffix)
                next
            }
            { print }' | gzip > ssu_ref_suffix.fasta.gz

        ${lsuExtractCmd} ${lsu_ref} | awk -v suffix=lsu '
            /^>/ {
                pos = index(\$0, " ")
                print (pos > 0) ? substr(\$0,1,pos-1) "::" toupper(suffix) substr(\$0,pos) : \$0 "::" toupper(suffix)
                next
            }
            { print }' | gzip > lsu_ref_suffix.fasta.gz

        # Concatenate files
        cat ssu_ref_suffix.fasta.gz lsu_ref_suffix.fasta.gz > ribo-ref-concat.fasta.gz

        # Return input files for testing
        ln -s ${ssu_ref} ssu_ref_input.${ssu_ref}
        ln -s ${lsu_ref} lsu_ref_input.${lsu_ref}
        """
}
