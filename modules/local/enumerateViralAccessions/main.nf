// Enumerate all assembly accessions under a parent viral taxon, emitting a
// metadata TSV that downstream filtering uses to decide which accessions to
// download. Replaces per-child taxon enumeration + parallel download with a
// single up-front summary, so downstream FILTER + chunked download can apply
// host/assembly-status filters before any genome data is fetched.
process ENUMERATE_VIRAL_ACCESSIONS {
    label "ncbi_datasets"
    label "single"
    input:
        val(taxid)
        val(assembly_source)
        val(extra_args)
    output:
        path("metadata.tsv"), emit: metadata
    script:
        """
        # 1. Enumerate all assemblies under ${taxid} via `datasets summary`.
        # Taxids with no assemblies will hard fail at this step.
        datasets summary genome taxon ${taxid} \\
            --assembly-source ${assembly_source} \\
            --as-json-lines \\
            ${extra_args} \\
            > assembly_data_report.jsonl

        # 2. Convert to TSV; rewrite the header to standardized column names.
        # `assminfo-status` is included so FILTER_VIRAL_GENBANK_METADATA can drop
        # non-current assemblies (the `datasets` `--assembly-version` does not allow
        # this when using `--assembly-source all`; see ncbi/datasets#576).
        dataformat tsv genome \\
            --inputfile assembly_data_report.jsonl \\
            --fields accession,organism-tax-id,organism-name,source_database,assminfo-status \\
            | { printf 'assembly_accession\\ttaxid\\torganism_name\\tsource_database\\tassembly_status\\n'; tail -n +2; } \\
            > metadata.tsv
        rm -f assembly_data_report.jsonl
        echo "Enumerated \$((  \$(wc -l < metadata.tsv) - 1  )) assemblies for taxid ${taxid}"
        """
}
