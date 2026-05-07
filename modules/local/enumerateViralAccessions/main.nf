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
        // Header schema matches the previous DOWNLOAD_VIRAL_GENOMES.metadata
        // output so downstream consumers (FILTER_VIRAL_GENBANK_METADATA,
        // PREPARE_VIRAL_METADATA) can be reused without changes.
        def metadata_header = "assembly_accession\\ttaxid\\torganism_name\\tsource_database\\tassembly_status"
        """
        # 1. Enumerate all assemblies under ${taxid} via `datasets summary`.
        # Unlike the per-child download module, no empty-taxon fallback is needed:
        # this is always called on the viral root (or download_virus_taxid),
        # which by construction has assemblies. Failures should be loud.
        datasets summary genome taxon ${taxid} \\
            --assembly-source ${assembly_source} \\
            --as-json-lines \\
            ${extra_args} \\
            > assembly_data_report.jsonl

        # 2. Convert to TSV with the same column set DOWNLOAD_VIRAL_GENOMES used.
        # `assminfo-status` is included so FILTER_VIRAL_GENBANK_METADATA can drop
        # non-current assemblies (the `datasets` `--assembly-version` arg is
        # broken; see ncbi/datasets#576).
        dataformat tsv genome \\
            --inputfile assembly_data_report.jsonl \\
            --fields accession,organism-tax-id,organism-name,source_database,assminfo-status \\
            > raw_metadata.tsv

        # 3. Replace header with standardized column names.
        { echo -e "${metadata_header}"
          tail -n +2 raw_metadata.tsv
        } > metadata.tsv
        rm -f assembly_data_report.jsonl raw_metadata.tsv
        echo "Enumerated \$((  \$(wc -l < metadata.tsv) - 1  )) assemblies for taxid ${taxid}"
        """
}
