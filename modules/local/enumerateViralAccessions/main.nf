// Enumerate all viral genome accessions under a parent viral taxon, emitting a
// metadata TSV that downstream filtering uses to decide which accessions to
// download. Two sourcing paths, selected by `source_type`:
//   - "assembly": NCBI genome assemblies (`datasets summary genome`), keyed by
//     GCA/GCF accession. This is the historical path.
//   - "sequence": NCBI Virus / nuccore sequence records (`datasets summary virus
//     genome`), keyed by nucleotide accession. NCBI stopped minting genome
//     assemblies for non-influenza viruses (~2025), so recent non-flu genomes
//     exist only as sequence records that the assembly path structurally cannot
//     see; this path recovers them.
// Both paths emit the same reconciled schema so the two can be unioned upstream.
process ENUMERATE_VIRAL_ACCESSIONS {
    label "ncbi_datasets"
    label "single"
    tag "id=index"
    input:
        val(taxid)
        val(assembly_source) // "genbank", "refseq", or "all"
        val(extra_args)
        val(source_type) // "assembly" or "sequence"
    output:
        path("virus-genome-metadata-raw.tsv"), emit: metadata
    script:
        // Single source of truth for the reconciled column header, so the two
        // branches cannot drift (their union is taken upstream).
        def schema_header = 'assembly_accession\\ttaxid\\torganism_name\\tsource_database\\tassembly_status\\trelease_date'
        if (source_type == "sequence") {
            // Map `assembly_source` onto the virus dataset's source filter. It
            // supports RefSeq-only (`--refseq`) but has no GenBank-only filter,
            // so for "genbank" we warn and enumerate all sources (RefSeq copies
            // are dropped later by cross-source dedup). Validate up front so a
            // typo fails fast rather than silently enumerating all sources (the
            // assembly branch gets this for free from `datasets --assembly-source`).
            def src = assembly_source.toLowerCase()
            if (!(src in ["refseq", "genbank", "all"])) {
                throw new IllegalArgumentException(
                    "ENUMERATE_VIRAL_ACCESSIONS: invalid assembly_source '${assembly_source}' (expected 'genbank', 'refseq', or 'all')")
            }
            def source_flag = src == "refseq" ? "--refseq" : ""
            def warn = src == "genbank" \
                ? '>&2 echo "Warning: sequence-based enumeration cannot filter for GenBank-only sequences; enumerating all sources for taxid ' + taxid + '."' \
                : "true"
            """
            # Fail loudly if any stage of the dataformat|awk pipe below errors
            # (Nextflow runs scripts under `bash -ue` without pipefail, so a
            # `dataformat` failure would otherwise be masked by a succeeding awk).
            set -o pipefail
            # 1. Enumerate sequence records under ${taxid} via NCBI Virus.
            # Taxids with no sequences will hard fail at this step.
            ${warn}
            datasets summary virus genome taxon ${taxid} \\
                ${source_flag} \\
                --as-json-lines \\
                ${extra_args} \\
                > virus_data_report.jsonl

            # 2. Convert to TSV and rewrite to the shared schema. Sequence rows
            # have no assembly status (empty column). `source_database` is
            # normalized to the assembly path's SOURCE_DATABASE_* vocabulary
            # (failing loudly on any other value so the contract can't silently
            # break), and the release date is truncated to YYYY-MM-DD (the virus
            # dataset emits a full ISO timestamp; the assembly path emits a bare
            # date). The header is printed once from the shared definition.
            {
                printf '${schema_header}\\n'
                dataformat tsv virus-genome \\
                    --inputfile virus_data_report.jsonl \\
                    --fields accession,virus-tax-id,virus-name,sourcedb,release-date \\
                    | awk 'BEGIN{FS=OFS="\\t"}
                        NR==1 {next}
                        {
                            db=\$4
                            if (db=="RefSeq") db="SOURCE_DATABASE_REFSEQ"
                            else if (db=="GenBank") db="SOURCE_DATABASE_GENBANK"
                            else {print "ERROR: unexpected sourcedb value: " \$4 > "/dev/stderr"; exit 1}
                            print \$1,\$2,\$3,db,"",substr(\$5,1,10)
                        }'
            } > virus-genome-metadata-raw.tsv
            rm -f virus_data_report.jsonl
            echo "Enumerated \$((  \$(wc -l < virus-genome-metadata-raw.tsv) - 1  )) sequences for taxid ${taxid}"
            """
        } else if (source_type == "assembly") {
            """
            # Fail loudly if any stage of the dataformat|tail pipe below errors
            # (Nextflow runs scripts under `bash -ue` without pipefail).
            set -o pipefail
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
            # `assminfo-release-date` is carried through for downstream tooling (index
            # benchmarking) to date each assembly. The header is printed once from
            # the shared definition.
            dataformat tsv genome \\
                --inputfile assembly_data_report.jsonl \\
                --fields accession,organism-tax-id,organism-name,source_database,assminfo-status,assminfo-release-date \\
                | { printf '${schema_header}\\n'; tail -n +2; } \\
                > virus-genome-metadata-raw.tsv
            rm -f assembly_data_report.jsonl
            echo "Enumerated \$((  \$(wc -l < virus-genome-metadata-raw.tsv) - 1  )) assemblies for taxid ${taxid}"
            """
        } else {
            throw new IllegalArgumentException(
                "ENUMERATE_VIRAL_ACCESSIONS: invalid source_type '${source_type}' (expected 'assembly' or 'sequence')")
        }
}
