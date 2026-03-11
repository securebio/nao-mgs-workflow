// Download viral genomes for a single taxon using NCBI datasets CLI
process DOWNLOAD_VIRAL_GENOMES {
    label "ncbi_datasets"
    label "small"
    input:
        val(taxid)
        val(assembly_source)
        val(api_key)
        val(extra_args)
    output:
        path("genomes/*.fna.gz"), emit: genomes, optional: true
        path("metadata.tsv"), emit: metadata
    script:
        def env_block = api_key ? "export NCBI_API_KEY='${api_key}'" : ""
        def api_flag = api_key ? '--api-key "\$NCBI_API_KEY"' : ""
        """
        ${env_block}

        # Validate inputs
        if ! echo "${assembly_source}" | grep -qxE 'genbank|refseq'; then
            echo "ERROR: assembly_source must be 'genbank' or 'refseq', got '${assembly_source}'" >&2
            exit 1
        fi
        if ! echo "${taxid}" | grep -qxE '[0-9]+'; then
            echo "ERROR: taxid must be numeric, got '${taxid}'" >&2
            exit 1
        fi

        # Run datasets download; capture exit code to distinguish errors from empty results
        set +e
        datasets download genome taxon ${taxid} \\
            --assembly-source ${assembly_source} \\
            --include genome \\
            --no-progress \\
            ${api_flag} \\
            ${extra_args} \\
            --filename output.zip
        EXIT_CODE=\$?
        set -e

        if [ \$EXIT_CODE -ne 0 ]; then
            # Check if this is a "no assemblies found" error vs a real failure
            # datasets exits with code 1 and no output.zip when there are no matching assemblies
            if [ \$EXIT_CODE -eq 1 ] && [ ! -f output.zip ]; then
                echo "No assemblies found for taxid ${taxid}, producing empty metadata"
                echo -e "assembly_accession\\ttaxid\\torganism_name\\tsource_database" > metadata.tsv
                mkdir -p genomes
            else
                echo "ERROR: datasets download failed with exit code \$EXIT_CODE for taxid ${taxid}" >&2
                exit \$EXIT_CODE
            fi
        elif [ ! -f output.zip ]; then
            echo "No assemblies found for taxid ${taxid} (no output file), producing empty metadata"
            echo -e "assembly_accession\\ttaxid\\torganism_name\\tsource_database" > metadata.tsv
            mkdir -p genomes
        else
            unzip -o output.zip -d output/

            # Convert assembly report to TSV with standardized column names
            dataformat tsv genome \\
                --inputfile output/ncbi_dataset/data/assembly_data_report.jsonl \\
                --fields accession,organism-tax-id,organism-name,source_database \\
                > raw_metadata.tsv

            # Replace header with standardized column names
            { echo -e "assembly_accession\\ttaxid\\torganism_name\\tsource_database"
              tail -n +2 raw_metadata.tsv
            } > metadata.tsv

            # Flatten genome FASTAs into genomes/ directory
            mkdir -p genomes
            find output/ncbi_dataset/data -name '*.fna' -exec gzip {} \\;
            find output/ncbi_dataset/data -name '*.fna.gz' -exec mv {} genomes/ \\;

            rm -rf output/ output.zip raw_metadata.tsv
            echo "Downloaded \$((  \$(wc -l < metadata.tsv) - 1  )) assemblies for taxid ${taxid}"
        fi
        """
}
