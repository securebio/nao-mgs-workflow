// Download viral genomes for a single taxon using NCBI datasets CLI
process DOWNLOAD_VIRAL_GENOMES {
    label "ncbi_datasets"
    label "large"
    input:
        val(taxid)
        val(assembly_source)
        val(extra_args)
        val(max_attempts)
    output:
        path("${taxid}_genomes/*.fna.gz"), emit: genomes
        path("${taxid}_metadata.tsv"), emit: metadata
    script:
        """
        # 1. Download dehydrated package (metadata + manifest only)
        datasets download genome taxon ${taxid} \\
            --assembly-source ${assembly_source} \\
            --include genome \\
            --no-progressbar \\
            --dehydrated \\
            ${extra_args} \\
            --filename output.zip
        unzip -o output.zip -d output/

        # 2. Rehydrate: download actual genome files with retry and exponential backoff
        BACKOFF=10
        for attempt in \$(seq 1 ${max_attempts}); do
            if datasets rehydrate --directory output/ --max-workers ${task.cpus} --no-progressbar --gzip; then
                break
            fi
            if [ \$attempt -eq ${max_attempts} ]; then
                echo "Rehydration failed after ${max_attempts} attempts" >&2
                exit 1
            fi
            echo "Rehydration attempt \$attempt failed, retrying in \${BACKOFF}s..." >&2
            sleep \$BACKOFF
            BACKOFF=\$((BACKOFF * 2))
        done

        # 3. Convert assembly report to TSV with standardized column names
        dataformat tsv genome \\
            --inputfile output/ncbi_dataset/data/assembly_data_report.jsonl \\
            --fields accession,organism-tax-id,organism-name,source_database \\
            > raw_metadata.tsv

        # 4. Replace header with standardized column names
        { echo -e "assembly_accession\\ttaxid\\torganism_name\\tsource_database"
          tail -n +2 raw_metadata.tsv
        } > ${taxid}_metadata.tsv

        # 5. Collect genome FASTAs into genomes/ directory
        mkdir -p ${taxid}_genomes
        find output/ncbi_dataset/data -name '*.fna.gz' -exec mv {} ${taxid}_genomes/ \\;
        rm -rf output/ output.zip raw_metadata.tsv
        echo "Downloaded \$((  \$(wc -l < ${taxid}_metadata.tsv) - 1  )) assemblies for taxid ${taxid}"
        """
}
