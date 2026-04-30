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
        path("${taxid}_genomes/*.fna.gz"), optional: true, emit: genomes
        path("${taxid}_metadata.tsv"), emit: metadata
    script:
        // Header schema for ${taxid}_metadata.tsv
        def metadata_header = "assembly_accession\\ttaxid\\torganism_name\\tsource_database"
        """
        trap 'rm -f dl_err.txt' EXIT

        # 1. Download dehydrated package (metadata + manifest only).
        # NCBI's taxonomy can include taxa without assemblies,
        # so catch and emit empty outputs instead of failing.
        # We capture stderr to grep for the empty-taxon signature; the
        # post-call `cat dl_err.txt >&2` replays it for diagnostic visibility.
        # This defers stderr (vs. streaming live to .command.err) but the
        # `datasets download --dehydrated` step is short, so the trade-off
        # is acceptable. The post-success replay is purely diagnostic and
        # non-load-bearing: it surfaces any informational stderr but no
        # downstream logic depends on it, so it is intentionally not covered
        # by a dedicated success-path stderr test.
        if ! datasets download genome taxon ${taxid} \\
            --assembly-source ${assembly_source} \\
            --include genome \\
            --no-progressbar \\
            --dehydrated \\
            ${extra_args} \\
            --filename output.zip 2> dl_err.txt
        then
            cat dl_err.txt >&2
            # Anchored single-line match is intentional: it pins the dispatch
            # to the exact `datasets` error string. If a future release wraps
            # or splits the message, this regex must be revisited.
            if grep -qE '^Error:.*no genome data is currently available for this taxon\\.\$' dl_err.txt; then
                echo -e "${metadata_header}" > ${taxid}_metadata.tsv
                echo "Taxon ${taxid} has no assemblies available; emitting empty outputs." >&2
                exit 0
            fi
            exit 1
        fi
        cat dl_err.txt >&2
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
        { echo -e "${metadata_header}"
          tail -n +2 raw_metadata.tsv
        } > ${taxid}_metadata.tsv

        # 5. Collect genome FASTAs into genomes/ directory
        mkdir -p ${taxid}_genomes
        find output/ncbi_dataset/data -name '*.fna.gz' -exec mv {} ${taxid}_genomes/ \\;
        rm -rf output/ output.zip raw_metadata.tsv
        echo "Downloaded \$((  \$(wc -l < ${taxid}_metadata.tsv) - 1  )) assemblies for taxid ${taxid}"
        """
}
