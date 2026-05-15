// Download viral genomes for a chunk of pre-filtered assembly accessions
// using NCBI datasets CLI.
process DOWNLOAD_VIRAL_GENOMES {
    label "ncbi_datasets"
    label "large"
    label "use_scratch"
    input:
        path(accession_chunk)
        val(assembly_source)
        val(extra_args)
        val(max_attempts)
    output:
        path("genomes/*.fna.gz"), emit: genomes
    script:
        """
        CHUNK_ID=\$(basename ${accession_chunk} .txt)

        # 1. Download dehydrated package (manifest only) for the accessions in
        # this chunk. Filtering happened upstream in FILTER_VIRAL_GENBANK_METADATA.
        datasets download genome accession \\
            --assembly-source ${assembly_source} \\
            --include genome \\
            --no-progressbar \\
            --dehydrated \\
            --inputfile ${accession_chunk} \\
            ${extra_args} \\
            --filename output.zip
        unzip -o output.zip -d output/

        # 2. Rehydrate: download actual genome files with retry and exponential backoff.
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

        # 3. Flatten rehydrate output into a single genomes/ directory.
        mkdir -p genomes
        find output/ncbi_dataset/data -name '*.fna.gz' -print0 \\
            | xargs -0 -r mv -t genomes/

        rm -rf output/ output.zip
        echo "Downloaded \$(find genomes -maxdepth 1 -name '*.fna.gz' | wc -l) genomes for chunk \$CHUNK_ID"
        """
}
