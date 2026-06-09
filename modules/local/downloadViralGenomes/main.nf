// Download viral genomes for a chunk of pre-filtered assembly accessions
// using NCBI datasets CLI.
process DOWNLOAD_VIRAL_GENOMES {
    label "ncbi_datasets"
    label "large"
    label "use_scratch"
    tag "id=index,name=${accession_chunk.baseName}"
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

        # Retry with exponential backoff: both the download and rehydrate hit
        # transient NCBI stream errors that Nextflow's immediate task retry can't.
        retry() {
            desc="\$1"; shift; backoff=10
            for attempt in \$(seq 1 ${max_attempts}); do
                if "\$@"; then return 0; fi
                if [ "\$attempt" -eq ${max_attempts} ]; then
                    echo "\$desc failed after ${max_attempts} attempts" >&2
                    return 1
                fi
                echo "\$desc attempt \$attempt failed, retrying in \${backoff}s..." >&2
                sleep "\$backoff"
                backoff=\$(( backoff * 2 ))
            done
        }

        # 1. Download dehydrated package (manifest only) for the accessions in
        # this chunk. Filtering happened upstream in FILTER_VIRAL_GENBANK_METADATA.
        download_pkg() {
            datasets download genome accession \\
                --assembly-source ${assembly_source} \\
                --include genome \\
                --no-progressbar \\
                --dehydrated \\
                --inputfile ${accession_chunk} \\
                ${extra_args} \\
                --filename output.zip \\
                && unzip -o output.zip -d output/
        }
        retry "Dehydrated download" download_pkg || exit 1

        # 2. Rehydrate: download the actual genome files.
        retry "Rehydration" datasets rehydrate --directory output/ \\
            --max-workers ${task.cpus} --no-progressbar --gzip || exit 1

        # 3. Flatten rehydrate output into a single genomes/ directory.
        mkdir -p genomes
        find output/ncbi_dataset/data -name '*.fna.gz' -print0 \\
            | xargs -0 -r mv -t genomes/

        rm -rf output/ output.zip
        echo "Downloaded \$(find genomes -maxdepth 1 -name '*.fna.gz' | wc -l) genomes for chunk \$CHUNK_ID"
        """
}
