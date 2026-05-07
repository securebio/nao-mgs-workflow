// Download viral genomes for a chunk of pre-filtered assembly accessions
// using NCBI datasets CLI. Replaces the previous per-child-taxon download:
// chunking caps the per-task wallclock (was ~11h on the Riboviria shard) and
// — on Batch profiles, where `aws.batch.volumes` mounts `/scratch` from the
// host — the `scratch '/scratch'` directive (configured in
// `configs/profiles.config`, not here, so local/CI runs don't try to use a
// nonexistent path) avoids Fusion metadata corruption during rehydrate's
// `.fna.temp` rename storm (see COMP-1680).
process DOWNLOAD_VIRAL_GENOMES {
    label "ncbi_datasets"
    label "large"
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
        # this chunk. Filtering happened upstream in
        # FILTER_VIRAL_GENBANK_METADATA, so any failure here is fatal — there
        # are no empty-taxon edge cases to swallow.
        datasets download genome accession \\
            --assembly-source ${assembly_source} \\
            --include genome \\
            --no-progressbar \\
            --dehydrated \\
            --inputfile ${accession_chunk} \\
            ${extra_args} \\
            --filename output.zip
        unzip -o output.zip -d output/

        # 2. Rehydrate: download actual genome files with retry and exponential
        # backoff. Runs on /scratch (via the process directive) so the
        # `.fna.temp` rename storm doesn't go through Fusion's metadata layer.
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

        # 3. Flatten — rehydrate writes output/ncbi_dataset/data/<accession>/*.fna.gz;
        # downstream prepare_viral_metadata.py uses a flat glob. Use a single
        # batched mv (xargs) instead of per-file `find -exec mv {} \\;`, which
        # was the second slow phase identified in COMP-1680.
        mkdir -p genomes
        find output/ncbi_dataset/data -name '*.fna.gz' -print0 \\
            | xargs -0 -r mv -t genomes/

        # 4. Count check — Fusion silent-drops on stage-out have happened
        # before; turn any mismatch into a hard failure rather than a
        # silently incomplete index.
        expected=\$(wc -l < ${accession_chunk})
        actual=\$(find genomes -maxdepth 1 -name '*.fna.gz' | wc -l)
        if [ "\$expected" -ne "\$actual" ]; then
            echo "Genome count mismatch for chunk \$CHUNK_ID: \$actual vs expected \$expected" >&2
            exit 1
        fi

        rm -rf output/ output.zip
        echo "Downloaded \$actual genomes for chunk \$CHUNK_ID"
        """
}
