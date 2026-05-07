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
        // Emit the genomes directory as a single path rather than the glob
        // `${taxid}_genomes/*.fna.gz`. The glob form makes Nextflow's
        // generated `.command.run` stage-out wrapper run a shell glob via
        // `ls ...`, which exceeds ARG_MAX once a shard has more than ~10k
        // files (e.g. Riboviria's 198k). Staging out one directory entry
        // sidesteps that. Downstream PREPARE_VIRAL_METADATA recursively
        // globs the staged directories, so the flatten-via-channel pattern
        // is preserved.
        path("${taxid}_genomes"), optional: true, emit: genomes
        path("${taxid}_metadata.tsv"), emit: metadata
    script:
        // Header schema for ${taxid}_metadata.tsv
        def metadata_header = "assembly_accession\\ttaxid\\torganism_name\\tsource_database\\tassembly_status"
        """
        trap 'rm -f dl_err.txt' EXIT

        # 1. Download dehydrated package (metadata + manifest only).
        # NCBI's taxonomy can include taxa without assemblies,
        # so catch and emit empty outputs instead of failing.
        if ! datasets download genome taxon ${taxid} \\
            --assembly-source ${assembly_source} \\
            --include genome \\
            --no-progressbar \\
            --dehydrated \\
            ${extra_args} \\
            --filename output.zip 2> dl_err.txt
        then
            cat dl_err.txt >&2
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

        # 3. Convert assembly report to TSV with standardized column names.
        # `assminfo-status` is included so downstream filtering in
        # filterViralGenbankMetadata can drop non-current assemblies
        # `datasets` `--assembly-version` arg does not work currently,
        # see ncbi/datasets#576 for bug report
        dataformat tsv genome \\
            --inputfile output/ncbi_dataset/data/assembly_data_report.jsonl \\
            --fields accession,organism-tax-id,organism-name,source_database,assminfo-status \\
            > raw_metadata.tsv

        # 4. Replace header with standardized column names
        { echo -e "${metadata_header}"
          tail -n +2 raw_metadata.tsv
        } > ${taxid}_metadata.tsv

        # 5. Collect genome FASTAs into genomes/ directory.
        # Use a single batched `xargs mv` instead of `find -exec mv {} \\;`
        # (one fork+exec per file). On Fusion, the per-file form was the
        # second slow phase identified in COMP-1680 — each `mv` becomes an
        # S3 CopyObject + DeleteObject through Fusion's rename path,
        # taking ~317 files/min for the 198k-assembly Riboviria shard.
        # Combined with the `scratch '/scratch'` directive (set in
        # configs/profiles.config for Batch profiles), this keeps the
        # entire rename storm on local disk.
        mkdir -p ${taxid}_genomes
        find output/ncbi_dataset/data -name '*.fna.gz' -print0 \\
            | xargs -0 -r mv -t ${taxid}_genomes/
        rm -rf output/ output.zip raw_metadata.tsv
        echo "Downloaded \$((  \$(wc -l < ${taxid}_metadata.tsv) - 1  )) assemblies for taxid ${taxid}"
        """
}
