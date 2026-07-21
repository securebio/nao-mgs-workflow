// Download viral genomes for a chunk of pre-filtered assembly accessions
// using NCBI datasets CLI. Emits a single combined FASTA plus an
// assembly-accession -> genome_id map per chunk, rather than one file per
// accession: staging many small files cripples Fusion on Batch (both stage-out
// here and the downstream `.collect()` stage-in), while one combined file per
// chunk keeps staging cheap. The map preserves the assembly -> constituent
// sequence linkage that downstream metadata preparation needs.
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
        path("*.fna.gz"), emit: genomes
        path("*.map.tsv"), emit: accession_map
    script:
        """
        set -o pipefail
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

        # 3. Collapse the rehydrate layout
        # (output/ncbi_dataset/data/<ASSEMBLY_ACC>/*.fna.gz) into a single
        # combined FASTA plus an assembly_accession -> genome_id map. The
        # directory name is the assembly accession; each sequence header's first
        # token is the genome_id. Reads are local scratch here, so per-file
        # reads are cheap; only the two combined outputs are staged out.
        printf 'assembly_accession\\tgenome_id\\n' > "\${CHUNK_ID}.map.tsv"
        : > combined.fna
        for accdir in output/ncbi_dataset/data/*/; do
            [ -d "\$accdir" ] || continue
            acc=\$(basename "\$accdir")
            for f in "\$accdir"*.fna.gz; do
                [ -e "\$f" ] || continue
                zcat "\$f" >> combined.fna
                zcat "\$f" | awk -v a="\$acc" '/^>/{ id=substr(\$1,2); print a"\\t"id }' \\
                    >> "\${CHUNK_ID}.map.tsv"
            done
        done
        gzip -c combined.fna > "\${CHUNK_ID}.fna.gz"
        rm -f combined.fna
        rm -rf output/ output.zip
        echo "Combined \$(( \$(wc -l < "\${CHUNK_ID}.map.tsv") - 1 )) sequences for chunk \$CHUNK_ID"
        """
}
