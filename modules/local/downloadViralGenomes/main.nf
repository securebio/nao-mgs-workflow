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
        set -euo pipefail
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

        # 3. Collapse the rehydrate output into a single combined FASTA plus an
        # assembly_accession -> genome_id map. A recursive `find` (robust to any
        # nesting under data/<ASSEMBLY_ACC>/) locates every genome file; the
        # accession is the path component directly under data/, and each sequence
        # header's first token is the genome_id. Reads are local scratch here, so
        # per-file reads are cheap; only the two combined outputs are staged out.
        #
        # Genome files are restricted to the accessions this chunk asked for.
        # `datasets` can return superseded assembly versions alongside the
        # requested ones (see #758), and those are absent from the filtered
        # metadata that PREPARE_VIRAL_METADATA expands; emitting them here would
        # put untracked sequences in the genome DB, which RUN rejects with
        # "No matching genome ID found". One awk pass does the accession
        # extraction and the membership test for the whole file list, rather
        # than forking per genome file.
        # `-printf '%P'` prints the path relative to the `find` root, so the
        # accession is everything before the first '/' — no need to match on a
        # '/data/' component, which a greedy regex could find again deeper in
        # the tree and mis-parse.
        find output/ncbi_dataset/data -mindepth 2 -name '*.fna.gz' -printf '%P\\n' \\
            | sort > all_files.txt
        awk -F/ 'NR==FNR { requested[\$0] = 1; next }
             { if (\$1 in requested) { print \$1 "\\toutput/ncbi_dataset/data/" \$0 }
               else { n_skipped++ } }
             END { if (n_skipped > 0) {
                       printf "Skipped %d unrequested genome file(s)\\n", n_skipped \\
                           > "/dev/stderr" } }' \\
            ${accession_chunk} all_files.txt > kept_files.tsv
        printf 'assembly_accession\\tgenome_id\\n' > "\${CHUNK_ID}.map.tsv"
        : > combined.fna
        while IFS=\$'\\t' read -r acc f; do
            # Decompress once: append sequences to the combined FASTA and
            # extract genome_ids (header first token) for the map in one pass.
            zcat "\$f" | tee -a combined.fna \\
                | awk -v a="\$acc" '/^>/{ id=substr(\$1,2); print a"\\t"id }' \\
                >> "\${CHUNK_ID}.map.tsv"
        done < kept_files.tsv
        # A successful rehydrate must yield sequences; an empty map means the
        # layout assumption broke — fail loudly rather than emit an empty DB.
        if [ "\$(wc -l < "\${CHUNK_ID}.map.tsv")" -le 1 ]; then
            echo "No genome sequences found under output/ncbi_dataset/data (unexpected layout?)" >&2
            exit 1
        fi
        gzip -c combined.fna > "\${CHUNK_ID}.fna.gz"
        rm -f combined.fna all_files.txt kept_files.tsv
        rm -rf output/ output.zip
        echo "Combined \$(( \$(wc -l < "\${CHUNK_ID}.map.tsv") - 1 )) sequences for chunk \$CHUNK_ID"
        """
}
