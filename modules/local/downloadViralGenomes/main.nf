// Download viral genomes for a chunk of pre-filtered accessions using the NCBI
// datasets CLI. Emits a single combined FASTA plus an assembly_accession ->
// genome_id map per chunk, rather than one file per accession: staging many
// small files cripples Fusion on Batch (both stage-out here and the downstream
// `.collect()` stage-in), while one combined file per chunk keeps staging cheap.
// The map preserves the assembly -> constituent sequence linkage that downstream
// metadata preparation needs. Two source paths, selected by `source_type`:
//   - "assembly": genome assemblies (`datasets download genome accession`),
//     rehydrated; the map links each assembly to its constituent sequences.
//   - "sequence": NCBI Virus / nuccore records (`datasets download virus genome
//     accession`); each record is its own genome, so the map is identity
//     (genome_id == accession). Recovers recent non-influenza viral genomes that
//     NCBI no longer publishes as assemblies.
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
        val(source_type) // "assembly" or "sequence"
    output:
        path("*.fna.gz"), emit: genomes
        path("*.map.tsv"), emit: accession_map
    script:
        // Shared retry-with-backoff wrapper: both download and rehydrate hit
        // transient NCBI stream errors that Nextflow's immediate task retry can't.
        def retry_fn = """
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
        }"""
        if (source_type == "sequence") {
            // Map `assembly_source` onto the virus dataset's source filter:
            // RefSeq-only (`--refseq`); no GenBank-only filter exists, so for
            // "genbank" warn and download all sources (RefSeq copies collapse in
            // the shared dedup). Validate up front so a typo fails fast.
            def src = assembly_source.toLowerCase()
            if (!(src in ["refseq", "genbank", "all"])) {
                throw new IllegalArgumentException(
                    "DOWNLOAD_VIRAL_GENOMES: invalid assembly_source '${assembly_source}' (expected 'genbank', 'refseq', or 'all')")
            }
            def source_flag = src == "refseq" ? "--refseq" : ""
            def warn = src == "genbank" \
                ? '>&2 echo "Warning: sequence-based download cannot filter for GenBank-only sequences; downloading all sources."' \
                : "true"
            """
            set -euo pipefail
            CHUNK_ID=\$(basename ${accession_chunk} .txt)
            # Suffix outputs with the branch so assembly/sequence chunks of the
            # same name don't collide when collected in the union.
            OUT="\${CHUNK_ID}_${source_type}"
            ${retry_fn}

            # 1. Download the virus genome package for this chunk's nuccore
            # accessions (no dehydrate/rehydrate for the virus dataset).
            ${warn}
            download_pkg() {
                datasets download virus genome accession \\
                    ${source_flag} \\
                    --include genome \\
                    --no-progressbar \\
                    --inputfile ${accession_chunk} \\
                    ${extra_args} \\
                    --filename output.zip \\
                    && unzip -o output.zip -d output/
            }
            retry "Sequence download" download_pkg || exit 1

            # 2. The virus package is a single combined genomic.fna; each record
            # is its own genome, so the map is identity (genome_id == accession).
            if [ ! -s output/ncbi_dataset/data/genomic.fna ]; then
                echo "No genomic.fna in downloaded virus package (unexpected?)" >&2
                exit 1
            fi
            printf 'assembly_accession\\tgenome_id\\n' > "\${OUT}.map.tsv"
            awk '/^>/{ id=substr(\$1,2); print id"\\t"id }' \\
                output/ncbi_dataset/data/genomic.fna >> "\${OUT}.map.tsv"
            # A non-empty genomic.fna must contain headers; empty map => malformed.
            n_seqs=\$(( \$(wc -l < "\${OUT}.map.tsv") - 1 ))
            if [ "\$n_seqs" -le 0 ]; then
                echo "No sequence headers extracted from genomic.fna (malformed package?)" >&2
                exit 1
            fi
            # `datasets download virus genome accession` skips unresolvable or
            # withdrawn accessions with a warning yet still exits 0 (unlike the
            # assembly path, which hard-fails on a bad accession), and `--refseq`
            # legitimately filters non-RefSeq accessions. Partial results are
            # therefore tolerated; report any shortfall for visibility.
            n_req=\$(grep -cve '^[[:space:]]*\$' ${accession_chunk})
            if [ "\$n_seqs" -lt "\$n_req" ]; then
                echo "Note: downloaded \$n_seqs of \$n_req requested accessions (unresolved/filtered skipped)." >&2
            fi
            gzip -c output/ncbi_dataset/data/genomic.fna > "\${OUT}.fna.gz"
            rm -rf output/ output.zip
            echo "Combined \$n_seqs sequences for chunk \$CHUNK_ID"
            """
        } else if (source_type == "assembly") {
            """
            set -euo pipefail
            CHUNK_ID=\$(basename ${accession_chunk} .txt)
            # Suffix outputs with the branch so assembly/sequence chunks of the
            # same name don't collide when collected in the union.
            OUT="\${CHUNK_ID}_${source_type}"
            ${retry_fn}

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
            printf 'assembly_accession\\tgenome_id\\n' > "\${OUT}.map.tsv"
            : > combined.fna
            find output/ncbi_dataset/data -mindepth 2 -name '*.fna.gz' | sort \\
                | while IFS= read -r f; do
                    acc=\$(printf '%s\\n' "\$f" | sed -E 's#.*/data/([^/]+)/.*#\\1#')
                    # Decompress once: append sequences to the combined FASTA and
                    # extract genome_ids (header first token) for the map in one pass.
                    zcat "\$f" | tee -a combined.fna \\
                        | awk -v a="\$acc" '/^>/{ id=substr(\$1,2); print a"\\t"id }' \\
                        >> "\${OUT}.map.tsv"
                done
            # A successful rehydrate must yield sequences; an empty map means the
            # layout assumption broke — fail loudly rather than emit an empty DB.
            if [ "\$(wc -l < "\${OUT}.map.tsv")" -le 1 ]; then
                echo "No genome sequences found under output/ncbi_dataset/data (unexpected layout?)" >&2
                exit 1
            fi
            gzip -c combined.fna > "\${OUT}.fna.gz"
            rm -f combined.fna
            rm -rf output/ output.zip
            echo "Combined \$(( \$(wc -l < "\${OUT}.map.tsv") - 1 )) sequences for chunk \$CHUNK_ID"
            """
        } else {
            throw new IllegalArgumentException(
                "DOWNLOAD_VIRAL_GENOMES: invalid source_type '${source_type}' (expected 'assembly' or 'sequence')")
        }
}
