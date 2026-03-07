// Download entire viral Genbank DB
process DOWNLOAD_VIRAL_NCBI {
    label "ncbi_genome_download"
    label "small"
    input:
        val(ncbi_viral_params)
    output:
        path("ncbi_metadata.txt"), emit: metadata
        path("ncbi_genomes"), emit: genomes
    shell:
        '''
        par="--formats fasta --flat-output --verbose --parallel !{task.cpus}"
        io="--output-folder ncbi_genomes --metadata-table ncbi_metadata.txt"
        max_attempts=5
        for attempt in $(seq 1 $max_attempts); do
            rc=0
            ncbi-genome-download !{ncbi_viral_params} ${par} ${io} viral || rc=$?
            if [ ${rc} -eq 0 ]; then
                break
            elif [ $attempt -lt $max_attempts ]; then
                # Remove corrupted downloads (e.g. XML error pages) so they get re-downloaded
                bad_files=0
                for f in ncbi_genomes/*.fna.gz; do
                    if [ -f "$f" ] && [ "$(od -An -tx1 -N2 "$f" | tr -d " ")" != "1f8b" ]; then
                        rm "$f"
                        bad_files=$((bad_files + 1))
                    fi
                done
                delay=$(( 30 * (2 ** (attempt - 1)) ))
                echo "Attempt $attempt failed (exit code ${rc}), removed $bad_files corrupted files, retrying in ${delay}s..."
                sleep $delay
            else
                echo "All $max_attempts attempts failed"
                exit ${rc}
            fi
        done
        '''
}
