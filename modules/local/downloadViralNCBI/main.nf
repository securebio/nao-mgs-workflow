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
        max_attempts=10
        for attempt in $(seq 1 $max_attempts); do
            rc=0
            ncbi-genome-download !{ncbi_viral_params} ${par} ${io} viral 2>ngd_stderr.log || rc=$?
            cat ngd_stderr.log >&2
            # Remove files with checksum mismatches so they get re-downloaded
            grep -oP "Checksum mismatch for '\\K[^']+'" ngd_stderr.log > bad_file_list.txt || true
            bad_files=$(wc -l < bad_file_list.txt)
            if [ $bad_files -gt 0 ]; then
                xargs rm -f < bad_file_list.txt
            else
                # Fall back to scanning all files if no mismatches were reported
                for f in ncbi_genomes/*.fna.gz; do
                    if [ -f "$f" ] && [ "$(od -An -tx1 -N2 "$f" | tr -d " ")" != "1f8b" ]; then
                        rm "$f"
                        bad_files=$((bad_files + 1))
                    fi
                done
            fi
            echo "Attempt $attempt: exit code ${rc}, $bad_files corrupted files"
            if [ ${rc} -eq 0 ] && [ $bad_files -eq 0 ]; then
                break
            elif [ $attempt -lt $max_attempts ]; then
                delay=$(( 30 * (2 ** (attempt - 1)) ))
                echo "Retrying in ${delay}s..."
                sleep $delay
            else
                echo "All $max_attempts attempts failed"
                exit 1
            fi
        done
        '''
}
