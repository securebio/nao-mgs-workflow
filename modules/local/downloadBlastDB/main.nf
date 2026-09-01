// Download a BLAST database into a fixed "blast_db" directory with the alias
// "blast_db", so consumers can use a fixed path independent of which database
// was downloaded.
// The reservation is sized for network bandwidth rather than compute: under a BEST_FIT
// compute environment it selects the instance, and the instance's network and EBS
// throughput is what bounds a quarter-terabyte transfer.
process DOWNLOAD_BLAST_DB {
    label "BLAST"
    label "max"
    // Retry a transient download failure rather than terminating on the first one, but
    // still fail the run rather than publishing an index with no BLAST database.
    errorStrategy { task.attempt <= task.maxRetries ? "retry" : "terminate" }
    tag "id=index"
    input:
        val(blast_db_name)
    output:
        path("blast_db"), emit: db
    script:
        // update_blastdb.pl fans out one `aws s3 cp` per volume under `xargs -P`, and each
        // of those is itself multithreaded. Concurrency is not the limiter here (4, 32 and
        // 64-way measure within 15% of each other), so it stays capped well below the
        // reservation rather than scaling with it.
        def download_threads = Math.min(task.cpus as int, 8)
        if (blast_db_name.startsWith("http://") || blast_db_name.startsWith("https://"))
            // Tarball URL
            """
            mkdir blast_db
            curl -fsSL "${blast_db_name}" -o blast_db.tar.gz
            tar -xzf blast_db.tar.gz -C blast_db --strip-components=1
            cd blast_db
            blastdb_aliastool -dblist "\$(basename "${blast_db_name}" .tar.gz)" -dbtype nucl -out blast_db -title blast_db
            """
        else
            // Named DB
            """
            mkdir blast_db
            cd blast_db
            ln -s \$(which curl) /usr/local/bin/curl
            update_blastdb.pl --source aws --num_threads ${download_threads} --force --decompress ${blast_db_name}
            blastdb_aliastool -dblist "${blast_db_name}" -dbtype nucl -out blast_db -title blast_db
            """
}
