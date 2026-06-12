// Download a BLAST database into a fixed "blast-db" directory with the alias
// // "blast-db", so consumers can used a fixed path independent of which database
// was downloaded.
process DOWNLOAD_BLAST_DB {
    label "BLAST"
    label "xsmall"
    errorStrategy "terminate"
    tag "id=index"
    input:
        val(blast_db_name)
    output:
        path("blast-db"), emit: db
    script:
        if (blast_db_name.startsWith("http://") || blast_db_name.startsWith("https://"))
            // Tarball URL
            """
            mkdir blast-db
            curl -fsSL "${blast_db_name}" -o blast_db.tar.gz
            tar -xzf blast_db.tar.gz -C blast-db --strip-components=1
            cd blast-db
            blastdb_aliastool -dblist "\$(basename "${blast_db_name}" .tar.gz)" -dbtype nucl -out blast-db -title blast-db
            """
        else
            // Named DB
            """
            mkdir blast-db
            cd blast-db
            ln -s \$(which curl) /usr/local/bin/curl
            update_blastdb.pl --source aws --num_threads ${task.cpus} --force --decompress ${blast_db_name}
            blastdb_aliastool -dblist "${blast_db_name}" -dbtype nucl -out blast-db -title blast-db
            """
}
