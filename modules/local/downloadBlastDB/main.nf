// Download a BLAST database into a fixed "blast-db" directory and expose it under
// the constant alias name "blast-db", so downstream references are independent of
// which database was downloaded (no blast_db_prefix that can drift from the index
// blast_db_name). The downloaded volume files keep their own names; an additive
// blast-db.nal alias (built with blastdb_aliastool, no rename) makes them resolve
// as "blast-db". Handles both a tarball URL (tests) and an update_blastdb.pl name.
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
            // Tarball URL (tiny test DB): download and extract, stripping the
            // archive's top-level directory so the volume files land flat in blast-db/.
            """
            mkdir blast-db
            curl -fsSL "${blast_db_name}" -o blast_db.tar.gz
            tar -xzf blast_db.tar.gz -C blast-db --strip-components=1
            cd blast-db
            blastdb_aliastool -dblist "\$(basename "${blast_db_name}" .tar.gz)" -dbtype nucl -out blast-db -title blast-db
            """
        else
            // Named DB (e.g. core_nt, nt_viruses): download with update_blastdb.pl.
            """
            mkdir blast-db
            cd blast-db
            ln -s \$(which curl) /usr/local/bin/curl
            update_blastdb.pl --source aws --num_threads ${task.cpus} --force --decompress ${blast_db_name}
            blastdb_aliastool -dblist "${blast_db_name}" -dbtype nucl -out blast-db -title blast-db
            """
}
