process DOWNLOAD_BLAST_DB {
    label "BLAST"
    label "small"
    errorStrategy "terminate"
    input:
        val(db)
    output:
        path("${db}"), emit: db
    shell:
        '''
        db="!{db}"
        mkdir ${db}
        cd ${db}
        ln -s $(which curl) /usr/local/bin/curl
        update_blastdb.pl --source aws --num_threads !{task.cpus} --force --decompress ${db}
        '''
}
