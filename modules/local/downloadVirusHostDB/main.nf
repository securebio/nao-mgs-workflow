// Download entire VirusHostDB
process DOWNLOAD_VIRUS_HOST_DB {
    label "single"
    label "curl"
    input:
        val(virus_host_db_url)
    output:
        path("virus-host-db.tsv")
    script:
        """
        curl -sS ${virus_host_db_url} > virus-host-db.tsv
        """
}
