// Download file via wget
process WGET {
    label "BBTools"
    label "single"
    input:
        val(url)
        val(name)
    output:
        path("${name}"), emit: file
    script:
        """
        wget "${url}" -O ${name}
        """
}
