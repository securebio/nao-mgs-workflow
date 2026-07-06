// Download file via wget
process WGET {
    label "BBTools"
    label "single"
    tag "id=index,name=${name}"
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
