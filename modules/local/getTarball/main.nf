// Download and extract a Gzipped tarball into a directory
process GET_TARBALL {
    label "tar_wget"
    label "huge_mem"
    input:
        val(tarball_url)
        val(outdir)
        val(makedir)
    output:
        path(outdir)
    script:
        """
        dl_path=\$(basename ${tarball_url})
        wget "${tarball_url}" -O \${dl_path}
        if [[ "${makedir}" == "true" ]]; then
            mkdir ${outdir}
            tar -xzf \${dl_path} -C ${outdir}
        else
            tar -xzf \${dl_path}
        fi
        """
}
