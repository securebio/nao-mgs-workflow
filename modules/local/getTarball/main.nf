// Download and extract a Gzipped tarball into a directory.
// The archive is piped straight into tar: staging it first put the whole tarball
// through the work directory, which on Fusion-backed profiles is a round trip to S3,
// and forced the download to finish before extraction could start.
process GET_TARBALL {
    label "tar_wget"
    label "single_huge_mem"
    tag "id=index,name=${outdir}"
    input:
        val(tarball_url)
        val(outdir)
        val(makedir)
    output:
        path(outdir)
    script:
        """
        set -euo pipefail
        if [[ "${makedir}" == "true" ]]; then
            mkdir ${outdir}
            dest=${outdir}
        else
            dest=.
        fi
        # Streaming leaves no partial archive on disk for a later invocation to resume
        # from, so a transient error has to be ridden out inside this one. --read-timeout
        # is what catches a silently stalled connection, which is the failure mode that
        # would otherwise hang the task rather than fail it.
        wget --tries=20 --waitretry=10 --retry-connrefused \\
            --retry-on-http-error=429,500,502,503,504 --read-timeout=120 \\
            -nv "${tarball_url}" -O - | tar -xz -C \${dest}
        """
}
