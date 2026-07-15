// N-mask k-mers in the viral genome FASTA that are shared with the human genome
// (CHM13), stripping human-contaminated regions before the nucleaze k-mer screen
// index is built so human reads don't clear the screen. The `ref_fasta` input is
// the reference to mask against and the output filenames use a "human" infix; this
// process is human-specific by design (see the hardcoded infix below).
process MASK_GENOME_REFERENCE {
    label "BBTools"
    label "bbduk_ref_mask_resources"
    tag "id=index"
    input:
        path(genome_fasta)
        path(ref_fasta)
        val(params_map) // k, hdist, name_pattern
    output:
        path("${params_map.name_pattern}-human-masked.fasta.gz"), emit: masked
        path("${params_map.name_pattern}-human-mask.stats.txt"), emit: log
    script:
        // rcomp=t matches nucleaze's canonical k-mers; no mink, so only
        // full-length-k matches are masked (exact k-mer semantics of the screen).
        """
        bbduk.sh \
            in=${genome_fasta} \
            out=${params_map.name_pattern}-human-masked.fasta.gz \
            ref=${ref_fasta} \
            stats=${params_map.name_pattern}-human-mask.stats.txt \
            k=${params_map.k} hdist=${params_map.hdist} mm=f mask=N rcomp=t \
            -Xmx${task.memory.toGiga()}g
        """
}
