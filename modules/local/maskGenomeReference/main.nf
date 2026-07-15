// Mask k-mers in a genome FASTA that are shared with a contaminant reference
// (e.g. the human genome), N-masking any k-mer present in the reference. Used
// to strip host/contaminant contamination from viral genomes before building
// the nucleaze k-mer screen index, so contaminant reads don't clear the screen.
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
