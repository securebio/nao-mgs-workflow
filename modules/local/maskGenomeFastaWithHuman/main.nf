// N-mask k-mers in the viral genome FASTA that are shared with the human genome
// (CHM13).
process MASK_GENOME_FASTA_WITH_HUMAN {
    label "BBTools"
    label "human_mask_resources"
    tag "id=index"
    input:
        path(genome_fasta)
        path(human_fasta)
        val(params_map) // k, hdist, name_pattern
    output:
        path("${params_map.name_pattern}-human-masked.fasta.gz"), emit: masked
    script:
        """
        bbduk.sh \
            in=${genome_fasta} \
            out=${params_map.name_pattern}-human-masked.fasta.gz \
            ref=${human_fasta} \
            k=${params_map.k} hdist=${params_map.hdist} mm=f mask=N rcomp=t \
            t=${task.cpus} -Xmx${task.memory.toGiga()}g
        """
}
