/****************************************************
| SUBWORKFLOW: EXTRACT VIRAL READS (PLATFORM DISPATCH) |
****************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { EXTRACT_VIRAL_READS_SHORT } from "../../../subworkflows/local/extractViralReadsShort"
include { EXTRACT_VIRAL_READS_ONT } from "../../../subworkflows/local/extractViralReadsONT"

/***********
| WORKFLOW |
***********/

workflow EXTRACT_VIRAL_READS {
    take:
        reads_ch    // Channel: samplesheet reads
        params_map  // Map: full params object
    main:
        if (params_map.platform == "ont") {
            ont_ch = EXTRACT_VIRAL_READS_ONT(reads_ch, params_map.ref_dir, params_map.taxid_artificial, params_map.db_download_timeout)
            hits_final = ont_ch.hits_final
            inter_lca = ont_ch.inter_lca
            inter_aligner = ont_ch.inter_minimap2
            kmer_match = channel.empty()
            kmer_trimmed = channel.empty()
        } else {
            def short_params = params_map + [
                aln_score_threshold: params_map.bt2_score_threshold,
                minhits: "1",
                kmer_suffix: "viral"
            ]
            short_ch = EXTRACT_VIRAL_READS_SHORT(reads_ch, params_map.ref_dir, short_params)
            hits_final = short_ch.hits_final
            inter_lca = short_ch.inter_lca
            inter_aligner = short_ch.inter_bowtie
            kmer_match = short_ch.kmer_match
            kmer_trimmed = short_ch.kmer_trimmed
        }
    emit:
        hits_final
        inter_lca
        inter_aligner
        kmer_match
        kmer_trimmed
}
