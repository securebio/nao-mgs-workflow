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
            EXTRACT_VIRAL_READS_ONT(reads_ch, params_map.ref_dir, params_map.taxid_artificial, params_map.db_download_timeout)
            hits_final = EXTRACT_VIRAL_READS_ONT.out.hits_final
            inter_lca = EXTRACT_VIRAL_READS_ONT.out.inter_lca
            inter_aligner = EXTRACT_VIRAL_READS_ONT.out.inter_minimap2
            kmer_match = Channel.empty()
            kmer_trimmed = Channel.empty()
        } else {
            // Read nucleaze_k from the index's params snapshot rather than
            // hardcoding here; guarantees the RUN screen uses the same k
            // the index was built against, even if defaults shift later.
            def index_params_path = file("${params_map.ref_dir}/input/index-params.json", checkIfExists: true)
            def index_params = new groovy.json.JsonSlurper().parse(index_params_path)
            if (index_params.nucleaze_k == null) {
                throw new IllegalStateException(
                    "Index at ${params_map.ref_dir} has no nucleaze_k in input/index-params.json; " +
                    "rebuild the index against a pipeline version >= 3.2.2.0."
                )
            }
            def short_params = params_map + [
                aln_score_threshold: params_map.bt2_score_threshold,
                minhits: "1",
                kmer_suffix: "viral",
                k: index_params.nucleaze_k.toString()
            ]
            EXTRACT_VIRAL_READS_SHORT(reads_ch, params_map.ref_dir, short_params)
            hits_final = EXTRACT_VIRAL_READS_SHORT.out.hits_final
            inter_lca = EXTRACT_VIRAL_READS_SHORT.out.inter_lca
            inter_aligner = EXTRACT_VIRAL_READS_SHORT.out.inter_bowtie
            kmer_match = EXTRACT_VIRAL_READS_SHORT.out.kmer_match
            kmer_trimmed = EXTRACT_VIRAL_READS_SHORT.out.kmer_trimmed
        }
    emit:
        hits_final
        inter_lca
        inter_aligner
        kmer_match
        kmer_trimmed
}
