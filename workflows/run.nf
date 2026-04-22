/***********************************************************************************************
| WORKFLOW: PREPROCESSING, TAXONMIC PROFILING AND HUMAN VIRUS ANALYSIS ON SHORT-READ MGS DATA (EITHER SINGLE-END OR PAIRED-END) |
***********************************************************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { LOAD_SAMPLESHEET } from "../subworkflows/local/loadSampleSheet"
include { COUNT_READS } from "../modules/local/countReads"
include { EXTRACT_VIRAL_READS_SHORT } from "../subworkflows/local/extractViralReadsShort"
include { EXTRACT_VIRAL_READS_ONT } from "../subworkflows/local/extractViralReadsONT"
include { SUBSET_TRIM } from "../subworkflows/local/subsetTrim"
include { RUN_QC } from "../subworkflows/local/runQc"
include { PROFILE} from "../subworkflows/local/profile"
include { CHECK_VERSION_COMPATIBILITY } from "../subworkflows/local/checkVersionCompatibility"
include { PREPARE_INPUT_LOGGING } from "../subworkflows/local/prepareInputLogging"

/*****************
| MAIN WORKFLOWS |
*****************/

// Complete primary workflow
workflow RUN {
    main:
    // Setup
    compat_ch = CHECK_VERSION_COMPATIBILITY(params.ref_dir, projectDir)
    samplesheet_ch = LOAD_SAMPLESHEET(params.sample_sheet, params.platform, false)

    // Extract human-viral reads
    if ( params.platform == "ont" ) {
        EXTRACT_VIRAL_READS_ONT(samplesheet_ch.samplesheet, params.ref_dir, params.taxid_artificial, params.db_download_timeout)
        hits_final = EXTRACT_VIRAL_READS_ONT.out.hits_final
        inter_lca = EXTRACT_VIRAL_READS_ONT.out.inter_lca
        inter_aligner = EXTRACT_VIRAL_READS_ONT.out.inter_minimap2
        bbduk_match = Channel.empty()
        bbduk_trimmed = Channel.empty()
     } else {
        def short_params = params.collectEntries { k, v -> [k, v] }
        short_params["aln_score_threshold"] = params.bt2_score_threshold // rename to match
        short_params["min_kmer_hits"] = "1"
        short_params["bbduk_suffix"] = "viral"
        short_params["k"] = "24" 
        EXTRACT_VIRAL_READS_SHORT(samplesheet_ch.samplesheet, params.ref_dir, short_params)
        hits_final = EXTRACT_VIRAL_READS_SHORT.out.hits_final
        inter_lca = EXTRACT_VIRAL_READS_SHORT.out.inter_lca
        inter_aligner = EXTRACT_VIRAL_READS_SHORT.out.inter_bowtie
        bbduk_match = EXTRACT_VIRAL_READS_SHORT.out.bbduk_match
        bbduk_trimmed = EXTRACT_VIRAL_READS_SHORT.out.bbduk_trimmed
    }

    // Other results
    count_ch = COUNT_READS(samplesheet_ch.samplesheet, samplesheet_ch.single_end)
    subset_ch = SUBSET_TRIM(samplesheet_ch.samplesheet, samplesheet_ch.single_end, params)
    qc_ch = RUN_QC(subset_ch.subset_reads, subset_ch.trimmed_subset_reads, samplesheet_ch.single_end)

    // Profile ribosomal and non-ribosomal reads of the subset adapter-trimmed reads
    kraken_db_path = "${params.ref_dir}/results/kraken_db"
    def profile_params = params.collectEntries { k, v -> [k, v] } + [min_kmer_fraction: "0.4", k: "27", ribo_suffix: "ribo"]
    profile_ch = PROFILE(subset_ch.trimmed_subset_reads, kraken_db_path, params.ref_dir, samplesheet_ch.single_end, profile_params)

    // Prepare input and logging files for publishing
    input_log_ch = PREPARE_INPUT_LOGGING(params, compat_ch.index_pyproject_path, compat_ch.pipeline_pyproject_path, samplesheet_ch.start_time_str)

    emit:
        input_run = input_log_ch.input_run
        logging_run = input_log_ch.logging_run
        intermediates_run = inter_lca.mix(inter_aligner)
        reads_raw_viral = bbduk_match
        reads_trimmed_viral = bbduk_trimmed
        qc_results_run = count_ch.output.mix(qc_ch.pre_qc, qc_ch.post_qc, subset_ch.fastp_json)
        other_results_run = hits_final.mix(profile_ch.bracken, profile_ch.kraken)
        experimental_run = Channel.empty()
}
