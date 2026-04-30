/***********************************************************************************************
| WORKFLOW: PREPROCESSING, TAXONMIC PROFILING AND HUMAN VIRUS ANALYSIS ON SHORT-READ MGS DATA (EITHER SINGLE-END OR PAIRED-END) |
***********************************************************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { LOAD_SAMPLESHEET } from "../subworkflows/local/loadSampleSheet"
include { COUNT_READS } from "../modules/local/countReads"
include { EXTRACT_VIRAL_READS } from "../subworkflows/local/extractViralReads"
include { SUBSET_TRIM } from "../subworkflows/local/subsetTrim"
include { RUN_QC } from "../subworkflows/local/runQc"
include { PROFILE} from "../subworkflows/local/profile"
include { CHECK_VERSION_COMPATIBILITY } from "../subworkflows/local/checkVersionCompatibility"
include { PREPARE_INPUT_LOGGING } from "../subworkflows/local/prepareInputLogging"
include { WRITE_SENTINEL_RUN } from "../modules/local/writeSentinelRun"

/*****************
| MAIN WORKFLOWS |
*****************/

// Complete primary workflow
workflow RUN {
    main:
        // Setup
        compat_ch = CHECK_VERSION_COMPATIBILITY(params.ref_dir, projectDir)
        samplesheet_ch = LOAD_SAMPLESHEET(params.sample_sheet, params.platform, false)
        // Results
        viral_ch = EXTRACT_VIRAL_READS(samplesheet_ch.samplesheet, params)
        count_ch = COUNT_READS(samplesheet_ch.samplesheet, samplesheet_ch.single_end)
        subset_ch = SUBSET_TRIM(samplesheet_ch.samplesheet, samplesheet_ch.single_end, params)
        qc_ch = RUN_QC(subset_ch.subset_reads, subset_ch.trimmed_subset_reads, samplesheet_ch.single_end)
        def profile_params = params + [min_kmer_fraction: "0.4", k: "27", ribo_suffix: "ribo"]
        profile_ch = PROFILE(subset_ch.trimmed_subset_reads, samplesheet_ch.single_end, profile_params)
        // Prepare output streams
        input_log_ch = PREPARE_INPUT_LOGGING(params, compat_ch.index_pyproject_path, compat_ch.pipeline_pyproject_path, samplesheet_ch.start_time_str)
        qc_results_ch = count_ch.output.mix(qc_ch.pre_qc, qc_ch.post_qc, subset_ch.fastp_json)
        other_results_ch = viral_ch.hits_final.mix(profile_ch.bracken, profile_ch.kraken)
        // Validate published outputs and write sentinel
        expected_ch = input_log_ch.input_run.mix(input_log_ch.logging_run, qc_results_ch, other_results_ch)
        sentinel_samples = samplesheet_ch.samplesheet.map { sample, _reads -> sample }.collect()
        sentinel_params = params + [output_dir: "${params.base_dir}/output", pyproject_path: "${projectDir}/pyproject.toml"]
        WRITE_SENTINEL_RUN(expected_ch.collect(), sentinel_samples, samplesheet_ch.start_time_str, sentinel_params)
    emit:
        input_run = input_log_ch.input_run
        logging_run = input_log_ch.logging_run
        intermediates_run = viral_ch.inter_lca.mix(viral_ch.inter_aligner)
        reads_raw_viral = viral_ch.bbduk_match
        reads_trimmed_viral = viral_ch.bbduk_trimmed
        qc_results_run = qc_results_ch
        other_results_run = other_results_ch
        experimental_run = channel.empty()
        sentinel_run = WRITE_SENTINEL_RUN.out.sentinel
}
