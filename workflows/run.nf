/***********************************************************************************************
| WORKFLOW: PREPROCESSING, TAXONMIC PROFILING AND HUMAN VIRUS ANALYSIS ON SHORT-READ MGS DATA (EITHER SINGLE-END OR PAIRED-END) |
***********************************************************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { LOAD_SAMPLESHEET } from "../subworkflows/local/loadSampleSheet"
include { COUNT_TOTAL_READS } from "../subworkflows/local/countTotalReads"
include { EXTRACT_VIRAL_READS_SHORT } from "../subworkflows/local/extractViralReadsShort"
include { EXTRACT_VIRAL_READS_ONT } from "../subworkflows/local/extractViralReadsONT"
include { SUBSET_TRIM } from "../subworkflows/local/subsetTrim"
include { RUN_QC } from "../subworkflows/local/runQc"
include { PROFILE} from "../subworkflows/local/profile"
include { CHECK_VERSION_COMPATIBILITY } from "../subworkflows/local/checkVersionCompatibility"
include { COPY_FILE_BARE as COPY_INDEX_PARAMS } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_INDEX_PYPROJECT } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_PYPROJECT } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_SAMPLESHEET } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_ADAPTERS } from "../modules/local/copyFile"

/***********************
| AUXILIARY FUNCTIONS |
***********************/

def getIndexPyprojectPath(ref_dir) {
    /* Get the pyproject.toml path for an index, with backwards compatibility
    for old indexes that use separate version text files. */
    def index_pyproject_file = file("${ref_dir}/logging/pyproject.toml")
    if (index_pyproject_file.exists()) {
        return index_pyproject_file
    }
    // Fall back to old format - generate pyproject content from old files
    def index_version = file("${ref_dir}/logging/pipeline-version.txt").text.trim()
    def index_min_pipeline = file("${ref_dir}/logging/index-min-pipeline-version.txt").text.trim()
    def pyproject_content = """\
[project]
version = "${index_version}"

[tool.mgs-workflow]
index-min-pipeline-version = "${index_min_pipeline}"
"""
    def temp_file = File.createTempFile("index-pyproject", ".toml")
    temp_file.text = pyproject_content
    temp_file.deleteOnExit()
    return file(temp_file.absolutePath)
}

/*****************
| MAIN WORKFLOWS |
*****************/

// Complete primary workflow
workflow RUN {
    main:
    // Check index/pipeline version compatibility
    pipeline_pyproject_path = file("${projectDir}/pyproject.toml")
    index_pyproject_path = getIndexPyprojectPath(params.ref_dir)
    CHECK_VERSION_COMPATIBILITY(pipeline_pyproject_path, index_pyproject_path)

    // Setting reference paths
    kraken_db_path = "${params.ref_dir}/results/kraken_db"

    // Load samplesheet and check platform
    LOAD_SAMPLESHEET(params.sample_sheet, params.platform, false)
    samplesheet_ch = LOAD_SAMPLESHEET.out.samplesheet
    start_time_str = LOAD_SAMPLESHEET.out.start_time_str
    single_end_ch = LOAD_SAMPLESHEET.out.single_end

    // Count reads in files
    COUNT_TOTAL_READS(samplesheet_ch, single_end_ch)

    // Extract and count human-viral reads
    if ( params.platform == "ont" ) {
        EXTRACT_VIRAL_READS_ONT(samplesheet_ch, params.ref_dir, params.taxid_artificial, params.db_download_timeout)
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
        EXTRACT_VIRAL_READS_SHORT(samplesheet_ch, params.ref_dir, short_params)
        hits_final = EXTRACT_VIRAL_READS_SHORT.out.hits_final
        inter_lca = EXTRACT_VIRAL_READS_SHORT.out.inter_lca
        inter_aligner = EXTRACT_VIRAL_READS_SHORT.out.inter_bowtie
        bbduk_match = EXTRACT_VIRAL_READS_SHORT.out.bbduk_match
        bbduk_trimmed = EXTRACT_VIRAL_READS_SHORT.out.bbduk_trimmed
    }

    // Subset reads to target number, and trim adapters
    def subset_trim_params = params.collectEntries { k, v -> [k, v] }
    SUBSET_TRIM(samplesheet_ch, single_end_ch, subset_trim_params)

    // Run QC on subset reads before and after adapter trimming
    RUN_QC(SUBSET_TRIM.out.subset_reads, SUBSET_TRIM.out.trimmed_subset_reads, single_end_ch)

    // Profile ribosomal and non-ribosomal reads of the subset adapter-trimmed reads
    def profile_params = params.collectEntries { k, v -> [k, v] } + [min_kmer_fraction: "0.4", k: "27", ribo_suffix: "ribo"]
    PROFILE(SUBSET_TRIM.out.trimmed_subset_reads, kraken_db_path, params.ref_dir, single_end_ch, profile_params)

    // Get index files for publishing
    index_params_path = file("${params.ref_dir}/input/index-params.json")
    index_params_ch = COPY_INDEX_PARAMS(Channel.fromPath(index_params_path), "params-index.json")
    index_pyproject_ch = COPY_INDEX_PYPROJECT(Channel.fromPath(index_pyproject_path), "pyproject-index.toml")

    // Prepare other publishing variables
    params_str = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(params))
    params_ch = Channel.of(params_str).collectFile(name: "params-run.json")
    time_ch = start_time_str.map { it + "\n" }.collectFile(name: "time.txt")
    // Note: we send these input/logging files through a COPY_FILE_BASE process
    // because nextflow 25.04 now only publishes files that have passed through the working directory.
    // We first tried collectFile() as an alternative; however it intermittantly gives serialization errors.
    pyproject_ch = COPY_PYPROJECT(Channel.fromPath(pipeline_pyproject_path), "pyproject.toml")
    samplesheet_ch = COPY_SAMPLESHEET(Channel.fromPath(params.sample_sheet), "samplesheet.csv")
    adapters_ch = COPY_ADAPTERS(Channel.fromPath(params.adapters), "adapters.fasta")

    emit:
        input_run = index_params_ch.mix(samplesheet_ch, adapters_ch, params_ch)
        logging_run = index_pyproject_ch.mix(time_ch, pyproject_ch)
        intermediates_run = inter_lca.mix(inter_aligner)
        reads_raw_viral = bbduk_match
        reads_trimmed_viral = bbduk_trimmed
        // Lots of results; split across 2 channels (QC, and other)
        qc_results_run = COUNT_TOTAL_READS.out.read_counts.mix(RUN_QC.out.qc_basic, RUN_QC.out.qc_adapt, RUN_QC.out.qc_qbase, RUN_QC.out.qc_qseqs, RUN_QC.out.qc_lengths)
        other_results_run = hits_final.mix(PROFILE.out.bracken, PROFILE.out.kraken)
}
