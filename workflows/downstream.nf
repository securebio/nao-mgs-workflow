/***********************************************************
| WORKFLOW: DOWNSTREAM ANALYSIS OF PRIMARY WORKFLOW OUTPUT |
***********************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { LOAD_DOWNSTREAM_DATA } from "../subworkflows/local/loadDownstreamData"
include { DISCOVER_RUN_OUTPUT } from "../subworkflows/local/discoverRunOutput"
include { CONCAT_RUN_OUTPUTS_BY_GROUP } from "../subworkflows/local/concatRunOutputsByGroup"
include { MARK_VIRAL_DUPLICATES } from "../subworkflows/local/markViralDuplicates"
include { VALIDATE_VIRAL_ASSIGNMENTS } from "../subworkflows/local/validateViralAssignments"
include { COUNT_READS_PER_CLADE } from "../modules/local/countReadsPerClade"
include { COPY_FILE_BARE as COPY_PYPROJECT } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_INPUT } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_TIME } from "../modules/local/copyFile"
include { SORT_TSV as SORT_ONT_HITS } from "../modules/local/sortTsv"
include { ADD_FIXED_COLUMN as PAD_ONT_COLUMNS } from "../modules/local/addFixedColumn"

/*****************
| MAIN WORKFLOWS |
*****************/

workflow DOWNSTREAM {
    main:
        // Prepare channels from input CSV file
        load_ch = LOAD_DOWNSTREAM_DATA(params.input_file, params.input_base_dir ?: projectDir)
        start_time_str = load_ch.start_time_str
        // Discover all per-sample output files and match to groups
        pipeline_pyproject_path = file("${projectDir}/pyproject.toml")
        discover_ch = DISCOVER_RUN_OUTPUT(load_ch.run_dirs, load_ch.groups, pipeline_pyproject_path).output
        // Concatenate per-sample outputs into per-group TSVs
        concat_ch = CONCAT_RUN_OUTPUTS_BY_GROUP(discover_ch)
        // Prepare inputs for clade counting and validating taxonomic assignments
        viral_db_path = "${params.ref_dir}/results/total-virus-db-annotated.tsv.gz"
        viral_db = channel.value(viral_db_path)
        // Conditionally mark duplicates and generate clade counts based on platform
        if (params.platform == "ont") {
            // ONT: Skip duplicate marking and clade counting, but still sort by seq_id
            viral_hits_ch = SORT_ONT_HITS(concat_ch.hits, "seq_id").sorted
            // Pad with paired-end columns (NA) so ONT and short-read share the same column set
            def pad_cols = [
                "query_len_rev", "query_seq_rev", "query_qual_rev",
                "prim_align_fragment_length",
                "prim_align_best_alignment_score_rev",
                "prim_align_edit_distance_rev",
                "prim_align_ref_start_rev", "prim_align_query_rc_rev",
                "prim_align_pair_status", "prim_align_dup_exemplar"
            ].join(",")
            viral_hits_ch = PAD_ONT_COLUMNS(viral_hits_ch, pad_cols, "NA", "padded").output
            dup_output_ch = Channel.empty()
            clade_counts_ch = Channel.empty()
        }
        else {
            // Short-read: Mark duplicates based on alignment coordinates
            MARK_VIRAL_DUPLICATES(concat_ch.hits, params.aln_dup_deviation)
            viral_hits_ch = MARK_VIRAL_DUPLICATES.out.dup.map { label, tab, _stats -> [label, tab] }
            dup_output_ch = MARK_VIRAL_DUPLICATES.out.dup.map { label, _reads, stats -> [label, stats] }
            // Generate clade counts
            clade_counts_ch = COUNT_READS_PER_CLADE(viral_hits_ch, viral_db).output
        }
        // Validate taxonomic assignments
        def validation_params = params.collectEntries { k, v -> [k, v] }
        validation_params["cluster_min_len"] = 15
        validate_ch = VALIDATE_VIRAL_ASSIGNMENTS(viral_hits_ch, viral_db, params.ref_dir, validation_params)
        // Prepare publishing channels
        params_str = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(params))
        params_ch = Channel.of(params_str).collectFile(name: "params-downstream.json")
        pyproject_ch = COPY_PYPROJECT(Channel.fromPath(pipeline_pyproject_path), "pyproject.toml")
        input_file_ch = COPY_INPUT(Channel.fromPath(params.input_file), "input_file.csv")
        time_file = start_time_str.map { it + "\n" }.collectFile(name: "time.txt")
        time_ch = COPY_TIME(time_file, "time.txt")

    emit:
        input_downstream = params_ch.mix(input_file_ch)
        logging_downstream = time_ch.mix(pyproject_ch)
        intermediates_downstream = validate_ch.blast_results
        results_downstream = dup_output_ch.mix(
                                clade_counts_ch,
                                validate_ch.annotated_hits,
                                concat_ch.other)
}
