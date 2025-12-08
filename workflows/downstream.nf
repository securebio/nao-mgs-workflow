/***********************************************************
| WORKFLOW: DOWNSTREAM ANALYSIS OF PRIMARY WORKFLOW OUTPUT |
***********************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { LOAD_DOWNSTREAM_DATA } from "../subworkflows/local/loadDownstreamData"
include { PREPARE_GROUP_TSVS } from "../subworkflows/local/prepareGroupTsvs"
include { MARK_VIRAL_DUPLICATES } from "../subworkflows/local/markViralDuplicates"
include { VALIDATE_VIRAL_ASSIGNMENTS } from "../subworkflows/local/validateViralAssignments"
include { COUNT_READS_PER_CLADE } from "../modules/local/countReadsPerClade"
include { COPY_FILE_BARE as COPY_VERSION } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_INPUT } from "../modules/local/copyFile"
include { SORT_TSV as SORT_ONT_HITS } from "../modules/local/sortTsv"
/*****************
| MAIN WORKFLOWS |
*****************/

workflow DOWNSTREAM {
    main:
        // Prepare channel from input TSV file
        LOAD_DOWNSTREAM_DATA(params.input_file)
        input_ch = LOAD_DOWNSTREAM_DATA.out.input
        start_time_str = LOAD_DOWNSTREAM_DATA.out.start_time_str
        // Add group information, partition into per-group TSVs
        PREPARE_GROUP_TSVS(input_ch)
        group_ch = PREPARE_GROUP_TSVS.out.groups
        // Prepare inputs for clade counting and validating taxonomic assignments
        viral_db_path = "${params.ref_dir}/results/total-virus-db-annotated.tsv.gz"
        viral_db = channel.value(viral_db_path)
        // Conditionally mark duplicates and generate clade counts based on platform
        if (params.platform == "ont") {
            // ONT: Skip duplicate marking and clade counting, but still sort by seq_id
            sorted_ont_ch = SORT_ONT_HITS(group_ch, "seq_id").sorted
            viral_hits_ch = sorted_ont_ch
            dup_output_ch = Channel.empty()
            clade_counts_ch = Channel.empty()
        }
        else {
            // Short-read: Mark duplicates based on alignment coordinates
            MARK_VIRAL_DUPLICATES(group_ch, params.aln_dup_deviation)
            viral_hits_ch = MARK_VIRAL_DUPLICATES.out.dup.map { label, tab, _stats -> [label, tab] }
            dup_output_ch = MARK_VIRAL_DUPLICATES.out.dup
            // Generate clade counts
            clade_counts_ch = COUNT_READS_PER_CLADE(viral_hits_ch, viral_db)
        }
        // Validate taxonomic assignments
        def validation_params = params.collectEntries { k, v -> [k, v] }
        validation_params["cluster_min_len"] = 15
        VALIDATE_VIRAL_ASSIGNMENTS(viral_hits_ch, viral_db, params.ref_dir, validation_params)
        // Publish results
        params_str = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(params))
        params_ch = Channel.of(params_str).collectFile(name: "params-downstream.json")
        time_ch = start_time_str.map { it + "\n" }.collectFile(name: "time.txt")
        version_path = file("${projectDir}/pipeline-version.txt")
        version_newpath = version_path.getFileName().toString()
        version_ch = COPY_VERSION(Channel.fromPath(version_path), version_newpath)
        input_newpath = file(params.input_file).getFileName().toString()
        input_file_ch = COPY_INPUT(Channel.fromPath(params.input_file), input_newpath)

    emit:
       input_downstream = params_ch.mix(input_file_ch)
       logging_downstream = time_ch.mix(version_ch)
       intermediates_downstream = VALIDATE_VIRAL_ASSIGNMENTS.out.blast_results.mix(
                                        PREPARE_GROUP_TSVS.out.zero_vv_logs,
                                  )
        results_downstream = dup_output_ch.mix(
                                clade_counts_ch,
                                VALIDATE_VIRAL_ASSIGNMENTS.out.annotated_hits
        )
}
