/*************************************************************
| SUBWORKFLOW: PREPARE INPUT AND LOGGING FILES FOR PUBLISHING |
*************************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { COPY_FILE_BARE as COPY_INDEX_PARAMS } from "../../../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_INDEX_PYPROJECT } from "../../../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_PYPROJECT } from "../../../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_SAMPLESHEET } from "../../../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_ADAPTERS } from "../../../modules/local/copyFile"

/***********
| WORKFLOW |
***********/

workflow PREPARE_INPUT_LOGGING {
    take:
        params_map              // Map: full params object
        index_pyproject_path    // Channel: from CHECK_VERSION_COMPATIBILITY.out.index_pyproject_path
        pipeline_pyproject_path // Channel: from CHECK_VERSION_COMPATIBILITY.out.pipeline_pyproject_path
        start_time_str          // Value channel: from LOAD_SAMPLESHEET.out.start_time_str
    main:
        // Serialize run params to JSON
        params_str = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(params_map))
        params_ch = Channel.of(params_str).collectFile(name: "params-run.json")

        // Copy index files for publishing
        index_params_path = file("${params_map.ref_dir}/input/index-params.json")
        index_params_ch = COPY_INDEX_PARAMS(Channel.fromPath(index_params_path), "params-index.json")
        index_pyproject_ch = COPY_INDEX_PYPROJECT(index_pyproject_path, "pyproject-index.toml")

        // Prepare time log
        time_ch = start_time_str.map { it + "\n" }.collectFile(name: "time.txt")

        // Copy pipeline files through work dir for NF 25.04 publishing compatibility
        // (nextflow 25.04 only publishes files that have passed through the working directory;
        //  collectFile() was tried as an alternative but intermittently gives serialization errors)
        pyproject_ch = COPY_PYPROJECT(pipeline_pyproject_path, "pyproject.toml")
        samplesheet_ch = COPY_SAMPLESHEET(Channel.fromPath(params_map.sample_sheet), "samplesheet.csv")
        adapters_ch = COPY_ADAPTERS(Channel.fromPath(params_map.adapters), "adapters.fasta")
    emit:
        input_run = index_params_ch.mix(samplesheet_ch, adapters_ch, params_ch)
        logging_run = index_pyproject_ch.mix(time_ch, pyproject_ch)
}
