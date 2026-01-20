/********************************************************
| WORKFLOW: POST-HOC VALIDATION OF PUTATIVE VIRAL READS |
********************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED as EXTRACT_FASTQ } from "../modules/local/extractViralHitsToFastqNoref"
include { BLAST_VIRAL } from "../subworkflows/local/blastViral"
include { COPY_FILE_BARE as COPY_PYPROJECT } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_INDEX_PARAMS } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_INDEX_PYPROJECT } from "../modules/local/copyFile"

/****************
| MAIN WORKFLOW |
****************/

// Complete primary workflow
workflow RUN_VALIDATION {
    main:
        // Start time
        start_time = new Date()
        start_time_str = start_time.format("YYYY-MM-dd HH:mm:ss z (Z)")

        // Get input FASTQ
        if ( params.viral_tsv == "" ) {
        // Option 1: Directly specify FASTQ path in config (interleaved/single-end)
            fastq_ch = Channel.fromPath(params.viral_fastq)
        } else {
        // Option 2: Extract read sequences from output DB from RUN workflow (default)
            // Define input
            tsv_ch = Channel.value(["viral_hits", file(params.viral_tsv)])
            fastq_out = EXTRACT_FASTQ(tsv_ch, params.drop_unpaired)
            fastq_ch = fastq_out.output.map { _label, fastq -> fastq }
        }

        // BLAST validation on host-viral reads
        def blast_viral_params = params.collectEntries { k, v -> [k, v] }
        blast_viral_params["read_fraction"] = params.blast_viral_fraction // rename to match subworkflow input
        BLAST_VIRAL(fastq_ch, params.ref_dir, blast_viral_params)

        // Prepare results for publishing
        params_str = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(params))
        params_ch = Channel.of(params_str).collectFile(name: "params-run.json")
        time_ch = Channel.of(start_time_str + "\n").collectFile(name: "time.txt")
        pipeline_pyproject_path = file("${projectDir}/pyproject.toml")
        pyproject_ch = COPY_PYPROJECT(Channel.fromPath(pipeline_pyproject_path), "pyproject.toml")
        index_params_ch = COPY_INDEX_PARAMS(Channel.fromPath("${params.ref_dir}/input/index-params.json"), "params-index.json")
        index_pyproject_ch = COPY_INDEX_PYPROJECT(Channel.fromPath("${params.ref_dir}/logging/pyproject.toml"), "pyproject-index.toml")

    emit:
        input_validation = index_params_ch.mix(params_ch)
        logging_validation = index_pyproject_ch.mix(time_ch, pyproject_ch)
        results_validation = BLAST_VIRAL.out.blast_subset.mix(BLAST_VIRAL.out.subset_reads)
}
