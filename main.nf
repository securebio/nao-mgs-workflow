include { RUN } from "./workflows/run"
include { RUN_VALIDATION } from "./workflows/run_validation"
include { INDEX } from "./workflows/index"
include { DOWNSTREAM } from "./workflows/downstream"

workflow {
    main:
        if (params.mode == "index") {
          INDEX()
        } else if (params.mode == "run") {
            RUN()
        } else if (params.mode == "run_validation") {
            RUN_VALIDATION()
        } else if (params.mode == "downstream") {
            DOWNSTREAM()
        }
    publish:
        // Conditional publish blocks are not allowed; hence the ternary operators
        // INDEX workflow publishing
        input_index = params.mode == 'index' ? INDEX.out.input_index : Channel.empty()
        logging_index = params.mode == 'index' ? INDEX.out.logging_index : Channel.empty()
        ref_dbs = params.mode == 'index' ? INDEX.out.ref_dbs : Channel.empty()
        alignment_indexes = params.mode == 'index' ? INDEX.out.alignment_indexes : Channel.empty()
        // RUN workflow publishing
        input_run = params.mode == 'run' ? RUN.out.input_run : Channel.empty()
        logging_run = params.mode == 'run' ? RUN.out.logging_run : Channel.empty()
        intermediates_run = params.mode == 'run' ? RUN.out.intermediates_run : Channel.empty()
        reads_raw_viral = params.mode == 'run' ? RUN.out.reads_raw_viral : Channel.empty()
        reads_trimmed_viral = params.mode == 'run' ? RUN.out.reads_trimmed_viral : Channel.empty()
        qc_results_run = params.mode == 'run' ? RUN.out.qc_results_run : Channel.empty()
        other_results_run = params.mode == 'run' ? RUN.out.other_results_run : Channel.empty()
        // RUN_VALIDATION workflow publishing
        input_validation = params.mode == 'run_validation' ? RUN_VALIDATION.out.input_validation  : Channel.empty()
        logging_validation = params.mode == 'run_validation' ? RUN_VALIDATION.out.logging_validation  : Channel.empty()
        results_validation = params.mode == 'run_validation' ? RUN_VALIDATION.out.results_validation  : Channel.empty()
        // DOWNSTREAM workflow publishing
        input_downstream = params.mode == 'downstream' ? DOWNSTREAM.out.input_downstream  : Channel.empty()
        logging_downstream = params.mode == 'downstream' ? DOWNSTREAM.out.logging_downstream  : Channel.empty()
        intermediates_downstream = params.mode == 'downstream' ? DOWNSTREAM.out.intermediates_downstream  : Channel.empty()
        results_downstream = params.mode == 'downstream' ? DOWNSTREAM.out.results_downstream  : Channel.empty()
}
        
output {
    // INDEX workflow output
    input_index {
        path "input"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    logging_index {
        path "logging"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    ref_dbs {
        path "results"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    alignment_indexes {
        path "results"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    // RUN workflow output
    input_run {
        path "input"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    logging_run {
        path "logging"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    intermediates_run {
        path "intermediates"
        tags nextflow_file_class: "intermediate", "nextflow.io/temporary": "false"
    }
    reads_raw_viral {
        path "intermediates/reads/raw_viral"
        tags nextflow_file_class: "intermediate", "nextflow.io/temporary": "false"
    }
    reads_trimmed_viral {
        path "intermediates/reads/trimmed_viral"
        tags nextflow_file_class: "intermediate", "nextflow.io/temporary": "false"
    }
    qc_results_run {
        path "results"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    other_results_run {
        path "results"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    // RUN_VALIDATION workflow output
    input_validation {
        path "input"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    logging_validation {
        path "logging"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    results_validation {
        path "results"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    // DOWNSTREAM workflow output
    input_downstream {
        path "input_downstream"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    logging_downstream {
        path "logging_downstream"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    intermediates_downstream {
        path "intermediates_downstream"
        tags nextflow_file_class: "intermediate", "nextflow.io/temporary": "false"
    }
    results_downstream {
        path "results_downstream"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
}
