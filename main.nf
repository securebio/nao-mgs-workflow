include { RUN } from "./workflows/run"
include { INDEX } from "./workflows/index"
include { DOWNSTREAM } from "./workflows/downstream"

workflow {
    main:
        if (params.mode == "index") {
          index_out = INDEX()
        } else if (params.mode == "run") {
            run_out = RUN()
        } else if (params.mode == "downstream") {
            downstream_out = DOWNSTREAM()
        }
    publish:
        // Conditional publish blocks are not allowed; hence the ternary operators
        // INDEX workflow publishing
        input_index = params.mode == 'index' ? index_out.input_index : channel.empty()
        logging_index = params.mode == 'index' ? index_out.logging_index : channel.empty()
        ref_dbs = params.mode == 'index' ? index_out.ref_dbs : channel.empty()
        alignment_indexes = params.mode == 'index' ? index_out.alignment_indexes : channel.empty()
        experimental_index = params.mode == 'index' ? index_out.experimental_index : channel.empty()
        // RUN workflow publishing
        input_run = params.mode == 'run' ? run_out.input_run : channel.empty()
        logging_run = params.mode == 'run' ? run_out.logging_run : channel.empty()
        intermediates_run = params.mode == 'run' ? run_out.intermediates_run : channel.empty()
        reads_raw_viral = params.mode == 'run' ? run_out.reads_raw_viral : channel.empty()
        reads_trimmed_viral = params.mode == 'run' ? run_out.reads_trimmed_viral : channel.empty()
        qc_results_run = params.mode == 'run' ? run_out.qc_results_run : channel.empty()
        other_results_run = params.mode == 'run' ? run_out.other_results_run : channel.empty()
        experimental_run = params.mode == 'run' ? run_out.experimental_run : channel.empty()
        sentinel_run = params.mode == 'run' ? run_out.sentinel_run : channel.empty()
        // DOWNSTREAM workflow publishing
        input_downstream = params.mode == 'downstream' ? downstream_out.input_downstream  : channel.empty()
        logging_downstream = params.mode == 'downstream' ? downstream_out.logging_downstream  : channel.empty()
        intermediates_downstream = params.mode == 'downstream' ? downstream_out.intermediates_downstream  : channel.empty()
        results_downstream = params.mode == 'downstream' ? downstream_out.results_downstream  : channel.empty()
        experimental_downstream = params.mode == 'downstream' ? downstream_out.experimental_downstream  : channel.empty()
        sentinel_downstream = params.mode == 'downstream' ? downstream_out.sentinel_downstream  : channel.empty()
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
    experimental_index {
        path "experimental"
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
    experimental_run {
        // Publish the domain-abundance table under a name that reflects its
        // Kraken2 source, renamed in place with `>>` (no copy process).
        path { sample, file -> file >> "experimental/${sample}_kraken_domains.tsv.gz" }
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    sentinel_run {
        path "logging"
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
    experimental_downstream {
        path "experimental_downstream"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
    sentinel_downstream {
        path "logging_downstream"
        tags nextflow_file_class: "publish", "nextflow.io/temporary": "false"
    }
}
