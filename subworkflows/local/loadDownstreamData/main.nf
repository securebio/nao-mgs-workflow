/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { CONCATENATE_TSVS_LABELED } from "../../../modules/local/concatenateTsvs"

/***********
| WORKFLOW |
***********/

workflow LOAD_DOWNSTREAM_DATA {
    take:
        input_file
        input_base_dir  // Base directory for resolving relative paths in input CSV
    main:
        // Start time
        start_time = new Date()
        start_time_str = start_time.format("YYYY-MM-dd HH:mm:ss z (Z)")

        // Validate headers
        def required_headers = ['label', 'run_results_dir', 'groups_tsv']
        def headers = file(input_file).readLines().first().tokenize(',')*.trim()
        if (headers != required_headers) {
            throw new Exception("""Invalid input header.
                Expected: ${required_headers.join(', ')}
                Found: ${headers.join(', ')}
                Please ensure the input file has the correct columns in the specified order.""".stripIndent())
        }

        // Helper to resolve paths: absolute and S3 paths used as-is, relative paths resolved against input_base_dir
        def resolvePath = { path ->
            (path.startsWith('s3://') || path.startsWith('/')) ? file(path) : file(input_base_dir).resolve(path)
        }

        // Parse input CSV rows
        rows_ch = Channel.fromPath(input_file).splitCsv(header: true)

        // Discover per-sample virus_hits files from run_results_dir and collect them per label
        files_ch = rows_ch.map { row ->
            if (!row.run_results_dir?.trim()) {
                throw new Exception("Missing or empty 'run_results_dir' for label '${row.label}' in input file.")
            }
            if (!row.groups_tsv?.trim()) {
                throw new Exception("Missing or empty 'groups_tsv' for label '${row.label}' in input file.")
            }
            // Resolve run_results_dir as a string to preserve S3 URIs (file().toString() strips the s3:// scheme)
            def run_results_dir = row.run_results_dir
            if (!run_results_dir.startsWith('s3://') && !run_results_dir.startsWith('/')) {
                run_results_dir = file(input_base_dir).resolve(run_results_dir).toString()
            }
            if (!run_results_dir.endsWith('/')) run_results_dir += '/'
            def hits_files = file("${run_results_dir}*_virus_hits.tsv{,.gz}")
            def files_list = (hits_files instanceof List) ? hits_files : (hits_files ? [hits_files] : [])
            tuple(row.label, files_list, resolvePath(row.groups_tsv))
        }

        // Concatenate per-sample files into single hits file per label
        concat_input = files_ch.map { label, files, _groups -> [label, files] }
        concatenated = CONCATENATE_TSVS_LABELED(concat_input, "virus_hits_combined")

        // Rejoin with groups file to produce output matching old interface
        groups_ch = files_ch.map { label, _files, groups -> [label, groups] }
        input_ch = concatenated.output.join(groups_ch)
            .map { label, hits, groups -> tuple(label, hits, groups) }

    emit:
        input = input_ch
        start_time_str = start_time_str
        test_input = input_file
}
