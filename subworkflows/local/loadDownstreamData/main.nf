/***********
| WORKFLOW |
***********/

workflow LOAD_DOWNSTREAM_DATA {
    take:
        input_file
        input_base_dir  // Base directory for resolving relative paths in input CSV
    main:
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
        // Helpers to resolve paths: absolute and S3 paths used as-is, relative paths resolved against input_base_dir
        def resolvePath = { path ->
            (path.startsWith('s3://') || path.startsWith('/')) ? file(path) : file(input_base_dir).resolve(path)
        }
        def resolveDir = { dir ->
            (dir.startsWith('s3://') || dir.startsWith('/')) ? dir : file(input_base_dir).resolve(dir).toString()
        }
        // Parse and validate input CSV rows
        rows_ch = Channel.fromPath(input_file).splitCsv(header: true)
            .map { row ->
                if (!row.run_results_dir?.trim()) {
                    throw new Exception("Missing or empty 'run_results_dir' for label '${row.label}' in input file.")
                }
                if (!row.groups_tsv?.trim()) {
                    throw new Exception("Missing or empty 'groups_tsv' for label '${row.label}' in input file.")
                }
                return row
            }
        // Unique resolved run_results_dir per label
        run_dirs_ch = rows_ch
            .map { row -> tuple(row.label, resolveDir(row.run_results_dir)) }
            .unique()
        // Parse groups files to get (label, sample, group) tuples
        groups_ch = rows_ch
            .map { row -> tuple(row.label, resolvePath(row.groups_tsv)) }
            .flatMap { label, groups_file ->
                groups_file.splitCsv(sep: '\t', header: true).collect { gRow ->
                    tuple(label, gRow.sample, gRow.group)
                }
            }
    emit:
        run_dirs = run_dirs_ch   // tuple(label, resolved_run_results_dir)
        groups = groups_ch       // tuple(label, sample, group)
        start_time_str = start_time_str
        test_input = input_file
}
