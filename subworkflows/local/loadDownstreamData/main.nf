/***********
| WORKFLOW |
***********/

workflow LOAD_DOWNSTREAM_DATA {
    take:
        input_file
        input_base_dir  // Base directory for resolving relative paths in input CSV (use projectDir or launchDir)
    main:
        // Start time
        start_time = new Date()
        start_time_str = start_time.format("YYYY-MM-dd HH:mm:ss z (Z)")

        // Validate headers
        def required_headers = ['label', 'hits_tsv', 'groups_tsv']
        def headers = file(input_file).readLines().first().tokenize(',')*.trim()
        if (headers != required_headers) {
            throw new Exception("""Invalid input header.
                Expected: ${required_headers.join(', ')}
                Found: ${headers.join(', ')}
                Please ensure the input file has the correct columns in the specified order.""".stripIndent())
        }

        // Helper to resolve paths: absolute and S3 paths used as-is, relative paths resolved against input_base_dir
        def resolvePath = { path ->
            (path.startsWith('s3://') || path.startsWith('/')) ? file(path) : file(input_base_dir, path)
        }

        // Construct input channel
        input_ch = Channel.fromPath(input_file).splitCsv(header: true)
            | map { row -> tuple(row.label, resolvePath(row.hits_tsv), resolvePath(row.groups_tsv)) }

    emit:
        input = input_ch
        start_time_str = start_time_str
        test_input = input_file
}
