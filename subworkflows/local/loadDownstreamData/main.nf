/***********
| WORKFLOW |
***********/

workflow LOAD_DOWNSTREAM_DATA {
    take:
        input_file
        input_base_dir  // Base directory for resolving relative paths in input CSV (use projectDir or launchDir)
    main:
        start_time = new Date()
        start_time_str = start_time.format("YYYY-MM-dd HH:mm:ss z (Z)")
        // Validate headers
        def required_headers = ['label', 'results_dir', 'groups_tsv']
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
        // Parse groups files to get (label, sample, group) tuples
        groups_ch = rows_ch
            .map { row -> tuple(row.label, resolvePath(row.groups_tsv)) }
            .flatMap { label, groups_file ->
                groups_file.splitCsv(sep: '\t', header: true).collect { gRow ->
                    tuple(label, gRow.sample, gRow.group)
                }
            }
        // Discover per-sample virus_hits files: (label, sample, hits_file)
        hits_ch = rows_ch.flatMap { row ->
            if (!row.results_dir?.trim()) {
                throw new Exception("Missing or empty 'results_dir' for label '${row.label}' in input file.")
            }
            if (!row.groups_tsv?.trim()) {
                throw new Exception("Missing or empty 'groups_tsv' for label '${row.label}' in input file.")
            }
            // Resolve results_dir as a string to preserve S3 URIs (file().toString() strips the s3:// scheme)
            def results_dir = row.results_dir
            if (!results_dir.startsWith('s3://') && !results_dir.startsWith('/')) {
                results_dir = file(input_base_dir).resolve(results_dir).toString()
            }
            if (!results_dir.endsWith('/')) results_dir += '/'
            def hits_files = file("${results_dir}*_virus_hits.tsv{,.gz}")
            if (hits_files instanceof List) {
                hits_files.collect { f ->
                    def sample = f.name.replace("_virus_hits.tsv.gz", "")
                    tuple(row.label, sample, f)
                }
            } else if (hits_files) {
                def sample = hits_files.name.replace("_virus_hits.tsv.gz", "")
                [tuple(row.label, sample, hits_files)]
            } else {
                []
            }
        }
        // Join hits with groups to get: (label, sample, hits_file, group)
        hits_with_groups = hits_ch
            .map { label, sample, hits_file -> tuple([label, sample], hits_file) }
            .join(groups_ch.map { label, sample, group -> tuple([label, sample], group) })
            .map { key, hits_file, group -> tuple(key[0], key[1], hits_file, group) }

        // Find groups with no hits by comparing all groups vs groups with hits
        all_groups = groups_ch.map { _label, _sample, group -> group }.unique().collect().map { ["key", it] }
        groups_with_hits = hits_with_groups.map { _label, _sample, _file, group -> group }.unique().collect().ifEmpty([]).map { ["key", it] }
        missing_groups = all_groups.join(groups_with_hits)
            .map { _key, all, with_hits -> (all as Set) - (with_hits as Set) }

    emit:
        hits = hits_with_groups  // tuple(label, sample, hits_file, group)
        missing_groups = missing_groups  // Set of group names with no hits
        start_time_str = start_time_str
        test_input = input_file
}
