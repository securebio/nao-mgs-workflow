/***********
| WORKFLOW |
***********/

workflow LOAD_DOWNSTREAM_DATA {
    take:
        input_file
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
        // Parse input CSV rows
        rows_ch = Channel.fromPath(input_file).splitCsv(header: true)
        // Parse groups files to get (label, sample, group) tuples
        groups_ch = rows_ch
            .map { row -> tuple(row.label, file(row.groups_tsv)) }
            .flatMap { label, groups_file ->
                groups_file.splitCsv(sep: '\t', header: true).collect { gRow ->
                    tuple(label, gRow.sample, gRow.group)
                }
            }
        // Discover per-sample virus_hits files: (label, sample, hits_file)
        hits_ch = rows_ch.flatMap { row ->
            def results_dir = row.results_dir.endsWith('/') ? row.results_dir : "${row.results_dir}/"
            def hits_files = file("${results_dir}*_virus_hits.tsv.gz")
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
    emit:
        hits = hits_with_groups  // tuple(label, sample, hits_file, group)
        groups = rows_ch.map { row -> tuple(row.label, file(row.groups_tsv)) }  // for validation
        start_time_str = start_time_str
        test_input = input_file
}
