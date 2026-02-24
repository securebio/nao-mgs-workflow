/***********
| WORKFLOW |
***********/

workflow LOAD_SAMPLESHEET {
    take:
        sample_sheet
        platform
        development_mode // less strict validation for platform/endedness
    main:
        // Start time
        start_time = new Date()
        start_time_str = start_time.format("yyyy-MM-dd HH:mm:ss z (Z)")

        // Check pairing and validate headers
        def headers = file(sample_sheet).readLines().first().tokenize(',')*.trim()
        def expected_headers_se = ['sample', 'fastq']
        def expected_headers_pe = ['sample', 'fastq_1', 'fastq_2']
        if (headers.take(2) == expected_headers_se) {
            single_end = true
        } else if (headers.take(3) == expected_headers_pe) {
            single_end = false
        } else {
            throw new Exception("""Invalid samplesheet header. 
                Expected ${expected_headers_se.join(', ')} or ${expected_headers_pe.join(', ')}
                Found: ${headers.join(', ')}
                Please ensure the samplesheet has the correct columns in the specified order.""".stripIndent())
        }

        // Check platform is allowed and has allowed endedness
        def allowed_platforms = ['illumina', 'ont', 'pacbio', 'aviti']
        def allowed_endedness = ['both', 'single', 'both', 'both']
        def platform_index = allowed_platforms.indexOf(platform)
        if (platform_index < 0) {
            throw new Exception("""Invalid sequencing platform.
                Expected: ${allowed_platforms.join(', ')}
                Found: ${platform}
                Please ensure the specified sequencing platform is in the list of allowed platforms.""".stripIndent())

        }
        def endedness = allowed_endedness[platform_index]
        if (endedness == "single" && !single_end) {
            throw new Exception("Platform '${platform}' requires single-end data.")
        }
        if (endedness == "paired" && single_end) {
            throw new Exception("Platform '${platform}' requires paired-end data.")
        }

        // If not in development mode, check if pipeline is implemented for specified platform and endedness
        if (!development_mode) {
            def implemented_platforms = ['illumina', 'aviti', 'ont']
            def implemented_endedness = ['paired', 'paired', 'single']
            def platform_index_2 = implemented_platforms.indexOf(platform)
            if (platform_index_2 < 0) {
                throw new Exception("""Pipeline not yet implemented in production for platform '${platform}'.
                    Permitted platforms: ${implemented_platforms.join(", ")}""")
            }
            def endedness_2 = implemented_endedness[platform_index_2]
            if (endedness_2 == "single" && !single_end) {
                throw new Exception("Pipeline is only implemented in production for platform '${platform}' for single-end data.")
            }
            if (endedness_2 == "paired" && single_end) {
                throw new Exception("Pipeline is only implemented in production for platform '${platform}' for paired-end data.")
            }
        }
        // Construct samplesheet channel
        if (single_end) {
            samplesheet = Channel
                .fromPath(sample_sheet)
                .splitCsv(header: true)
                .map { row -> tuple(row.sample, file(row.fastq)) }
            samplesheet_ch = samplesheet.map { sample, read -> tuple(sample, [read]) }
        } else {
            samplesheet = Channel
                .fromPath(sample_sheet)
                .splitCsv(header: true)
                .map { row -> tuple(row.sample, file(row.fastq_1), file(row.fastq_2)) }
            samplesheet_ch = samplesheet.map { sample, read1, read2 -> tuple(sample, [read1, read2]) }
        }

    emit:
        single_end = single_end
        samplesheet = samplesheet_ch
        start_time_str = start_time_str
        test_input = sample_sheet
}
