/***********
| WORKFLOW |
***********/

workflow DISCOVER_RUN_OUTPUT {
    take:
        run_dirs   // Channel of tuple(label, resolved_run_results_dir), unique
        groups     // Channel of tuple(label, sample, group)
    main:
        // Discover all TSV files from each run_results_dir
        all_files_ch = run_dirs.flatMap { label, dir ->
            def resolved = dir.endsWith('/') ? dir : "${dir}/"
            def files = file("${resolved}*.tsv{,.gz}")
            def file_list = files instanceof List ? files : (files ? [files] : [])
            file_list.collect { f -> tuple(label, f) }
        }
        // Match files to samples using groups channel (by filename prefix)
        output_ch = all_files_ch
            .combine(groups, by: 0)  // [label, file, sample, group]
            .filter { _label, f, sample, _group -> f.getFileName().toString().startsWith("${sample}_") }
            .map { label, f, sample, group -> tuple(label, sample, f, group) }

    emit:
        output = output_ch  // tuple(label, sample, file, group)
}
