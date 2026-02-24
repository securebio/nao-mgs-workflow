/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { GET_RUN_OUTPUT_SUFFIXES } from "../../../modules/local/getRunOutputSuffixes"

/***********
| WORKFLOW |
***********/

workflow DISCOVER_RUN_OUTPUT {
    take:
        run_dirs        // Channel of tuple(label, resolved_run_results_dir), unique
        groups          // Channel of tuple(label, sample, group)
        pyproject_path  // Path to pyproject.toml
    main:
        // Extract valid per-sample output suffixes from pyproject.toml
        suffixes_ch = GET_RUN_OUTPUT_SUFFIXES(pyproject_path).suffixes  // comma-separated string
        // Discover all TSV files from each run_results_dir
        all_files_ch = run_dirs.flatMap { label, dir ->
            def resolved = dir.endsWith('/') ? dir : "${dir}/"
            def files = file("${resolved}*.tsv{,.gz}")
            def file_list = files instanceof List ? files : (files ? [files] : [])
            file_list.collect { f -> tuple(label, f) }
        }
        // Match files to samples using exact suffix matching to avoid ambiguity
        // when one sample name is a prefix of another (e.g. "s1" vs "s1_extra")
        output_ch = all_files_ch
            .combine(groups, by: 0)  // [label, file, sample, group]
            .combine(suffixes_ch)    // [label, file, sample, group, suffixes_str]
            .filter { _label, f, sample, _group, suffixes_str ->
                def filename = f.getFileName().toString()
                suffixes_str.split(',').any { suffix -> filename == "${sample}_${suffix}" || filename == "${sample}_${suffix}.gz" }
            }
            .map { label, f, sample, group, _suffixes_str -> tuple(label, sample, f, group) }
        // Validate that all expected files were found: for each (label, sample),
        // every expected suffix from pyproject.toml should have a matching file.
        // This catches incomplete run_results_dir (e.g. files not yet copied to S3).
        found_set_ch = output_ch
            .map { label, sample, f, _group ->
                def filename = f.getFileName().toString()
                def suffix = filename.substring(sample.length() + 1)
                if (suffix.endsWith(".gz")) suffix = suffix[0..-4]
                "${label}\t${sample}\t${suffix}"
            }
            .collect().ifEmpty([]).map { ["key", it as Set] }
        expected_set_ch = groups
            .combine(suffixes_ch)
            .flatMap { label, sample, _group, suffixes_str ->
                suffixes_str.split(',').collect { suffix ->
                    "${label}\t${sample}\t${suffix}"
                }
            }
            .collect().ifEmpty([]).map { ["key", it as Set] }
        found_set_ch.join(expected_set_ch).subscribe { _key, found, expected ->
            def missing = expected - found
            if (missing) {
                def formatted = missing.sort().collect { it.replace('\t', ' / ') }.join('\n  ')
                throw new RuntimeException(
                    "Missing ${missing.size()} expected RUN output file(s) in run_results_dir:\n  " +
                    "${formatted}\n" +
                    "Ensure the RUN workflow has completed and all files are available."
                )
            }
        }

    emit:
        output = output_ch  // tuple(label, sample, file, group)
}
