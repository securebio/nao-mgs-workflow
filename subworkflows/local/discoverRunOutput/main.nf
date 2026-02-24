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

    emit:
        output = output_ch  // tuple(label, sample, file, group)
}
