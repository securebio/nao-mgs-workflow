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
        // For each (sample, suffix), construct the expected path and check existence.
        // This avoids the O(N²) explosion of globbing all files then combining with
        // all samples: instead we do O(N × suffixes) direct path probes.
        candidates_ch = groups
            .combine(run_dirs, by: 0)    // [label, sample, group, dir]
            .combine(suffixes_ch)        // [label, sample, group, dir, suffixes_str]
            .flatMap { label, sample, group, dir, suffixes_str ->
                def resolved = dir.endsWith('/') ? dir : "${dir}/"
                suffixes_str.split(',').collect { suffix ->
                    def gz_path = file("${resolved}${sample}_${suffix}.gz")
                    def plain_path = file("${resolved}${sample}_${suffix}")
                    def found = gz_path.exists() ? gz_path : (plain_path.exists() ? plain_path : null)
                    tuple(label, sample, group, suffix, found)
                }
            }
        // Validate all expected files were found, then emit output tuples
        validated_output_ch = candidates_ch
            .toList()
            .flatMap { all_candidates ->
                def missing = all_candidates
                    .findAll { it[4] == null }
                    .collect { "${it[0]}\t${it[1]}\t${it[3]}" }
                if (missing) {
                    def unique_missing = (missing as Set).sort()
                    def formatted = unique_missing.collect { it.replace('\t', ' / ') }.join('\n  ')
                    throw new RuntimeException(
                        "Missing ${unique_missing.size()} expected RUN output file(s) in run_results_dir:\n  " +
                        "${formatted}\n" +
                        "Ensure the RUN workflow has completed and all files are available."
                    )
                }
                all_candidates.collect { label, sample, group, _suffix, found ->
                    tuple(label, sample, found, group)
                }
            }

    emit:
        output = validated_output_ch  // tuple(label, sample, file, group)
}
