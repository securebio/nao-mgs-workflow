/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { COMBINE_SAMPLE_JSONS } from "../../../modules/local/combineSampleJsons"
include { COPY_FILE } from "../../../modules/local/copyFile"

/***********
| WORKFLOW |
***********/

workflow CONCAT_JSON_BY_GROUP {
    take:
        files        // tuple(label, sample, file, group)
        suffix       // string file suffix to filter on, e.g. "fastp.json"
        output_name  // string, e.g. "fastp"
    main:
        // Filter for files matching the suffix using exact matching.
        // Unlike CONCAT_BY_GROUP, .gz variants are not matched here because
        // JSON outputs (e.g. fastp) are never gzipped.
        filtered = files
            .filter { _label, sample, f, _group ->
                def name = f.getFileName().toString()
                name == "${sample}_${suffix}"
            }
        // Group files by group and combine
        files_by_group = filtered
            .map { _label, _sample, file, group -> [group, file] }
            .groupTuple()
        combined_ch = COMBINE_SAMPLE_JSONS(files_by_group, suffix).output
        // Rename to final output name: {group}_{output_name}.json
        renamed_ch = COPY_FILE(combined_ch, "${output_name}.json")
    emit:
        groups = renamed_ch
}
