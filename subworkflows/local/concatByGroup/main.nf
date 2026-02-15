/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { ADD_SAMPLE_COLUMN as ADD_GROUP_COLUMN } from "../../../modules/local/addSampleColumn"
include { CONCATENATE_TSVS_LABELED } from "../../../modules/local/concatenateTsvs"
include { COPY_FILE } from "../../../modules/local/copyFile"

/***********
| WORKFLOW |
***********/

workflow CONCAT_BY_GROUP {
    take:
        files        // tuple(label, sample, file, group)
        suffix       // string file suffix to filter on, e.g. "virus_hits.tsv"
        output_name  // string, e.g. "grouped_hits"
    main:
        // Filter for files matching the suffix
        def escaped_suffix = suffix.replace('.', '\\.')
        filtered = files
            .filter { _label, _sample, f, _group -> f.name ==~ /.*_${escaped_suffix}(\.gz)?$/ }
        // Group files by group and concatenate
        files_by_group = filtered
            .map { _label, _sample, file, group -> [group, file] }
            .groupTuple()
        concatenated_ch = CONCATENATE_TSVS_LABELED(files_by_group, "concat").output
        // Add group column to concatenated files
        grouped_ch = ADD_GROUP_COLUMN(concatenated_ch, "group", "grouped").output
        // Rename to final output name: {group}_{output_name}.tsv.gz
        renamed_ch = COPY_FILE(grouped_ch, "${output_name}.tsv.gz")
    emit:
        groups = renamed_ch
}
