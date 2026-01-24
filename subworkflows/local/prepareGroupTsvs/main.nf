/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { SORT_TSV as SORT_INPUT } from "../../../modules/local/sortTsv"
include { SORT_TSV as SORT_GROUPS } from "../../../modules/local/sortTsv"
include { SORT_TSV as SORT_JOINED_GROUPS } from "../../../modules/local/sortTsv"
include { JOIN_TSVS } from "../../../modules/local/joinTsvs"
include { VALIDATE_GROUPING } from "../../../modules/local/validateGrouping"
include { PARTITION_TSV } from "../../../modules/local/partitionTsv"
include { CONCATENATE_TSVS_LABELED } from "../../../modules/local/concatenateTsvs"
include { CONCATENATE_TSVS_LABELED as CONCATENATE_PARTIAL_GROUP_LOGS } from "../../../modules/local/concatenateTsvs"
include { CONCATENATE_TSVS_LABELED as CONCATENATE_EMPTY_GROUP_LOGS } from "../../../modules/local/concatenateTsvs"

/***********
| WORKFLOW |
***********/

workflow PREPARE_GROUP_TSVS {
    take:
        input_files
    main:
        // 1. Sort inputs by sample ID
        input_ch = input_files.map{ label, input, _groups -> tuple(label, input) }
        groups_ch = input_files.map{ label, _input, groups -> tuple(label, groups) }
        input_sorted_ch = SORT_INPUT(input_ch, "sample").sorted
        groups_sorted_ch = SORT_GROUPS(groups_ch, "sample").sorted
        combined_sorted_ch = input_sorted_ch.combine(groups_sorted_ch, by: 0)
        // 2. Validate grouping and filter zero-VV samples
        validated_grouping_ch = VALIDATE_GROUPING(combined_sorted_ch).output
        partial_group_log_ch = VALIDATE_GROUPING.out.partial_group_log
        empty_group_log_ch = VALIDATE_GROUPING.out.empty_group_log
        // 3. Join with inner join (guaranteed to succeed after validation)
        joined_ch = JOIN_TSVS(validated_grouping_ch, "sample", "inner", "input").output
        joined_sorted_ch = SORT_JOINED_GROUPS(joined_ch, "group").sorted
        // 4. Partition each TSV by group ID
        partitioned_ch = PARTITION_TSV(joined_sorted_ch, "group").output
        // 5. Restructure channel so all files with the same group ID are together
        // First rearrange each element from [sample, [paths]] to [[group1, path1], [group2, path2], ...]
        partitioned_flattened_ch = partitioned_ch.flatMap{
            sample, filepaths ->
                def pathlist = (filepaths instanceof List) ? filepaths : [filepaths]
                pathlist.collect { path ->
                    def filename = path.last()
                    def pattern = "^partition_(.*?)_sorted_group_${sample}_input_inner_joined_sample\\.tsv\\.gz\$"
                    def matcher = (filename =~ pattern)
                    if (!matcher) {
                        def msg = "Filename doesn't match required pattern: ${sample}, ${path}, ${path.last()}"
                        throw new IllegalArgumentException(msg)
                    }
                    [matcher[0][1], path]
                }
            }
        // Then rearrange channel to [[group1, [paths]], [group2, [paths]], ...]
        partitioned_grouped_ch = partitioned_flattened_ch.groupTuple()
        // 6. Concatenate TSVs for each group
        concat_ch = CONCATENATE_TSVS_LABELED(partitioned_grouped_ch, "grouped").output
        // 7. Concatenate partial group logs (empty samples with non-empty group-mates)
        partial_log_grouped_ch = partial_group_log_ch.map { _label, file -> ["all_samples", file] }.groupTuple()
        consolidated_partial_ch = CONCATENATE_PARTIAL_GROUP_LOGS(partial_log_grouped_ch, "partial_group_consolidated").output
        // 8. Concatenate empty group logs (empty samples with empty group-mates only)
        empty_log_grouped_ch = empty_group_log_ch.map { _label, file -> ["all_samples", file] }.groupTuple()
        consolidated_empty_ch = CONCATENATE_EMPTY_GROUP_LOGS(empty_log_grouped_ch, "empty_group_consolidated").output
    emit:
        groups = concat_ch
        partial_group_logs = consolidated_partial_ch
        empty_group_logs = consolidated_empty_ch
        test_in = input_files
        test_join = joined_ch
        test_part = partitioned_ch
        test_grps = partitioned_grouped_ch
}
