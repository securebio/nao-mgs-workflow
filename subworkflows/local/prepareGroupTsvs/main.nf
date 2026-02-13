/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { ADD_SAMPLE_COLUMN as ADD_GROUP_COLUMN } from "../../../modules/local/addSampleColumn"
include { CONCATENATE_TSVS_LABELED } from "../../../modules/local/concatenateTsvs"

/***********
| WORKFLOW |
***********/

workflow PREPARE_GROUP_TSVS {
    take:
        hits    // tuple(label, sample, hits_file, group)
    main:
        // 1. Group hits by group and concatenate
        hits_by_group = hits
            .map { _label, _sample, hits_file, group -> [group, hits_file] }
            .groupTuple()
        concatenated_ch = CONCATENATE_TSVS_LABELED(hits_by_group, "grouped").output

        // 2. Add group column to concatenated hits
        grouped_ch = ADD_GROUP_COLUMN(concatenated_ch, "group", "with_group").output

    emit:
        groups = grouped_ch
}
