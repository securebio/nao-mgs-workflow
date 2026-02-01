/**************************************************
| SUBWORKFLOW: CONCATENATE & SUBSET RAW MGS READS |
**************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/
include { COUNT_READS } from "../../../modules/local/countReads"

/***********
| WORKFLOW |
***********/

workflow COUNT_TOTAL_READS {
    take:
        samplesheet_ch
        single_end
    main:
        read_counts_ch = COUNT_READS(samplesheet_ch, single_end)
    emit:
        read_counts = read_counts_ch.output  // tuple(sample, file) per sample
}
