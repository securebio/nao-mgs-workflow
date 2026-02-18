/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { CONCAT_BY_GROUP as CONCAT_HITS_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_READ_COUNTS_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_KRAKEN_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_BRACKEN_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_ADAPTER_STATS_CLEANED_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_ADAPTER_STATS_RAW_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_BASIC_STATS_CLEANED_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_BASIC_STATS_RAW_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_LENGTH_STATS_CLEANED_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_LENGTH_STATS_RAW_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_QUALITY_BASE_STATS_CLEANED_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_QUALITY_BASE_STATS_RAW_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_QUALITY_SEQUENCE_STATS_CLEANED_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_QC_QUALITY_SEQUENCE_STATS_RAW_BY_GROUP } from "../concatByGroup"

/***********
| WORKFLOW |
***********/

workflow CONCAT_RUN_OUTPUTS_BY_GROUP {
    take:
        files  // tuple(label, sample, file, group) from DISCOVER_RUN_OUTPUT
    main:
        hits_ch                              = CONCAT_HITS_BY_GROUP(files, "virus_hits.tsv", "grouped_hits").groups
        read_counts_ch                       = CONCAT_READ_COUNTS_BY_GROUP(files, "read_counts.tsv", "read_counts").groups
        kraken_ch                            = CONCAT_KRAKEN_BY_GROUP(files, "kraken.tsv", "kraken").groups
        bracken_ch                           = CONCAT_BRACKEN_BY_GROUP(files, "bracken.tsv", "bracken").groups
        qc_adapter_stats_cleaned_ch          = CONCAT_QC_ADAPTER_STATS_CLEANED_BY_GROUP(files, "qc_adapter_stats_cleaned.tsv", "qc_adapter_stats_cleaned").groups
        qc_adapter_stats_raw_ch              = CONCAT_QC_ADAPTER_STATS_RAW_BY_GROUP(files, "qc_adapter_stats_raw.tsv", "qc_adapter_stats_raw").groups
        qc_basic_stats_cleaned_ch            = CONCAT_QC_BASIC_STATS_CLEANED_BY_GROUP(files, "qc_basic_stats_cleaned.tsv", "qc_basic_stats_cleaned").groups
        qc_basic_stats_raw_ch                = CONCAT_QC_BASIC_STATS_RAW_BY_GROUP(files, "qc_basic_stats_raw.tsv", "qc_basic_stats_raw").groups
        qc_length_stats_cleaned_ch           = CONCAT_QC_LENGTH_STATS_CLEANED_BY_GROUP(files, "qc_length_stats_cleaned.tsv", "qc_length_stats_cleaned").groups
        qc_length_stats_raw_ch               = CONCAT_QC_LENGTH_STATS_RAW_BY_GROUP(files, "qc_length_stats_raw.tsv", "qc_length_stats_raw").groups
        qc_quality_base_stats_cleaned_ch     = CONCAT_QC_QUALITY_BASE_STATS_CLEANED_BY_GROUP(files, "qc_quality_base_stats_cleaned.tsv", "qc_quality_base_stats_cleaned").groups
        qc_quality_base_stats_raw_ch         = CONCAT_QC_QUALITY_BASE_STATS_RAW_BY_GROUP(files, "qc_quality_base_stats_raw.tsv", "qc_quality_base_stats_raw").groups
        qc_quality_sequence_stats_cleaned_ch = CONCAT_QC_QUALITY_SEQUENCE_STATS_CLEANED_BY_GROUP(files, "qc_quality_sequence_stats_cleaned.tsv", "qc_quality_sequence_stats_cleaned").groups
        qc_quality_sequence_stats_raw_ch     = CONCAT_QC_QUALITY_SEQUENCE_STATS_RAW_BY_GROUP(files, "qc_quality_sequence_stats_raw.tsv", "qc_quality_sequence_stats_raw").groups
    emit:
        hits  = hits_ch
        other = read_counts_ch.mix(
            kraken_ch, bracken_ch,
            qc_adapter_stats_cleaned_ch, qc_adapter_stats_raw_ch,
            qc_basic_stats_cleaned_ch, qc_basic_stats_raw_ch,
            qc_length_stats_cleaned_ch, qc_length_stats_raw_ch,
            qc_quality_base_stats_cleaned_ch, qc_quality_base_stats_raw_ch,
            qc_quality_sequence_stats_cleaned_ch, qc_quality_sequence_stats_raw_ch,
        )
}
