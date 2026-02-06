/***********************************************************
| SUBWORKFLOW: TAXONOMIC PROFILING WITH KRAKEN AND BRACKEN |
***********************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { MERGE_JOIN_READS } from "../../../subworkflows/local/mergeJoinReads"
include { KRAKEN } from "../../../modules/local/kraken"
include { HEAD_TSV as HEAD_KRAKEN_REPORTS } from "../../../modules/local/headTsv"
include { ADD_SAMPLE_COLUMN as LABEL_KRAKEN_REPORTS } from "../../../modules/local/addSampleColumn"
include { BRACKEN } from "../../../modules/local/bracken"
include { REHEAD_TSV as REHEAD_BRACKEN } from "../../../modules/local/reheadTsv"
include { ADD_SAMPLE_COLUMN as LABEL_BRACKEN } from "../../../modules/local/addSampleColumn"

/***********
| WORKFLOW |
***********/

workflow TAXONOMY {
    take:
        reads_ch // Should be interleaved for paired-end data
        kraken_db_ch
        single_end
        params_map // classification_level, bracken_threshold, db_download_timeout
    main:
        // Merge and join interleaved sequences to produce a single sequence per input pair
        merge_ch = MERGE_JOIN_READS(reads_ch, single_end)
        single_read_ch = merge_ch.single_reads
        summarize_bbmerge_ch = merge_ch.bbmerge_summary
        // Run Kraken and munge reports
        kraken_ch = KRAKEN(single_read_ch, kraken_db_ch, params_map.db_download_timeout)
        kraken_headers = "pc_reads_total,n_reads_clade,n_reads_direct,n_minimizers_total,n_minimizers_distinct,rank,taxid,name"
        kraken_head_ch = HEAD_KRAKEN_REPORTS(kraken_ch.report, kraken_headers, "kraken_report")
        kraken_label_ch = LABEL_KRAKEN_REPORTS(kraken_head_ch.output, "sample", "kraken_report")
        // Run Bracken and munge reports
        bracken_ch = BRACKEN(kraken_ch.report, kraken_db_ch, params_map.classification_level, params_map.bracken_threshold) // NB: Not streamed
        bracken_rehead_ch = REHEAD_BRACKEN(bracken_ch, "taxonomy_id,taxonomy_lvl,kraken_assigned_reads", "taxid,rank,kraken2_assigned_reads")
        bracken_label_ch = LABEL_BRACKEN(bracken_rehead_ch.output, "sample", "bracken")
    emit:
        input_reads = reads_ch
        single_reads = single_read_ch
        bbmerge_summary = summarize_bbmerge_ch
        kraken_output = kraken_ch.output
        kraken_reports = kraken_label_ch.output
        bracken = bracken_label_ch.output
}
