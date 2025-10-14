/*
Take in reads in either single-end or interleaved format, then handle
them based on endedness:

- Single-end reads are just passed out again as-is
- Interleaved reads are merged with BBMERGE, then unmerged reads are
    concatenated (with an intervening "N"), to produce a single output
    sequence per input read pair.

The convoluted way of handling the single-end vs interleaved cases
is necessitated by the fact that `single_end` is a channel, not a bare boolean.
*/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { BBMERGE_LIST as BBMERGE } from "../../../modules/local/bbmerge"
include { JOIN_FASTQ_LIST as JOIN_FASTQ } from "../../../modules/local/joinFastq"
include { SUMMARIZE_BBMERGE_LIST as SUMMARIZE_BBMERGE } from "../../../modules/local/summarizeBBMerge"

/***********
| WORKFLOW |
***********/

workflow MERGE_JOIN_READS_LIST {
    take:
        reads_ch // Single-end or interleaved FASTQ sequences
        single_end // Boolean channel: true if input reads are single-ended, false if interleaved
    main:
        // Split single-end value channel into two branches, one of which will be empty
        single_end_check = single_end.branch{
            single: it
            paired: !it
        }
        // Forward reads into one of two channels based on endedness (the other will be empty)
        reads_ch_single = single_end_check.single.combine(reads_ch).map{it -> [it[1], it[2]] }
        reads_ch_paired = single_end_check.paired.combine(reads_ch).map{it -> [it[1], it[2]] }
        // In paired-end case, merge and join
        merged_ch = BBMERGE(reads_ch_paired)
        // Sort both merged and unmerged lists to ensure alignment
        sorted_reads_ch = merged_ch.reads.map { sample, merged, unmerged ->
            tuple(sample, merged.sort(), unmerged.sort())
        }
        single_read_ch_paired = JOIN_FASTQ(sorted_reads_ch, false).reads
            .map{ sample, files -> tuple(sample, files instanceof List ? files : [files]) }
        summarize_bbmerge_ch_paired = SUMMARIZE_BBMERGE(sorted_reads_ch).summary
            .map{ sample, files -> tuple(sample, files instanceof List ? files : [files]) }
        // In single-end case, take unmodified reads
        single_read_ch_single = reads_ch_single
        summarize_bbmerge_ch_single = Channel.empty()
        single_read_ch = single_read_ch_paired.mix(single_read_ch_single)
        summarize_bbmerge_ch = summarize_bbmerge_ch_paired.mix(summarize_bbmerge_ch_single)
    emit:
        input_reads = reads_ch
        single_reads = single_read_ch
        bbmerge_summary = summarize_bbmerge_ch
}
