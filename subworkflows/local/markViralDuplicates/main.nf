/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { MARK_ALIGNMENT_DUPLICATES } from "../../../modules/local/markAlignmentDuplicates"
include { SORT_TSV as SORT_STATS } from "../../../modules/local/sortTsv"
include { SORT_TSV as SORT_READS } from "../../../modules/local/sortTsv"
include { COPY_FILE as COPY_STATS } from "../../../modules/local/copyFile"
include { COPY_FILE as COPY_READS } from "../../../modules/local/copyFile"
include { COPY_FILE as COPY_SIM_DUP } from "../../../modules/local/copyFile"
include { MARK_SIMILARITY_DUPLICATES } from "../../../modules/local/markSimilarityDuplicates"

/***********
| WORKFLOW |
***********/

workflow MARK_VIRAL_DUPLICATES {
    take:
        groups // Labeled viral hit TSVs partitioned by group
        deviation // Maximum alignment deviation that qualifies as a duplicate
    main:
        // 1. Mark duplicates
        dup_ch = MARK_ALIGNMENT_DUPLICATES(groups, deviation).output
        // 2. Sort output
        reads_ch = dup_ch.map{ id, reads, _stats -> tuple(id, reads) }
        stats_ch = dup_ch.map{ id, _reads, stats -> tuple(id, stats) }
        reads_sorted_ch = SORT_READS(reads_ch, "seq_id").sorted
        stats_sorted_ch = SORT_STATS(stats_ch, "prim_align_genome_id_all").sorted
        // 3. Rename and prepare files for output
        reads_out_ch = COPY_READS(reads_sorted_ch, "duplicate_reads.tsv.gz")
        stats_out_ch = COPY_STATS(stats_sorted_ch, "duplicate_stats.tsv.gz")
        out_ch = reads_out_ch.combine(stats_out_ch, by: 0)
        // 4. Run similarity-based duplicate marking on alignment-deduplicated reads
        sim_dup_raw_ch = MARK_SIMILARITY_DUPLICATES(reads_out_ch).output
        sim_dup_ch = COPY_SIM_DUP(sim_dup_raw_ch, "duplicate_reads_similarity.tsv.gz")
    emit:
        dup = out_ch
        sim_dup = sim_dup_ch
        test_in = groups
}
