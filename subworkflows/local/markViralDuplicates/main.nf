/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { MARK_ALIGNMENT_DUPLICATES } from "../../../modules/local/markAlignmentDuplicates"
include { SORT_TSV as SORT_STATS } from "../../../modules/local/sortTsv"
include { SORT_TSV as SORT_READS } from "../../../modules/local/sortTsv"
include { COPY_FILE as COPY_STATS } from "../../../modules/local/copyFile"
include { MARK_SIMILARITY_DUPLICATES } from "../../../modules/local/markSimilarityDuplicates"

/***********
| WORKFLOW |
***********/

workflow MARK_VIRAL_DUPLICATES {
    take:
        groups // Labeled viral hit TSVs partitioned by group
        deviation // Maximum alignment deviation that qualifies as a duplicate
    main:
        // 1. Mark duplicates by alignment coordinates
        dup_ch = MARK_ALIGNMENT_DUPLICATES(groups, deviation).output
        // 2. Sort output
        reads_ch = dup_ch.map{ id, reads, _stats -> tuple(id, reads) }
        stats_ch = dup_ch.map{ id, _reads, stats -> tuple(id, stats) }
        reads_sorted_ch = SORT_READS(reads_ch, "seq_id").sorted
        stats_sorted_ch = SORT_STATS(stats_ch, "prim_align_genome_id_all").sorted
        // 3. Rename the summary for output. The reads table is no longer published, so it
        // no longer needs renaming.
        stats_out_ch = COPY_STATS(stats_sorted_ch, "duplicate_stats.tsv.gz")
        // 4. Mark similarity duplicates among the reads that survived alignment marking.
        sim_dup_ch = MARK_SIMILARITY_DUPLICATES(reads_sorted_ch).output
    emit:
        // Hits annotated by both duplicate-marking passes
        hits = sim_dup_ch
        // Per-alignment-group summary statistics
        stats = stats_out_ch
        // Extra outputs for testing
        test_in = groups
        test_align_marked = reads_sorted_ch
}
