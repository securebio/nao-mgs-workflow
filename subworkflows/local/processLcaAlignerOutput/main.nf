// Process LCA and aligner columns by sorting, joining, filtering, selecting, and renaming columns
/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { SORT_TSV as SORT_LCA } from "../../../modules/local/sortTsv"
include { SORT_TSV as SORT_ALIGNER } from "../../../modules/local/sortTsv"
include { JOIN_TSVS } from "../../../modules/local/joinTsvs"
include { FILTER_TSV_COLUMN_BY_VALUE } from "../../../modules/local/filterTsvColumnByValue"
include { SELECT_TSV_COLUMNS } from "../../../modules/local/selectTsvColumns"
include { REHEAD_TSV } from "../../../modules/local/reheadTsv"
include { ADD_SAMPLE_COLUMN as ADD_SAMPLE_COLUMN_ALIGNER } from "../../../modules/local/addSampleColumn"
include { ADD_SAMPLE_COLUMN as ADD_SAMPLE_COLUMN_LCA } from "../../../modules/local/addSampleColumn"

/***********
| WORKFLOW |
***********/

workflow PROCESS_LCA_ALIGNER_OUTPUT {
    take:
        lca_tsv        // Output from LCA_TSV process
        aligner_tsv    // Output from PROCESS_VIRAL_{MINIMAP2,BOWTIE2}_SAM
        col_keep_no_prefix // Columns to keep without prefix
        col_keep_add_prefix // Columns to keep with prefix
        column_prefix  // Prefix to add to specified columns
    main:
        // Step 1: Sort LCA tsv by seq_id (aligner_tsv is already sorted)
        lca_sorted_ch = SORT_LCA(lca_tsv, "seq_id")
        // Step 2: Add sample column to aligner TSV
        aligner_labeled_tsv = ADD_SAMPLE_COLUMN_ALIGNER(aligner_tsv, "sample", "aligner")
        // Step 3: Join LCA and aligner TSV on seq_id
        joined_input_ch = lca_sorted_ch.sorted.join(aligner_labeled_tsv.output, by: 0)
        joined_ch = JOIN_TSVS(joined_input_ch, "seq_id", "inner", "lca_aligner")
        // Step 4: Filter to keep only primary alignments
        filtered_ch = FILTER_TSV_COLUMN_BY_VALUE(joined_ch.output, "classification", "primary", true)
        // Step 5: Select specific columns
        col_keep = (col_keep_no_prefix + col_keep_add_prefix).join(",")
        selected_ch = SELECT_TSV_COLUMNS(filtered_ch.output, col_keep, "keep")
        // Step 6: Rename columns with prefix using REHEAD_TSV
        old_cols = col_keep_add_prefix.join(",")
        new_cols = col_keep_add_prefix.collect { "${column_prefix}${it}" }.join(",")
        renamed_ch = REHEAD_TSV(selected_ch.output, old_cols, new_cols)
        // Step 7: Add sample column to LCA TSV for intermediate output
        lca_labeled_ch = ADD_SAMPLE_COLUMN_LCA(lca_tsv, "sample", "viral_lca")
    emit:
        viral_hits_tsv = renamed_ch.output              // tuple(sample, file) per sample
        aligner_tsv = aligner_labeled_tsv.output        // tuple(sample, file) per sample
        lca_tsv = lca_labeled_ch.output                 // tuple(sample, file) per sample
}
