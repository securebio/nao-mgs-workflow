/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { MINIMAP2 as MINIMAP2_VIRUS } from "../../../modules/local/minimap2"
include { MINIMAP2 as MINIMAP2_HUMAN } from "../../../modules/local/minimap2"
include { MINIMAP2_NON_STREAMED as MINIMAP2_CONTAM } from "../../../modules/local/minimap2"
include { FILTLONG } from "../../../modules/local/filtlong"
include { MASK_FASTQ_READS } from "../../../modules/local/maskRead"
include { PROCESS_VIRAL_MINIMAP2_SAM } from "../../../modules/local/processViralMinimap2Sam"
include { LCA_TSV } from "../../../modules/local/lcaTsv"
include { SORT_TSV as SORT_MINIMAP2_VIRAL } from "../../../modules/local/sortTsv"
include { SORT_TSV as SORT_LCA } from "../../../modules/local/sortTsv"
include { JOIN_TSVS } from "../../../modules/local/joinTsvs"
include { FILTER_TSV_COLUMN_BY_VALUE } from "../../../modules/local/filterTsvColumnByValue"
include { PROCESS_LCA_ALIGNER_OUTPUT } from "../../../subworkflows/local/processLcaAlignerOutput/"
include { COPY_FILE as RENAME_VIRUS_HITS } from "../../../modules/local/copyFile"

/***********
| WORKFLOW |
***********/

workflow EXTRACT_VIRAL_READS_ONT {
    take:
        reads_ch
        ref_dir
        taxid_artificial
        db_download_timeout // Timeout in seconds for database downloads
    main:
        // Get reference_paths
        minimap2_virus_index = "${ref_dir}/results/mm2-virus-index"
        minimap2_human_index = "${ref_dir}/results/mm2-human-index"
        minimap2_contam_index = "${ref_dir}/results/mm2-other-index"
        genome_meta_path = "${ref_dir}/results/virus-genome-metadata-gid.tsv.gz"
        virus_db_path = "${ref_dir}/results/total-virus-db-annotated.tsv.gz"
        nodes_db = "${ref_dir}/results/taxonomy-nodes.dmp"
        names_db = "${ref_dir}/results/taxonomy-names.dmp"
       // Define columns to keep, separating by ones to prefix and ones to not
        col_keep_no_prefix = ["seq_id", "sample", "aligner_taxid_lca", "aligner_taxid_top", 
                              "aligner_length_normalized_score_mean", "aligner_taxid_lca_combined",
                              "aligner_n_assignments_combined", "aligner_length_normalized_score_mean_combined",
                              "aligner_taxid_lca_artificial", "aligner_n_assignments_artificial", 
                              "aligner_length_normalized_score_mean_artificial", "query_len", "query_seq",  
                               "query_qual"]
        col_keep_add_prefix = ["genome_id_all", "taxid_all", "best_alignment_score", "edit_distance",  
                               "ref_start", "query_rc"]
        // Filter reads by length and quality scores
        filtered_ch = FILTLONG(reads_ch, 50, 15000, 90)
        // Mask non-complex read sections
        masked_ch = MASK_FASTQ_READS(filtered_ch, 25, 0.55)
        // Drop human reads before pathogen identification
        minimap2_base_params = [remove_sq: false, db_download_timeout: db_download_timeout]
        human_minimap2_params = minimap2_base_params + [suffix: "human", alignment_params: ""]
        human_minimap2_ch = MINIMAP2_HUMAN(masked_ch.masked, minimap2_human_index, human_minimap2_params)
        no_human_ch = human_minimap2_ch.reads_unmapped
        // Identify other contaminants
        contam_minimap2_params = minimap2_base_params + [suffix: "other", alignment_params: ""]
        contam_minimap2_ch = MINIMAP2_CONTAM(no_human_ch, minimap2_contam_index, contam_minimap2_params)
        no_contam_ch = contam_minimap2_ch.reads_unmapped
        // Identify virus reads with multiple alignments for LCA analysis
        virus_minimap2_params = minimap2_base_params + [suffix: "virus", alignment_params: "-N 10"]
        virus_minimap2_ch = MINIMAP2_VIRUS(no_contam_ch, minimap2_virus_index, virus_minimap2_params)
        virus_sam_ch = virus_minimap2_ch.sam
        // Group cleaned reads and sam files by sample
        sam_fastq_ch = virus_sam_ch.join(filtered_ch)
        // Generate TSV of viral hits, and sort
        processed_minimap2_ch = PROCESS_VIRAL_MINIMAP2_SAM(sam_fastq_ch, genome_meta_path, virus_db_path)
        processed_minimap2_sorted_ch = SORT_MINIMAP2_VIRAL(processed_minimap2_ch.output, "seq_id")
        // Run LCA on viral hits TSV
        lca_params = [
            group_field: "seq_id",
            taxid_field: "taxid",
            score_field: "length_normalized_score",
            taxid_artificial: taxid_artificial,
            prefix: "aligner"
        ]
        lca_ch = LCA_TSV(processed_minimap2_sorted_ch.sorted, nodes_db, names_db, lca_params)
        // Process LCA and Minimap2 columns
        processed_ch = PROCESS_LCA_ALIGNER_OUTPUT(
            lca_ch.output,
            processed_minimap2_sorted_ch.sorted,
            col_keep_no_prefix,
            col_keep_add_prefix,
            "prim_align_"
        )
        // Rename virus hits to clean file name
        renamed_hits_ch = RENAME_VIRUS_HITS(processed_ch.viral_hits_tsv, "virus_hits.tsv.gz")
    emit:
        hits_final = renamed_hits_ch
        inter_lca = processed_ch.lca_tsv
        inter_minimap2 = processed_ch.aligner_tsv
        test_minimap2_virus = virus_sam_ch
        test_fastq_filtered_human = human_minimap2_ch.reads_unmapped
        test_fastq_filtered_contam = contam_minimap2_ch.reads_unmapped
}
