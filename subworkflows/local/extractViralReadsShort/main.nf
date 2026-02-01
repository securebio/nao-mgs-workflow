// Short-read version of EXTRACT_VIRAL_READS that uses streaming and interleaved files to minimize memory requirements and loading times
/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { BBDUK_HITS_INTERLEAVE as BBDUK_HITS } from "../../../modules/local/bbduk"
include { FASTP } from "../../../modules/local/fastp"
include { BOWTIE2 as BOWTIE2_VIRUS } from "../../../modules/local/bowtie2"
include { BOWTIE2 as BOWTIE2_HUMAN } from "../../../modules/local/bowtie2"
include { BOWTIE2 as BOWTIE2_OTHER } from "../../../modules/local/bowtie2"
include { PROCESS_VIRAL_BOWTIE2_SAM } from "../../../modules/local/processViralBowtie2Sam"
include { SORT_TSV as SORT_BOWTIE_VIRAL } from "../../../modules/local/sortTsv"
include { CONCATENATE_FILES } from "../../../modules/local/concatenateFiles"
include { CONCATENATE_TSVS as CONCATENATE_VIRAL_HITS_FOR_FASTQ } from "../../../modules/local/concatenateTsvs"
include { EXTRACT_VIRAL_HITS_TO_FASTQ } from "../../../modules/local/extractViralHitsToFastq"
include { LCA_TSV } from "../../../modules/local/lcaTsv"
include { SORT_FASTQ } from "../../../modules/local/sortFastq"
include { SORT_FILE } from "../../../modules/local/sortFile"
include { FILTER_VIRAL_SAM } from "../../../modules/local/filterViralSam"
include { PROCESS_LCA_ALIGNER_OUTPUT } from "../../../subworkflows/local/processLcaAlignerOutput/"
include { COPY_FILE as RENAME_VIRUS_HITS } from "../../../modules/local/copyFile"

/***********
| WORKFLOW |
***********/

workflow EXTRACT_VIRAL_READS_SHORT {
    take:
        reads_ch
        ref_dir
        params_map // aln_score_threshold, adapters, min_kmer_hits, k, bbduk_suffix, taxid_artificial
    main:
        // Get reference paths
        viral_genome_path = "${ref_dir}/results/virus-genomes-masked.fasta.gz"
        genome_meta_path = "${ref_dir}/results/virus-genome-metadata-gid.tsv.gz"
        bt2_virus_index_path = "${ref_dir}/results/bt2-virus-index"
        bt2_human_index_path = "${ref_dir}/results/bt2-human-index"
        bt2_other_index_path = "${ref_dir}/results/bt2-other-index"
        virus_db_path = "${ref_dir}/results/total-virus-db-annotated.tsv.gz"
        nodes_db = "${ref_dir}/results/taxonomy-nodes.dmp"
        names_db = "${ref_dir}/results/taxonomy-names.dmp"
       // Define columns to keep, separating by ones to prefix and ones to not
        col_keep_no_prefix = ["seq_id", "sample", "aligner_taxid_lca", "aligner_taxid_top", 
                              "aligner_length_normalized_score_mean", "aligner_taxid_lca_combined",
                              "aligner_n_assignments_combined", "aligner_length_normalized_score_mean_combined",
                              "aligner_taxid_lca_artificial", "aligner_n_assignments_artificial", 
                              "aligner_length_normalized_score_mean_artificial", "query_len", "query_len_rev",
                              "query_seq", "query_seq_rev", "query_qual", "query_qual_rev"]
        col_keep_add_prefix = ["genome_id_all", "taxid_all", "fragment_length", 
                               "best_alignment_score", "best_alignment_score_rev",
                               "edit_distance", "edit_distance_rev", "ref_start", 
                               "ref_start_rev", "query_rc", "query_rc_rev", "pair_status"]
         // 1. Run initial screen against viral genomes with BBDuk
        bbduk_params = params_map
        bbduk_ch = BBDUK_HITS(reads_ch, viral_genome_path, bbduk_params)
        // 2. Carry out adapter removal with FASTP
        fastp_ch = FASTP(bbduk_ch.fail, params_map.adapters, true)
        // 3. Run Bowtie2 against a viral database and process output
        def bowtie_base_params = [
            remove_sq: true,
            debug: false,
            interleaved: true,
            db_download_timeout: params_map.db_download_timeout
        ]
        par_virus = "--local --very-sensitive-local --score-min G,0.1,19 -k 10"
        bowtie2_virus_params = bowtie_base_params + [par_string: par_virus, suffix: "virus"]
        bowtie2_ch = BOWTIE2_VIRUS(fastp_ch.reads, bt2_virus_index_path, bowtie2_virus_params)

        // 4. Filter contaminants
        par_contaminants = "--local --very-sensitive-local"
        bowtie2_human_params = bowtie_base_params + [par_string: par_contaminants, suffix: "human"]
        human_bt2_ch = BOWTIE2_HUMAN(bowtie2_ch.reads_mapped, bt2_human_index_path, bowtie2_human_params)
        bowtie2_other_params = bowtie_base_params + [par_string: par_contaminants, suffix: "other"]
        other_bt2_ch = BOWTIE2_OTHER(human_bt2_ch.reads_unmapped, bt2_other_index_path, bowtie2_other_params)

        // 5. Sort SAM and FASTQ files before filtering
        bowtie2_sam_sorted_ch = SORT_FILE(bowtie2_ch.sam, "-t\$\'\\t\' -k1,1", "sam")
        other_fastq_sorted_ch = SORT_FASTQ(other_bt2_ch.reads_unmapped)
        // 6. Consolidated viral SAM filtering: keep contaminant-free reads, applies score threshold, adds missing mates
        bowtie2_ch_combined = bowtie2_sam_sorted_ch.output.combine(other_fastq_sorted_ch.output, by: 0)
        bowtie2_filtered_ch = FILTER_VIRAL_SAM(bowtie2_ch_combined, params_map.aln_score_threshold)
        // 7. Convert SAM to TSV
        bowtie2_tsv_ch = PROCESS_VIRAL_BOWTIE2_SAM(bowtie2_filtered_ch.sam, genome_meta_path, virus_db_path, true)
        // 8. Run LCA
        lca_params = [
            group_field: "seq_id",
            taxid_field: "taxid",
            score_field: "length_normalized_score",
            taxid_artificial: params_map.taxid_artificial,
            prefix: "aligner"
        ]
        lca_ch = LCA_TSV(bowtie2_tsv_ch.output, nodes_db, names_db, lca_params)
        // 9. Process LCA and Bowtie2 columns (emits per-sample outputs)
        processed_ch = PROCESS_LCA_ALIGNER_OUTPUT(
            lca_ch.output,
            bowtie2_tsv_ch.output,
            col_keep_no_prefix,
            col_keep_add_prefix,
            "prim_align_"
        )
        // 10. Rename virus hits to clean file name
        renamed_hits_ch = RENAME_VIRUS_HITS(processed_ch.viral_hits_tsv, "virus_hits.tsv.gz")
        // 11. Extract filtered virus hits in FASTQ format
        // This requires concatenated TSV and FASTQ, so we concatenate the per-sample viral_hits for this step
        viral_hits_for_fastq = processed_ch.viral_hits_tsv.map { _sample, file -> file }.collect().ifEmpty([])
        viral_hits_concat = CONCATENATE_VIRAL_HITS_FOR_FASTQ(viral_hits_for_fastq, "virus_hits_for_fastq")
        fastq_unfiltered_collect = other_bt2_ch.reads_unmapped.map{ _sample, file -> file }.collect().ifEmpty([])
        fastq_unfiltered_concat = CONCATENATE_FILES(fastq_unfiltered_collect, "reads_unfiltered", "fastq.gz")
        fastq_ch = EXTRACT_VIRAL_HITS_TO_FASTQ(viral_hits_concat.output, fastq_unfiltered_concat.output)
    emit:
        bbduk_match = bbduk_ch.fail
        bbduk_trimmed = fastp_ch.reads
        hits_final = renamed_hits_ch                    // tuple(sample, file) per sample
        inter_lca = processed_ch.lca_tsv                // tuple(sample, file) per sample
        inter_bowtie = processed_ch.aligner_tsv         // tuple(sample, file) per sample
        hits_prelca = bowtie2_tsv_ch.output
        hits_fastq = fastq_ch.fastq
        test_reads = other_bt2_ch.reads_unmapped
        test_filt_bowtie = bowtie2_filtered_ch.sam
        test_unfilt_bowtie = bowtie2_ch.sam
}
