/*
Perform efficient post-hoc validation of putative viral reads identified by the RUN workflow.

A. Partition putative hits by assigned species
B. Downsample each species to a fixed number of reads
C. Align the retained reads against a large reference DB
D. Compare taxids assigned to those assigned by RUN workflow
E. Annotate every hit with its own validation status

Validation results are never extrapolated from one read to another: a read either carries
the result of its own alignment, or carries NA and a status column saying why.
*/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { SPLIT_VIRAL_TSV_BY_SELECTED_TAXID } from "../../../subworkflows/local/splitViralTsvBySelectedTaxid"
include { SAMPLE_VIRAL_ASSIGNMENTS } from "../../../subworkflows/local/sampleViralAssignments"
include { CONCATENATE_FILES_BY_EXTENSION } from "../../../modules/local/concatenateFilesByExtension"
include { CONCATENATE_TSVS_LABELED as CONCAT_PARTITIONED_HITS } from "../../../modules/local/concatenateTsvs"
include { CONCATENATE_TSVS_LABELED as CONCAT_SAMPLED_HITS } from "../../../modules/local/concatenateTsvs"
include { BLAST_FASTA } from "../../../subworkflows/local/blastFasta"
include { VALIDATE_SAMPLED_READS } from "../../../subworkflows/local/validateSampledReads"
include { ANNOTATE_VALIDATION_STATUS } from "../../../modules/local/annotateValidationStatus"
include { SELECT_TSV_COLUMNS as SELECT_SAMPLED_IDS } from "../../../modules/local/selectTsvColumns"
include { SELECT_TSV_COLUMNS as DROP_ALIGNER_TAXID } from "../../../modules/local/selectTsvColumns"
include { SELECT_TSV_COLUMNS as DROP_SPECIES_TAXID } from "../../../modules/local/selectTsvColumns"
include { SORT_TSV as SORT_ANNOTATED_HITS } from "../../../modules/local/sortTsv"
include { COPY_FILE as COPY_HITS } from "../../../modules/local/copyFile"
include { COPY_FILE as COPY_BLAST } from "../../../modules/local/copyFile"
include { CREATE_EMPTY_GROUP_OUTPUTS } from "../../../modules/local/createEmptyGroupOutputs"

/***********
| WORKFLOW |
***********/

workflow VALIDATE_VIRAL_ASSIGNMENTS {
    take:
        groups // Labeled viral hit TSVs partitioned by group
        db // Viral taxonomy DB
        ref_dir // Path to reference directory containing BLAST DB
        params_map // Map containing parameters:
                   // - validation_n_sample: Number of reads to validate for each selected taxid
                   // - blast_perc_id: Minimum %ID required for BLAST to return an alignment
                   // - blast_qcov_hsp_perc: Minimum query coverage required for BLAST to return an alignment
                   // - blast_max_rank: Only keep alignments that are in the top-N for that query by bitscore
                   // - blast_min_frac: Only keep alignments that have at least this fraction of the best bitscore for that query
                   // - taxid_artificial: Parent taxid for artificial sequences in NCBI taxonomy
    main:
        // 1. Split viral hits TSV by species
        split_ch = SPLIT_VIRAL_TSV_BY_SELECTED_TAXID(groups, db)
        // 2. Reassemble the partitions into one table per group. This is the hits table
        // annotated with selected_taxid, so every read (sampled or not) carries the
        // species group it was validated within.
        concat_hits_ch = CONCAT_PARTITIONED_HITS(split_ch.tsv, "partitioned_hits").output
        // 3. Downsample each species to a fixed number of reads and render them as FASTA
        sample_ch = SAMPLE_VIRAL_ASSIGNMENTS(split_ch.tsv, params_map.validation_n_sample,
            channel.of(params_map.platform == "ont"))
        // 4. Concatenate data across species (prepare for group-level BLAST)
        concat_fasta_ch = CONCATENATE_FILES_BY_EXTENSION(sample_ch.fasta, "sampled_reads").output
        concat_sampled_ch = CONCAT_SAMPLED_HITS(sample_ch.tsv, "sampled_hits").output
        sampled_ids_ch = SELECT_SAMPLED_IDS(concat_sampled_ch, "seq_id", "keep").output
        // 5. Run BLAST on the sampled reads (single job per group)
        blast_fasta_params = params_map + [lca_prefix: "validation"]
        blast_ch = BLAST_FASTA(concat_fasta_ch, ref_dir, blast_fasta_params)
        // 6. Compute taxonomic distance for sampled reads that produced alignments
        distance_params = [
            taxid_field_1: "aligner_taxid_lca",
            taxid_field_2: "validation_staxid_lca",
            distance_field_1: "validation_distance_aligner",
            distance_field_2: "validation_distance_validation"
        ]
        validate_ch = VALIDATE_SAMPLED_READS(groups, blast_ch.lca, ref_dir, distance_params)
        // Drop the original taxid, which is already present in the hits table
        validation_ch = DROP_ALIGNER_TAXID(validate_ch.output, distance_params.taxid_field_1, "drop").output
        // 7. Annotate every hit with its own validation result and status
        annotate_in_ch = concat_hits_ch
            .combine(validation_ch, by: 0)
            .combine(sampled_ids_ch, by: 0)
        annotate_ch = ANNOTATE_VALIDATION_STATUS(annotate_in_ch, "seq_id", "validation_status").output
        // 8. Restore seq_id ordering (partitioning reordered the rows) and clean up
        sorted_ch = SORT_ANNOTATED_HITS(annotate_ch, "seq_id").sorted
        regrouped_drop_ch = DROP_SPECIES_TAXID(sorted_ch, "taxid_species", "drop").output
        output_hits_ch = COPY_HITS(regrouped_drop_ch, "validation_hits.tsv.gz")
        output_blast_ch = COPY_BLAST(blast_ch.blast, "validation_blast.tsv.gz")

        // 9. Create empty validation_hits files for groups that produced no output
        input_groups = groups.map { label, _file -> label }.collect().ifEmpty([]).map { labels -> ["key", labels] }
        output_groups = output_hits_ch.map { label, _file -> label }.collect().ifEmpty([]).map { labels -> ["key", labels] }
        groups_without_output = input_groups.join(output_groups).map { _key, input_list, output_list ->
            (input_list as Set) - (output_list as Set)
        }
        platform = params_map.platform ?: "illumina"
        empty_outputs_ch = CREATE_EMPTY_GROUP_OUTPUTS(
            groups_without_output,
            file("${projectDir}/pyproject.toml"),
            file("${projectDir}/schemas"),
            platform,
            "validation_hits"
        )
        all_hits_ch = output_hits_ch.mix(empty_outputs_ch.outputs.flatten().map { f ->
            def group = f.name.replace("_validation_hits.tsv.gz", "")
            [group, f]
        })
    emit:
        // Main output
        annotated_hits = all_hits_ch
        // Intermediate output
        blast_results = output_blast_ch
        // Extra outputs for testing
        test_in   = groups
        test_split_tsv = split_ch.tsv
        test_concat_hits = concat_hits_ch
        test_sampled_tsv = sample_ch.tsv
        test_sampled_fasta = sample_ch.fasta
        test_concat_fasta = concat_fasta_ch
        test_concat_sampled = concat_sampled_ch
        test_sampled_ids = sampled_ids_ch
        test_blast_db = blast_ch.blast
        test_blast_query = blast_ch.query
        test_blast_lca = blast_ch.lca
        test_validate = validate_ch.output
        test_annotate = annotate_ch
}
