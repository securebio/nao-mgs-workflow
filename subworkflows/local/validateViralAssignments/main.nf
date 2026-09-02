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
include { DOWNSAMPLE_VIRAL_ASSIGNMENTS } from "../../../subworkflows/local/downsampleViralAssignments"
include { CONCATENATE_FILES_BY_EXTENSION } from "../../../modules/local/concatenateFilesByExtension"
include { CONCATENATE_TSVS_LABELED } from "../../../modules/local/concatenateTsvs"
include { BLAST_FASTA } from "../../../subworkflows/local/blastFasta"
include { VALIDATE_SAMPLED_READS } from "../../../subworkflows/local/validateSampledReads"
include { ANNOTATE_VALIDATION_STATUS } from "../../../modules/local/annotateValidationStatus"
include { SORT_TSV } from "../../../modules/local/sortTsv"
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
        // 1. Split viral hits TSV by species. The `annotated` output is every hit labelled
        // with the selected_taxid it was grouped by, which is what step 5 annotates.
        split_ch = SPLIT_VIRAL_TSV_BY_SELECTED_TAXID(groups, db)
        // 2. Downsample each species to a fixed number of reads and render them as FASTA.
        // Validation sits downstream of MARK_VIRAL_DUPLICATES, so sample only duplicate-group
        // Validation sits downstream of MARK_VIRAL_DUPLICATES, so sample only duplicate-group
        // exemplars: a read that duplicates another adds no evidence about its species.
        // Only exemplars selected by both alignment- and similarity-based marking are eligible.
        // ONT skips duplicate marking, so no restriction is applied there and every read stays
        // eligible.
        def exemplar_columns = params_map.platform == "ont" ? "" : "seq_id,sim_dup_exemplar"
        sample_ch = DOWNSAMPLE_VIRAL_ASSIGNMENTS(split_ch.tsv, params_map.validation_n_sample,
            channel.of(params_map.platform == "ont"), exemplar_columns)
        // 3. Concatenate data across species (prepare for group-level BLAST)
        concat_fasta_ch = CONCATENATE_FILES_BY_EXTENSION(sample_ch.fasta, "sampled_reads").output
        concat_sampled_ch = CONCATENATE_TSVS_LABELED(sample_ch.tsv, "sampled_hits").output
        // 4. Run BLAST on the sampled reads (single job per group)
        blast_fasta_params = params_map + [lca_prefix: "validation"]
        blast_ch = BLAST_FASTA(concat_fasta_ch, ref_dir, blast_fasta_params)
        // 5. Compute taxonomic distance for sampled reads that produced alignments
        distance_params = [
            taxid_field_1: "aligner_taxid_lca",
            taxid_field_2: "validation_staxid_lca",
            distance_field_1: "validation_distance_aligner",
            distance_field_2: "validation_distance_validation"
        ]
        validate_ch = VALIDATE_SAMPLED_READS(groups, blast_ch.lca, ref_dir, distance_params)
        // 6. Annotate every hit with its own validation result and status
        // ANNOTATE_VALIDATION_STATUS reads seq_id by name, so it takes the sampled hits
        // table directly rather than a projection of it
        annotate_in_ch = split_ch.annotated
            .combine(validate_ch.output, by: 0)
            .combine(concat_sampled_ch, by: 0)
        annotate_ch = ANNOTATE_VALIDATION_STATUS(annotate_in_ch, "seq_id", "validation_status").output
        // 7. Restore seq_id ordering, which partitioning by selected_taxid disturbed
        sorted_ch = SORT_TSV(annotate_ch, "seq_id").sorted
        output_hits_ch = COPY_HITS(sorted_ch, "validation_hits.tsv.gz")
        output_blast_ch = COPY_BLAST(blast_ch.blast, "validation_blast.tsv.gz")

        // 8. Create empty validation_hits files for groups that produced no output
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
        test_sampled_fasta = sample_ch.fasta
        test_concat_fasta = concat_fasta_ch
        test_blast_db = blast_ch.blast
        test_blast_query = blast_ch.query
        test_blast_lca = blast_ch.lca
        test_validate = validate_ch.output
        test_annotate = annotate_ch
}
