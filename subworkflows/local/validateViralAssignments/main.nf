/*
Perform efficient post-hoc validation of putative viral reads identified by the RUN workflow.

A. Partition putative hits by assigned species
B. Cluster sequences from each species and identify labeled representative sequences
C. Align representative sequences against a large reference DB
D. Compare taxids assigned to those assigned by RUN workflow
E. Propagate validation information from cluster representatives to other hits
*/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { SPLIT_VIRAL_TSV_BY_SELECTED_TAXID } from "../../../subworkflows/local/splitViralTsvBySelectedTaxid"
include { CLUSTER_VIRAL_ASSIGNMENTS } from "../../../subworkflows/local/clusterViralAssignments"
include { CONCATENATE_FILES_BY_EXTENSION } from "../../../modules/local/concatenateFilesByExtension"
include { CONCATENATE_TSVS_LABELED } from "../../../modules/local/concatenateTsvs"
include { BLAST_FASTA } from "../../../subworkflows/local/blastFasta"
include { VALIDATE_CLUSTER_REPRESENTATIVES } from "../../../subworkflows/local/validateClusterRepresentatives"
include { PROPAGATE_VALIDATION_INFORMATION } from "../../../subworkflows/local/propagateValidationInformation"
include { SELECT_TSV_COLUMNS } from "../../../modules/local/selectTsvColumns"
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
                   // - validation_cluster_identity: Identity threshold for VSEARCH clustering
                   // - cluster_min_len: Minimum sequence length for VSEARCH clustering
                   // - validation_n_clusters: Number of cluster representatives to validate for each specie
                   // - blast_db_prefix: Prefix for BLAST reference DB files (e.g. "nt")
                   // - blast_perc_id: Minimum %ID required for BLAST to return an alignment
                   // - blast_qcov_hsp_perc: Minimum query coverage required for BLAST to return an alignment
                   // - blast_max_rank: Only keep alignments that are in the top-N for that query by bitscore
                   // - blast_min_frac: Only keep alignments that have at least this fraction of the best bitscore for that query
                   // - taxid_artificial: Parent taxid for artificial sequences in NCBI taxonomy
    main:
        // 1. Split viral hits TSV by species
        split_ch = SPLIT_VIRAL_TSV_BY_SELECTED_TAXID(groups, db)
        // 2. Cluster sequences within species and obtain representatives of largest clusters
        cluster_ch = CLUSTER_VIRAL_ASSIGNMENTS(split_ch.fastq, params_map.validation_cluster_identity,
            params_map.cluster_min_len, params_map.validation_n_clusters, Channel.of(params.platform == "ont"))
        // Ensure [[label, [files]]] structure even if there is only one partition
        cluster_ch_fasta = cluster_ch.fasta.map { sample, files ->
            def file_list = files instanceof List ? files : [files]
            [sample, file_list]
        }
        cluster_ch_tsv = cluster_ch.tsv.map { sample, files ->
            def file_list = files instanceof List ? files : [files]
            [sample, file_list]
        }
        // 3. Concatenate data across species (prepare for group-level BLAST)
        concat_fasta_ch = CONCATENATE_FILES_BY_EXTENSION(cluster_ch_fasta, "cluster_reps").output
        concat_cluster_ch = CONCATENATE_TSVS_LABELED(cluster_ch_tsv, "cluster_info")
        // 4. Run BLAST on concatenated cluster representatives (single job per group)
        blast_fasta_params = params_map + [lca_prefix: "validation"]
        blast_ch = BLAST_FASTA(concat_fasta_ch, ref_dir, blast_fasta_params)
        // 5. Validate original group hits against concatenated BLAST results
        distance_params = [
            taxid_field_1: "aligner_taxid_lca",
            taxid_field_2: "validation_staxid_lca",
            distance_field_1: "validation_distance_aligner",
            distance_field_2: "validation_distance_validation"
        ]
        validate_ch = VALIDATE_CLUSTER_REPRESENTATIVES(groups, blast_ch.lca,
            ref_dir, distance_params)
        // 6. Propagate validation information back to individual hits
        propagate_ch = PROPAGATE_VALIDATION_INFORMATION(groups, concat_cluster_ch.output,
            validate_ch.output, "aligner_taxid_lca")
        // 7. Cleanup and generate final outputs
        regrouped_drop_ch = SELECT_TSV_COLUMNS(propagate_ch.output, "taxid_species,selected_taxid", "drop").output
        output_hits_ch = COPY_HITS(regrouped_drop_ch, "validation_hits.tsv.gz")
        output_blast_ch = COPY_BLAST(blast_ch.blast, "validation_blast.tsv.gz")

        // 8. Create empty validation_hits files for groups that produced no output
        input_groups = groups.map { label, _file -> label }.collect().ifEmpty([]).map { ["key", it] }
        output_groups = output_hits_ch.map { label, _file -> label }.collect().ifEmpty([]).map { ["key", it] }
        groups_without_output = input_groups.join(output_groups).map { _key, input_list, output_list ->
            (input_list as Set) - (output_list as Set)
        }
        platform = params_map.platform ?: "illumina"
        CREATE_EMPTY_GROUP_OUTPUTS(
            groups_without_output,
            file("${projectDir}/pyproject.toml"),
            platform,
            "validation_hits"
        )
        all_hits_ch = output_hits_ch.mix(CREATE_EMPTY_GROUP_OUTPUTS.out.outputs.flatten().map {
            def group = it.name.replace("_validation_hits.tsv.gz", "")
            [group, it]
        })
    emit:
        // Main output
        annotated_hits = all_hits_ch
        // Intermediate output
        blast_results = output_blast_ch
        // Extra outputs for testing
        test_in   = groups
        test_split_tsv = split_ch.tsv
        test_cluster_tab = cluster_ch_tsv
        test_reps_fasta = cluster_ch_fasta
        test_concat_fasta = concat_fasta_ch
        test_concat_cluster = concat_cluster_ch.output
        test_blast_db = blast_ch.blast
        test_blast_query = blast_ch.query
        test_blast_lca = blast_ch.lca
        test_validate = validate_ch.output
        test_propagate = propagate_ch.output
}
