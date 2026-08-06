/*
Given a viral hits TSV and the LCA of a large-reference-DB search for the subset of reads
selected for validation, compute the taxonomic distance between each sampled read's
original assignment and its validated assignment.

The join is an inner join, so the result covers exactly those sampled reads that produced
at least one alignment. Reads that were sampled but produced no alignment, and reads that
were never sampled, are handled downstream by ANNOTATE_VALIDATION_STATUS.

The emitted table is keyed on seq_id and carries only the validation columns, with the
original taxid dropped again after the distance calculation has consumed it. That leaves a
table that can be joined straight onto the full hits TSV, which already holds that column.

The input map distance_params should specify the following fields
for computing taxonomic distance between the original and validated taxids:
- taxid_field_1: Column header for original taxid
- taxid_field_2: Column header for validated taxid
- distance_field_1: Column header for original taxid distance
- distance_field_2: Column header for validated taxid distance
*/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { SELECT_TSV_COLUMNS } from "../../../modules/local/selectTsvColumns"
include { SELECT_TSV_COLUMNS as DROP_ALIGNER_TAXID } from "../../../modules/local/selectTsvColumns"
include { JOIN_TSVS } from "../../../modules/local/joinTsvs"
include { REHEAD_TSV } from "../../../modules/local/reheadTsv"
include { COMPUTE_TAXID_DISTANCE } from "../../../modules/local/computeTaxidDistance"

/***********
| WORKFLOW |
***********/

workflow VALIDATE_SAMPLED_READS {
    take:
        hits_tsv // Viral hits TSV, including seq_id and original taxid assignment
        lca_tsv // LCA TSV from validation against the reference DB
        ref_dir // Path to reference directory containing taxonomy information
        distance_params // Map specifying input taxid fields and output distance fields
    main:
        // 0. Get reference paths
        nodes_db = "${ref_dir}/results/taxonomy-nodes.dmp"
        // 1. Prepare inputs for joining
        // Subset hits TSV to only seq_id and taxid columns
        def hits_taxid_column_str = "seq_id,${distance_params.taxid_field_1}"
        select_ch = SELECT_TSV_COLUMNS(hits_tsv, hits_taxid_column_str, "keep").output
        // Rename qseqid to seq_id in LCA TSV
        rehead_ch = REHEAD_TSV(lca_tsv, "qseqid", "seq_id").output
        // Combine channels for joining
        combine_ch = select_ch.combine(rehead_ch, by: 0)
        // 2. Inner-join hits and LCA tables by seq_id
        // Inner join subsets hits TSV to sequences that produced an alignment
        join_ch = JOIN_TSVS(combine_ch, "seq_id", "inner", "sampled").output
        // 3. Compute taxonomic distance between original and validated taxids
        dist_ch = COMPUTE_TAXID_DISTANCE(join_ch, distance_params, nodes_db).output
        // NB: As implemented, this will produce a negative distance if the original
        // taxid is too high (ancestor of LCA taxid), and a positive distance if the
        // original taxid is too low (descendant of LCA taxid).
        // 4. Drop the original taxid, which the hits TSV this joins onto already carries
        output_ch = DROP_ALIGNER_TAXID(dist_ch, distance_params.taxid_field_1, "drop").output
    emit:
        output = output_ch
        test_select = select_ch
        test_rehead = rehead_ch
        test_join = join_ch
        test_dist = dist_ch
}
