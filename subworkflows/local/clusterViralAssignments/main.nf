/*
Cluster a channel of viral sequences with VSEARCH, process the output,
and extract the representative sequences of the top N largest clusters.
*/

def listFiles = { label, files ->
    def file_list = files instanceof List ? files : [files]
    [label, file_list]
}

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { MERGE_JOIN_READS_LIST as MERGE_JOIN_READS } from "../../../subworkflows/local/mergeJoinReadsList"
include { VSEARCH_CLUSTER_LIST as VSEARCH_CLUSTER } from "../../../modules/local/vsearch"
include { PROCESS_VSEARCH_CLUSTER_OUTPUT_LIST as PROCESS_VSEARCH_CLUSTER_OUTPUT } from "../../../modules/local/processVsearchClusterOutput"
include { DOWNSAMPLE_FASTN_BY_ID_LIST as DOWNSAMPLE_FASTN_BY_ID } from "../../../modules/local/downsampleFastnById"
include { CONVERT_FASTQ_FASTA } from "../../../modules/local/convertFastqFasta"
include { ADD_SAMPLE_COLUMN_LIST as LABEL_GROUP_SPECIES } from "../../../modules/local/addSampleColumn"

/***********
| WORKFLOW |
***********/

workflow CLUSTER_VIRAL_ASSIGNMENTS {
    take:
        reads_ch // Single-end or interleaved FASTQ sequences
        cluster_identity // Identity threshold for VSEARCH clustering
        cluster_min_len // Minimum sequence length for VSEARCH clustering
        n_clusters // Number of cluster representatives to validate for each species
        single_end // Is the input read data single-ended (true) or interleaved (false)?
    main:
        // 1. Merge and join interleaved sequences to produce a single sequence per input pair
        merge_ch = MERGE_JOIN_READS(reads_ch, single_end)
        // 2. Cluster merged reads
        cluster_ch = VSEARCH_CLUSTER(merge_ch.single_reads, cluster_identity, 0, cluster_min_len)
        // 3. Extract clustering information and representative IDs
        cluster_info_ch = PROCESS_VSEARCH_CLUSTER_OUTPUT(cluster_ch.summary, n_clusters, "vsearch")
        // 4. Add group_species column to cluster TSVs
        labeled_cluster_ch = LABEL_GROUP_SPECIES(cluster_info_ch.output, "group_species", "group_species")
        // 5. Extract representative sequences for the N largest clusters for each species
        // Note: Must wrap single Paths in a list before sorting because calling .sort() on a
        // single Path treats it as an iterable of path segments, returning ["segment1", "segment2", ...]
        // instead of preserving the Path object, which causes Nextflow file staging collisions
        id_prep_ch = merge_ch.single_reads.combine(cluster_info_ch.ids, by: 0)
            .map { sample, reads, ids ->
                def reads_list = reads instanceof List ? reads : [reads]
                def ids_list = ids instanceof List ? ids : [ids]
                tuple(sample, reads_list.sort { it.name }, ids_list.sort { it.name })
            }
        rep_fastq_ch = DOWNSAMPLE_FASTN_BY_ID(id_prep_ch).output
        rep_fasta_ch = CONVERT_FASTQ_FASTA(rep_fastq_ch).output
    emit:
        tsv = labeled_cluster_ch.output.map(listFiles)
        ids = cluster_info_ch.ids.map(listFiles)
        fastq = rep_fastq_ch.map(listFiles)
        fasta = rep_fasta_ch.map(listFiles)
        test_merged = merge_ch.single_reads.map(listFiles)
        test_cluster_reps = cluster_ch.reps.map(listFiles)
        test_cluster_summ = cluster_ch.summary.map(listFiles)
}
