/*
Downsample per-species partitions of viral hits to at most n_sample reads each, then
render the retained reads as FASTA for alignment against a large reference DB. Sampling
is a uniform per-species draw (a bottom-N hash sketch, so reproducible and
order-independent), so each retained read is validated on its own evidence.
*/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { DOWNSAMPLE_TSV_BY_HASH } from "../../../modules/local/downsampleTsvByHash"
include { EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED_LIST } from "../../../modules/local/extractViralHitsToFastqNoref"
include { MERGE_JOIN_READS_LIST } from "../../../subworkflows/local/mergeJoinReadsList"
include { CONVERT_FASTQ_FASTA } from "../../../modules/local/convertFastqFasta"

/***********
| WORKFLOW |
***********/

workflow DOWNSAMPLE_VIRAL_ASSIGNMENTS {
    take:
        tsv_ch // Viral hit TSVs partitioned by selected taxid, as [label, [files]]
        n_sample // Maximum reads to validate per selected taxid
        single_end // Is the input read data single-ended (true) or interleaved (false)?
        exemplar_columns // Optional "colA,colB"; restricts sampling to rows where they agree
    main:
        // Wrap single-Path values in a list, so every emit has a [label, [files]] shape
        def listFiles = { label, files ->
            def file_list = files instanceof List ? files : [files]
            return [label, file_list]
        }
        // 1. Downsample each partition, one task each; exemplar_columns optionally
        // confines sampling to duplicate-group exemplars
        partition_ch = tsv_ch.transpose()
        downsampled_ch = DOWNSAMPLE_TSV_BY_HASH(partition_ch, "seq_id", n_sample, exemplar_columns).output
        // Sort the group: it reaches the processes below as a command-line argument, so
        // task-completion order would change their task hashes and defeat -resume
        sampled_ch = downsampled_ch.groupTuple()
            .map { label, files -> [label, files.sort { f -> f.name }] }
        // 2. Extract the retained reads into interleaved FASTQ
        fastq_ch = EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED_LIST(sampled_ch, false).output.map(listFiles)
        // 3. Merge and join pairs to produce a single sequence per retained read pair
        merge_ch = MERGE_JOIN_READS_LIST(fastq_ch, single_end)
        // 4. Convert to FASTA for alignment
        fasta_ch = CONVERT_FASTQ_FASTA(merge_ch.single_reads).output.map(listFiles)
    emit:
        tsv = sampled_ch
        fastq = fastq_ch
        fasta = fasta_ch
        test_merged = merge_ch.single_reads.map(listFiles)
        test_bbmerge_summary = merge_ch.bbmerge_summary
}
