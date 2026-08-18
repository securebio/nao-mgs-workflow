/*
Downsample per-species partitions of viral hits to a fixed number of reads each, then
render the retained reads as FASTA ready for alignment against a large reference DB.

Sampling is a uniform random draw per species (a bottom-N hash sketch, so reproducible
and order-independent), so every retained read is validated on its own evidence and rare
species stay thoroughly validated while abundant ones are capped. Where the caller
restricts sampling to duplicate-group exemplars, the cap bounds the exemplars.

Only the downsampling step runs one task per partition; the steps after it are list-based
processes that loop internally over the grouped files.
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
        // Helper to wrap single-Path values in a list, so all emit channels have a
        // uniform [label, [files]] shape
        def listFiles = { label, files ->
            def file_list = files instanceof List ? files : [files]
            return [label, file_list]
        }
        // 1. Downsample each species partition to at most n_sample reads. Flatten to one
        // item per partition so the per-file process runs concurrently, then regroup.
        partition_ch = tsv_ch.transpose()
        // exemplar_columns confines sampling to duplicate-group exemplars, since aligning
        // a duplicate read buys no information; empty leaves every read eligible.
        downsampled_ch = DOWNSAMPLE_TSV_BY_HASH(partition_ch, "seq_id", n_sample, exemplar_columns).output
        // groupTuple emits in task-completion order, so sort by name: the grouped list
        // reaches the processes below as a command-line argument, and leaving it
        // unordered would change their task hashes between runs and defeat -resume.
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
