/*
Downsample per-species partitions of viral hits to a fixed number of reads each, then
render the retained reads as FASTA ready for alignment against a large reference DB.

This replaces the earlier cluster-and-take-an-exemplar approach. Sampling is a uniform
random draw over reads (implemented as a bottom-N hash sketch, so it is reproducible and
independent of row order), which means every retained read is validated on its own
evidence and no verdict has to be extrapolated to its neighbours. Keeping the per-species
split means rare species are still validated thoroughly while abundant ones are capped.

Because sampling happens before FASTQ extraction and pair merging, those steps only ever
see the retained reads rather than the whole viral read pool.

Note that only the downsampling step runs one task per partition. The steps after it are
still list-based processes that loop internally over the grouped files, so they do not
parallelise across partitions. Converting them would be a separate change: they predate
this work and MERGE_JOIN_READS_LIST is shared with other code paths.
*/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { DOWNSAMPLE_TSV_BY_HASH } from "../../../modules/local/downsampleTsvByHash"
include { EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED_LIST as EXTRACT_FASTQ } from "../../../modules/local/extractViralHitsToFastqNoref"
include { MERGE_JOIN_READS_LIST as MERGE_JOIN_READS } from "../../../subworkflows/local/mergeJoinReadsList"
include { CONVERT_FASTQ_FASTA } from "../../../modules/local/convertFastqFasta"

/***********
| WORKFLOW |
***********/

workflow DOWNSAMPLE_VIRAL_ASSIGNMENTS {
    take:
        tsv_ch // Viral hit TSVs partitioned by selected taxid, as [label, [files]]
        n_sample // Maximum reads to validate per selected taxid
        single_end // Is the input read data single-ended (true) or interleaved (false)?
    main:
        // Helper to wrap single-Path values in a list, so all emit channels have a
        // uniform [label, [files]] shape
        def listFiles = { label, files ->
            def file_list = files instanceof List ? files : [files]
            return [label, file_list]
        }
        // 1. Downsample each species partition to at most n_sample reads.
        // DOWNSAMPLE_TSV_BY_HASH takes one file, so flatten to one item per partition and
        // let Nextflow run them concurrently, then regroup for the list-based steps below.
        // The cap applies per partition, which is what keeps rare species fully validated.
        partition_ch = tsv_ch.transpose()
        downsampled_ch = DOWNSAMPLE_TSV_BY_HASH(partition_ch, "seq_id", n_sample).output
        // groupTuple emits in task-completion order, which is a race between the
        // per-partition tasks, so sort by name. This is not a correctness requirement --
        // the consumers below are order-insensitive, and MERGE_JOIN_READS_LIST sorts its
        // own inputs before pairing them -- but the grouped list reaches those processes
        // as a command-line argument, so leaving it unordered would change their task
        // hashes between otherwise identical runs and defeat -resume caching.
        sampled_ch = downsampled_ch.groupTuple()
            .map { label, files -> [label, files.sort { f -> f.name }] }
        // 2. Extract the retained reads into interleaved FASTQ
        fastq_ch = EXTRACT_FASTQ(sampled_ch, false).output.map(listFiles)
        // 3. Merge and join pairs to produce a single sequence per retained read pair
        merge_ch = MERGE_JOIN_READS(fastq_ch, single_end)
        // 4. Convert to FASTA for alignment
        fasta_ch = CONVERT_FASTQ_FASTA(merge_ch.single_reads).output.map(listFiles)
    emit:
        tsv = sampled_ch
        fastq = fastq_ch
        fasta = fasta_ch
        test_merged = merge_ch.single_reads.map(listFiles)
        test_bbmerge_summary = merge_ch.bbmerge_summary
}
