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
*/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { SAMPLE_TSV_BY_HASH_LIST } from "../../../modules/local/sampleTsvByHash"
include { EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED_LIST as EXTRACT_FASTQ } from "../../../modules/local/extractViralHitsToFastqNoref"
include { MERGE_JOIN_READS_LIST as MERGE_JOIN_READS } from "../../../subworkflows/local/mergeJoinReadsList"
include { CONVERT_FASTQ_FASTA } from "../../../modules/local/convertFastqFasta"

/***********
| WORKFLOW |
***********/

workflow SAMPLE_VIRAL_ASSIGNMENTS {
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
        // 1. Downsample each species partition to at most n_sample reads
        sampled_ch = SAMPLE_TSV_BY_HASH_LIST(tsv_ch, "seq_id", n_sample).output.map(listFiles)
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
