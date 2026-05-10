/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { SUBSET_READS_SINGLE_TARGET as SUBSET_SINGLE } from "../../../modules/local/subsetReads"
include { SUBSET_READS_PAIRED_TARGET as SUBSET_PAIRED } from "../../../modules/local/subsetReads"
include { FASTP } from "../../../modules/local/fastp"
include { FILTLONG as FILTLONG_STRINGENT } from "../../../modules/local/filtlong"
include { FILTLONG as FILTLONG_LOOSE } from "../../../modules/local/filtlong"

/***********
| WORKFLOW |
***********/

workflow SUBSET_TRIM {
    take:
        reads_ch
        counts_ch       // tuple(sample, counts_tsv) — output of COUNT_READS
        single_end
        params_map      // n_reads_profile, adapters, platform, random_seed
    main:
        // Split single-end value channel into two branches, one of which will be empty
        single_end_check = single_end.branch{
            single: it
            paired: !it
        }
        // Forward reads + counts into one of two channels based on endedness
        reads_with_counts = reads_ch.join(counts_ch) // [sample, reads, counts_tsv]
        reads_paired = single_end_check.paired.combine(reads_with_counts).map{ [it[1], it[2], it[3]] }
        reads_single = single_end_check.single.combine(reads_with_counts).map{ [it[1], it[2], it[3]] }
        // Subset reads. SUBSET_PAIRED emits interleaved output directly (subsumes
        // the legacy INTERLEAVE_FASTQ step that previously ran here); SUBSET_SINGLE
        // is already a single stream. INTERLEAVE_FASTQ remains in the codebase as a
        // test-setup helper used by other module tests.
        subset_ch_single = SUBSET_SINGLE(reads_single, params_map.n_reads_profile, params_map.random_seed).output
        subset_ch_paired = SUBSET_PAIRED(reads_paired, params_map.n_reads_profile, params_map.random_seed).output
        inter_ch = subset_ch_single.mix(subset_ch_paired)
        // Read cleaning
        if (params_map.platform == "ont") {
            cleaned_ch = FILTLONG_STRINGENT(inter_ch, 100, 15000, 90)
            subset_reads = FILTLONG_LOOSE(inter_ch, 1, 500000, 0.01) // Very loose filtering just to avoid out-of-memory errors
        } else {
            cleaned_ch = FASTP(inter_ch, params_map.adapters, single_end.map{!it})
            subset_reads = inter_ch
        }
    emit:
        subset_reads
        trimmed_subset_reads = cleaned_ch.reads
        fastp_json = params_map.platform == "ont" ? Channel.empty() : cleaned_ch.json
        test_failed = params_map.platform == "ont" ? Channel.empty() : cleaned_ch.failed // TODO: Capture rejected ONT reads somehow
}
