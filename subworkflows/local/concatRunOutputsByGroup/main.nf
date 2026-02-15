/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { CONCAT_BY_GROUP as CONCAT_HITS_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_READ_COUNTS_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_KRAKEN_BY_GROUP } from "../concatByGroup"
include { CONCAT_BY_GROUP as CONCAT_BRACKEN_BY_GROUP } from "../concatByGroup"

/***********
| WORKFLOW |
***********/

workflow CONCAT_RUN_OUTPUTS_BY_GROUP {
    take:
        files  // tuple(label, sample, file, group) from DISCOVER_RUN_OUTPUT
    main:
        hits_ch        = CONCAT_HITS_BY_GROUP(files, "virus_hits.tsv", "grouped_hits").groups
        read_counts_ch = CONCAT_READ_COUNTS_BY_GROUP(files, "read_counts.tsv", "read_counts").groups
        kraken_ch      = CONCAT_KRAKEN_BY_GROUP(files, "kraken.tsv", "kraken").groups
        bracken_ch     = CONCAT_BRACKEN_BY_GROUP(files, "bracken.tsv", "bracken").groups
    emit:
        hits  = hits_ch
        other = read_counts_ch.mix(kraken_ch, bracken_ch)
}
