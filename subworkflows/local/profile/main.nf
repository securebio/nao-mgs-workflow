/**********************************************
| SUBWORKFLOW: HIGH-LEVEL TAXONOMIC PROFILING |
**********************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { BBDUK } from "../../../modules/local/bbduk"
include { MINIMAP2 } from "../../../modules/local/minimap2"
include { TAXONOMY as TAXONOMY_RIBO } from "../../../subworkflows/local/taxonomy"
include { TAXONOMY as TAXONOMY_NORIBO } from "../../../subworkflows/local/taxonomy"
include { ADD_FIXED_COLUMN as ADD_KRAKEN_RIBO } from "../../../modules/local/addFixedColumn"
include { ADD_FIXED_COLUMN as ADD_BRACKEN_RIBO } from "../../../modules/local/addFixedColumn"
include { ADD_FIXED_COLUMN as ADD_KRAKEN_NORIBO } from "../../../modules/local/addFixedColumn"
include { ADD_FIXED_COLUMN as ADD_BRACKEN_NORIBO } from "../../../modules/local/addFixedColumn"
include { CONCATENATE_TSVS_LABELED as CONCATENATE_KRAKEN_PER_SAMPLE } from "../../../modules/local/concatenateTsvs"
include { CONCATENATE_TSVS_LABELED as CONCATENATE_BRACKEN_PER_SAMPLE } from "../../../modules/local/concatenateTsvs"

/****************
| MAIN WORKFLOW |
****************/

workflow PROFILE {
    take:
        reads_ch
        kraken_db_ch
        ref_dir
        single_end
        params_map // min_kmer_fraction, k, ribo_suffix, bracken_threshold, platform, db_download_timeout
    main:
        // Separate ribosomal reads
        if (params_map.platform == "ont") {
            ribo_ref = "${ref_dir}/results/mm2-ribo-index"
            ribo_minimap2_params = params_map + [remove_sq: false, alignment_params: ""]
            ribo_ch = MINIMAP2(reads_ch, ribo_ref, ribo_minimap2_params)
            ribo_in = ribo_ch.reads_mapped
            noribo_in = ribo_ch.reads_unmapped
        } else {
            ribo_path = "${ref_dir}/results/ribo-ref-concat.fasta.gz"
            ribo_bbduk_params = params_map + [interleaved: single_end.map{!it}]
            ribo_ch = BBDUK(reads_ch, ribo_path, ribo_bbduk_params)
            ribo_in = ribo_ch.match
            noribo_in = ribo_ch.nomatch
        }
        // Run taxonomic profiling separately on ribo and non-ribo reads
        taxonomy_params = params_map + [classification_level: "D"]
        tax_ribo_ch = TAXONOMY_RIBO(ribo_in, kraken_db_ch, single_end, taxonomy_params)
        tax_noribo_ch = TAXONOMY_NORIBO(noribo_in, kraken_db_ch, single_end, taxonomy_params)
        // Add ribosomal status to output TSVs
        kr_ribo = ADD_KRAKEN_RIBO(tax_ribo_ch.kraken_reports, "ribosomal", "TRUE", "ribo")
        kr_noribo = ADD_KRAKEN_NORIBO(tax_noribo_ch.kraken_reports, "ribosomal", "FALSE", "noribo")
        br_ribo = ADD_BRACKEN_RIBO(tax_ribo_ch.bracken, "ribosomal", "TRUE", "ribo")
        br_noribo = ADD_BRACKEN_NORIBO(tax_noribo_ch.bracken, "ribosomal", "FALSE", "noribo")
        // Concatenate ribo + noribo for each sample
        kr_combined = kr_ribo.output.join(kr_noribo.output)
            .map { sample, ribo_file, noribo_file -> [sample, [ribo_file, noribo_file]] }
        kr_per_sample = CONCATENATE_KRAKEN_PER_SAMPLE(kr_combined, "kraken")
        br_combined = br_ribo.output.join(br_noribo.output)
            .map { sample, ribo_file, noribo_file -> [sample, [ribo_file, noribo_file]] }
        br_per_sample = CONCATENATE_BRACKEN_PER_SAMPLE(br_combined, "bracken")
    emit:
        bracken = br_per_sample.output
        kraken = kr_per_sample.output
}
