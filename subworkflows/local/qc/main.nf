/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { FASTQC_LABELED as FASTQC } from "../../../modules/local/fastqc"
include { MULTIQC_LABELED as MULTIQC } from "../../../modules/local/multiqc"
include { SUMMARIZE_MULTIQC } from "../../../modules/local/summarizeMultiqc"

/***********
| WORKFLOW |
***********/

workflow QC {
    take:
        reads
        stage_label
        single_end
    main:
        // 1. Run FASTQC on each read file (single-end or interleaved)
        fastqc_ch = FASTQC(reads)
        // 2. Extract data with MultiQC
        multiqc_ch = MULTIQC(stage_label, fastqc_ch.zip)
        // 3. Summarize MultiQC information for each read file / pair of read files
        process_ch = SUMMARIZE_MULTIQC(multiqc_ch.data, single_end)
    emit:
        qc = process_ch  // tuple(sample, basic, adapt, qbase, qseqs, lengths) per sample
        test_input = reads
}
