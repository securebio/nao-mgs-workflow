/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { QC as PRE_ADAPTER_TRIM_QC } from "../../../subworkflows/local/qc"
include { QC as POST_ADAPTER_TRIM_QC } from "../../../subworkflows/local/qc"

/***********
| WORKFLOW |
***********/

workflow RUN_QC {
    take:
      subset_reads
      trimmed_subset_reads
      single_end
    main:
      // Run FASTQC before and after adapter trimming
      // QC outputs tuple(sample, basic, adapt, qbase, qseqs, lengths)
      pre_qc_ch = PRE_ADAPTER_TRIM_QC(subset_reads, "raw", single_end)
      post_qc_ch = POST_ADAPTER_TRIM_QC(trimmed_subset_reads, "cleaned", single_end)
    emit:
      pre_qc = pre_qc_ch.qc   // tuple(sample, basic, adapt, qbase, qseqs, lengths) per sample
      post_qc = post_qc_ch.qc // tuple(sample, basic, adapt, qbase, qseqs, lengths) per sample
}
