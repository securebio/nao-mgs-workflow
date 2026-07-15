/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { DOWNLOAD_GENOME } from "../../../modules/local/downloadGenome"
include { BOWTIE2_INDEX } from "../../../modules/local/bowtie2"
include { MINIMAP2_INDEX } from "../../../modules/local/minimap2"

/***********
| WORKFLOW |
***********/

workflow MAKE_HUMAN_INDEX {
    take:
        human_genome_url
    main:
        ref_ch = channel
            .of(tuple(human_genome_url, "human"))  // (url, name)
        genome_ch = DOWNLOAD_GENOME(ref_ch)
        bowtie2_ch = BOWTIE2_INDEX(genome_ch, "bt2-human-index")
        minimap2_ch = MINIMAP2_INDEX(genome_ch, "mm2-human-index")
    emit:
        bt2 = bowtie2_ch
        mm2 = minimap2_ch.output
        genome = genome_ch  // downloaded human FASTA, reused to mask the viral screen index
}
