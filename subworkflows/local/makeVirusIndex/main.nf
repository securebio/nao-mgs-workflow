/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { DUSTMASKER_FASTA_GZIPPED } from "../../../modules/local/dustmasker"
include { MASK_GENOME_FASTA_WITH_HUMAN } from "../../../modules/local/maskGenomeFastaWithHuman"
include { BOWTIE2_INDEX } from "../../../modules/local/bowtie2"
include { MINIMAP2_INDEX } from "../../../modules/local/minimap2"
include { NUCLEAZE_INDEX } from "../../../modules/local/nucleazeIndex"

/***********
| WORKFLOW |
***********/

workflow MAKE_VIRUS_INDEX {
    take:
        virus_genome_fasta
        human_genome_fasta
        nucleaze_k
    main:
        mask_ch = DUSTMASKER_FASTA_GZIPPED(virus_genome_fasta)
        bowtie2_ch = BOWTIE2_INDEX(mask_ch, "bt2-virus-index")
        minimap2_ch = MINIMAP2_INDEX(mask_ch, "mm2-virus-index")
        // Further mask human (CHM13) k-mers from the viral genomes prior to
        // nucleaze k-mer index building.
        human_masked_ch = MASK_GENOME_FASTA_WITH_HUMAN(
            virus_genome_fasta, human_genome_fasta,
            [k: nucleaze_k, hdist: 0, name_pattern: "virus-genomes"]
        )
        // Index off the BBDuk-masked FASTA, not the further-dustmasked
        // variant fed to bowtie2/minimap2 — matches what RUN's screen sees.
        nucleaze_ch = NUCLEAZE_INDEX(human_masked_ch.masked, nucleaze_k, "virus-genomes-masked")
    emit:
        bt2 = bowtie2_ch
        mm2 = minimap2_ch.output
        nucleaze = nucleaze_ch.index
        human_mask_log = human_masked_ch.log
}
// TODO: Consider changing masking tool
