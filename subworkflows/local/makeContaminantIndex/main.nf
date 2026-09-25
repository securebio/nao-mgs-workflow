/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { DOWNLOAD_GENOME } from "../../../modules/local/downloadGenome"
include { CONCATENATE_FASTA_GZIPPED } from "../../../modules/local/concatenateFasta"
include { BOWTIE2_INDEX } from "../../../modules/local/bowtie2"
include { MINIMAP2_INDEX } from "../../../modules/local/minimap2"

/***********
| WORKFLOW |
***********/

workflow MAKE_CONTAMINANT_INDEX {
    take:
        genome_urls
        contaminants_path
    main:
        // Download reference genomes
        ref_ch = channel
            .fromList(genome_urls.entrySet())
            .map { entry ->
                tuple(entry.value, entry.key)  // (url, name)
            }

        downloaded_ch = DOWNLOAD_GENOME(ref_ch)
        // Gather only once every download has arrived: an ignored download would otherwise
        // build the index without that genome
        n_genomes = genome_urls.size()
        all_downloads_ch = downloaded_ch
            .map { f -> [groupKey("genomes", n_genomes), f] }
            .groupTuple() // complete: sized with groupKey, so a failed download drops the gather
            .map { _key, files -> files }

        combined_ch = all_downloads_ch
            .mix(channel.fromPath(contaminants_path))
            .collect() // complete: every download is gathered above

        // Then use combined_ch for the rest of your workflow
        genome_ch = CONCATENATE_FASTA_GZIPPED(combined_ch, "ref_concat")

        // Make indexes
        bowtie2_ch = BOWTIE2_INDEX(genome_ch, "bt2-other-index")
        minimap2_ch = MINIMAP2_INDEX(genome_ch, "mm2-other-index")
    emit:
        bt2 = bowtie2_ch
        mm2 = minimap2_ch.output
}
