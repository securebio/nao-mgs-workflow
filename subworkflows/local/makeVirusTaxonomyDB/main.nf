/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { DOWNLOAD_NCBI_TAXONOMY } from "../../../modules/local/downloadNcbiTaxonomy"
include { EXTRACT_NCBI_TAXONOMY } from "../../../modules/local/extractNcbiTaxonomy"
include { DOWNLOAD_VIRUS_HOST_DB } from "../../../modules/local/downloadVirusHostDB"
include { BUILD_VIRUS_TAXID_DB } from "../../../modules/local/buildVirusTaxidDB"
include { RAISE_TAXONOMY_RANKS } from "../../../modules/local/raiseTaxonomyRanks"
include { ANNOTATE_VIRUS_INFECTION } from "../../../modules/local/annotateVirusInfection"

/***********
| WORKFLOW |
***********/

workflow MAKE_VIRUS_TAXONOMY_DB {
    take:
        taxonomy_url // URL pointing to NCBI taxonomy files
        virus_host_db_url // URL pointing to Virus-Host-DB
        host_taxon_db // TSV giving host taxa to annotate infection status
        virus_taxid // Top-level taxid for viruses as a whole
        hard_exclude_taxids // Virus taxids to hard-exclude from host annotation
        host_infection_overrides // JSON of per-host taxid overrides forcing MATCH
    main:
        // Get NCBI taxonomy
        dl_ch = DOWNLOAD_NCBI_TAXONOMY(taxonomy_url)
        ext_ch = EXTRACT_NCBI_TAXONOMY(dl_ch)
        // Get Virus-Host-DB
        vh_ch = DOWNLOAD_VIRUS_HOST_DB(virus_host_db_url)
        // Build virus taxid DB from NCBI taxonomy files
        virus_ch = BUILD_VIRUS_TAXID_DB(ext_ch.nodes, ext_ch.names, virus_taxid)
        // Add rank-raised taxids
        raised_ch = RAISE_TAXONOMY_RANKS(virus_ch, "species genus family order class phylum")
        // Annotate virus taxid DB with infection status for host taxa
        annot_ch = ANNOTATE_VIRUS_INFECTION(raised_ch, host_taxon_db, vh_ch,
            ext_ch.nodes, hard_exclude_taxids, host_infection_overrides)
    emit:
        db = annot_ch
        nodes = ext_ch.nodes
        names = ext_ch.names
}
