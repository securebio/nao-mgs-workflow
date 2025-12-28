/**************************************************************************************
| WORKFLOW: GENERATE INDEX AND REFERENCE FILES FOR DOWNSTREAM PROCESSING AND ANALYSIS |
**************************************************************************************/

/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { MAKE_VIRUS_TAXONOMY_DB } from "../subworkflows/local/makeVirusTaxonomyDB"
include { MAKE_VIRUS_GENOME_DB } from "../subworkflows/local/makeVirusGenomeDB"
include { WGET as WGET_SSU } from "../modules/local/wget"
include { WGET as WGET_LSU } from "../modules/local/wget"
include { JOIN_RIBO_REF } from "../modules/local/joinRiboRef"
include { DOWNLOAD_BLAST_DB } from "../modules/local/downloadBlastDB"
include { MAKE_HUMAN_INDEX } from "../subworkflows/local/makeHumanIndex"
include { MAKE_CONTAMINANT_INDEX } from "../subworkflows/local/makeContaminantIndex"
include { MAKE_VIRUS_INDEX } from "../subworkflows/local/makeVirusIndex"
include { MAKE_RIBO_INDEX } from "../subworkflows/local/makeRiboIndex"
include { GET_TARBALL as GET_KRAKEN_DB } from "../modules/local/getTarball"
include { GET_TARBALL as GET_BLAST_DB } from "../modules/local/getTarball"
include { COPY_FILE_BARE as COPY_VERSION } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_COMPAT } from "../modules/local/copyFile"

/****************
| MAIN WORKFLOW |
****************/

workflow INDEX {
    main:
        // Start time
        start_time = new Date()
        start_time_str = start_time.format("YYYY-MM-dd HH:mm:ss z (Z)")
        // Build viral taxonomy and infection DB
        MAKE_VIRUS_TAXONOMY_DB(params.taxonomy_url, params.virus_host_db_url,
            params.host_taxon_db, params.virus_taxid,
            params.viral_taxids_exclude_hard)
        // Get reference DB of viral genomes of interest
        virus_genome_params = params.collectEntries { k, v -> [k, v] }
        virus_genome_params.putAll([k: "20", hdist: "3", entropy: "0.5", polyx_len: "10"])
        MAKE_VIRUS_GENOME_DB(params.ncbi_viral_params, MAKE_VIRUS_TAXONOMY_DB.out.db, virus_genome_params)
        // Download ribosomal references
        WGET_SSU(params.ssu_url, "ssu_ref.fasta.gz")
        WGET_LSU(params.lsu_url, "lsu_ref.fasta.gz")
        // Build alignment indices
        JOIN_RIBO_REF(WGET_SSU.out.file, WGET_LSU.out.file)
        MAKE_VIRUS_INDEX(MAKE_VIRUS_GENOME_DB.out.fasta)
        MAKE_HUMAN_INDEX(params.human_url)
        MAKE_CONTAMINANT_INDEX(params.genome_urls, params.contaminants)
        MAKE_RIBO_INDEX(JOIN_RIBO_REF.out.ribo_ref)
        // Other index files
        if (params.blast_db_name.startsWith("http://") || params.blast_db_name.startsWith("https://")) {
            GET_BLAST_DB(params.blast_db_name, "blast_db", false)
            blast_db_ch = GET_BLAST_DB.out
        } else {
            DOWNLOAD_BLAST_DB(params.blast_db_name)
            blast_db_ch = DOWNLOAD_BLAST_DB.out.db
        }
        GET_KRAKEN_DB(params.kraken_db, "kraken_db", true)
        // Prepare results for publishing
        params_str = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(params))
        params_ch = Channel.of(params_str).collectFile(name: "index-params.json")
        time_ch = Channel.of(start_time_str + "\n").collectFile(name: "time.txt")
        version_path = file("${projectDir}/pipeline-version.txt")
        version_newpath = version_path.getFileName().toString()
        version_ch = COPY_VERSION(Channel.fromPath(version_path), version_newpath)
        version_path = file("${projectDir}/pipeline-version.txt")
        version_newpath = version_path.getFileName().toString()
        compat_ch = file("${projectDir}/index-min-pipeline-version.txt")
        compat_newpath = compat_ch.getFileName().toString()
        compatibility_ch = COPY_COMPAT(Channel.fromPath(compat_ch), compat_newpath)

    emit:
        input_index = params_ch
        logging_index = time_ch.mix(version_ch, compatibility_ch)
        // Lots of results; split across 2 channels (reference databases and bowtie2/minimap2 indexes)
        ref_dbs = MAKE_VIRUS_TAXONOMY_DB.out.db.mix( // Taxonomy and virus databases
            MAKE_VIRUS_TAXONOMY_DB.out.nodes,
            MAKE_VIRUS_TAXONOMY_DB.out.names,
            // Virus genome database
            MAKE_VIRUS_GENOME_DB.out.fasta,
            MAKE_VIRUS_GENOME_DB.out.metadata,
            // Other reference files & directories
            JOIN_RIBO_REF.out.ribo_ref,
            blast_db_ch,
            GET_KRAKEN_DB.out
        )
        alignment_indexes = MAKE_HUMAN_INDEX.out.bt2.mix( // Bowtie2 alignment indexes
            MAKE_CONTAMINANT_INDEX.out.bt2,
            MAKE_VIRUS_INDEX.out.bt2,
            // Minimap2 alignment indices
            MAKE_VIRUS_INDEX.out.mm2,
            MAKE_HUMAN_INDEX.out.mm2,
            MAKE_RIBO_INDEX.out.mm2,
            MAKE_CONTAMINANT_INDEX.out.mm2
        )
}
