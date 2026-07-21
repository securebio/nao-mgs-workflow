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
include { COPY_FILE_BARE as COPY_PYPROJECT } from "../modules/local/copyFile"
include { COPY_FILE_BARE as COPY_OVERRIDES } from "../modules/local/copyFile"

/****************
| MAIN WORKFLOW |
****************/

workflow INDEX {
    main:
        // Start time
        start_time = new Date()
        start_time_str = start_time.format("yyyy-MM-dd HH:mm:ss z (Z)")
        // Build viral taxonomy and infection DB
        taxonomy_ch = MAKE_VIRUS_TAXONOMY_DB(params.taxonomy_url, params.virus_host_db_url,
            params.host_taxon_db, params.virus_taxid,
            params.viral_taxids_exclude_hard,
            params.host_infection_overrides)
        // Get reference DB of viral genomes of interest
        virus_genome_params = params.collectEntries { k, v -> [k, v] }
        virus_genome_params.putAll([k: "20", hdist: "3", entropy: "0.5", polyx_len: "10"])
        genome_ch = MAKE_VIRUS_GENOME_DB(
            params.download_virus_taxid ?: params.virus_taxid,
            params.assembly_source,
            taxonomy_ch.db,
            taxonomy_ch.nodes,
            virus_genome_params
        )
        // Download ribosomal references
        ssu_ch = WGET_SSU(params.ssu_url, "ssu_ref.fasta.gz")
        lsu_ch = WGET_LSU(params.lsu_url, "lsu_ref.fasta.gz")
        // Build alignment indices
        ribo_ref_ch = JOIN_RIBO_REF(ssu_ch.file, lsu_ch.file)
        human_index_ch = MAKE_HUMAN_INDEX(params.human_url)
        virus_index_ch = MAKE_VIRUS_INDEX(genome_ch.fasta, human_index_ch.human_genome, params.nucleaze_k)
        contaminant_index_ch = MAKE_CONTAMINANT_INDEX(params.genome_urls, params.contaminants)
        ribo_index_ch = MAKE_RIBO_INDEX(ribo_ref_ch.ribo_ref)
        // Other index files
        blast_db_ch = DOWNLOAD_BLAST_DB(params.blast_db_name).db
        kraken_ch = GET_KRAKEN_DB(params.kraken_db, "kraken_db", true)
        // Prepare results for publishing
        params_str = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(params))
        params_ch = channel.of(params_str).collectFile(name: "index-params.json")
        time_ch = channel.of(start_time_str + "\n").collectFile(name: "time.txt")
        pipeline_pyproject_path = file("${projectDir}/pyproject.toml")
        pyproject_ch = COPY_PYPROJECT(channel.fromPath(pipeline_pyproject_path), "pyproject.toml")
        // Publish the host-infection overrides alongside the params so index
        // outputs record the surveillance rules used to build them.
        overrides_ch = COPY_OVERRIDES(
            channel.fromPath(file(params.host_infection_overrides)),
            "host-infection-overrides.json")

    emit:
        input_index = params_ch.mix(overrides_ch)
        logging_index = time_ch.mix(pyproject_ch)
        // Lots of results; split across 2 channels (reference databases and bowtie2/minimap2 indexes)
        ref_dbs = taxonomy_ch.db.mix( // Taxonomy and virus databases
            taxonomy_ch.nodes,
            taxonomy_ch.names,
            // Virus genome database
            genome_ch.fasta,
            genome_ch.metadata,
            genome_ch.raw_metadata,
            // Other reference files & directories
            ribo_ref_ch.ribo_ref,
            blast_db_ch,
            kraken_ch
        )
        alignment_indexes = human_index_ch.bt2.mix( // Bowtie2 alignment indexes
            contaminant_index_ch.bt2,
            virus_index_ch.bt2,
            // Minimap2 alignment indices
            virus_index_ch.mm2,
            human_index_ch.mm2,
            ribo_index_ch.mm2,
            contaminant_index_ch.mm2,
            // Nucleaze k-mer index for the viral screen in RUN
            virus_index_ch.nucleaze
        )
        experimental_index = channel.empty()
}
