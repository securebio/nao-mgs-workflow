// Standalone benchmark for ADD_GENBANK_GENOME_IDS.
// Imports the module unchanged; resource sizing per cohort is supplied via
// `process { withName: ... }` blocks in bench/cohort_*.config.

nextflow.enable.dsl = 2

include { ADD_GENBANK_GENOME_IDS } from "./modules/local/addGenbankGenomeIDs"

workflow {
    metadata = file(params.metadata)
    genomes_dir = file(params.genomes_dir)
    ADD_GENBANK_GENOME_IDS(metadata, genomes_dir, "virus-genome")
}
