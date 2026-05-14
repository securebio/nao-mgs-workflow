// Standalone benchmark for CONCATENATE_GENOME_FASTA.
// Imports the module unchanged; resource sizing per cohort is supplied via
// `process { withName: ... }` blocks in bench/cohort_*.config.
//
// Run: see bench/launch.sh for the four-cohort invocation.

nextflow.enable.dsl = 2

include { CONCATENATE_GENOME_FASTA } from "./modules/local/concatenateGenomeFasta"

workflow {
    genome_dir = file(params.genome_dir)
    paths_file = file(params.paths_file)
    CONCATENATE_GENOME_FASTA(genome_dir, paths_file)
}
