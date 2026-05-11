/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { ENUMERATE_VIRAL_ACCESSIONS } from "../../../modules/local/enumerateViralAccessions"
include { FILTER_VIRAL_GENBANK_METADATA } from "../../../modules/local/filterViralGenbankMetadata"
include { DOWNLOAD_VIRAL_GENOMES } from "../../../modules/local/downloadViralGenomes"
include { PREPARE_VIRAL_METADATA } from "../../../modules/local/prepareViralMetadata"
include { ADD_GENBANK_GENOME_IDS } from "../../../modules/local/addGenbankGenomeIDs"
include { CONCATENATE_GENOME_FASTA } from "../../../modules/local/concatenateGenomeFasta"
include { FILTER_GENOME_FASTA } from "../../../modules/local/filterGenomeFasta"
include { MASK_GENOME_FASTA } from "../../../modules/local/maskGenomeFasta"

/***********
| WORKFLOW |
***********/

workflow MAKE_VIRUS_GENOME_DB {
    take:
        virus_taxid // Top-level taxid to enumerate viral assemblies for
        assembly_source // Assembly source: "genbank", "refseq", or "all"
        datasets_summary_extra_args // Additional args passed to `datasets summary genome taxon` in ENUMERATE
        datasets_download_extra_args // Additional args passed to `datasets download genome accession` in DOWNLOAD
        chunk_size // Max accessions per parallel download chunk
        virus_db // TSV giving taxonomic structure and host infection status of virus taxids
        other_params // Map containing:
                     // - genome_patterns_exclude: File of sequence header patterns to exclude from genome DB
                     // - host_taxa_screen: Tuple of host taxa to include
                     // - adapters: FASTA file of adapters to mask
                     // - hdist: hdist (allowed mismatches) to use for bbduk adapter masking
                     // - entropy: entropy cutoff for bbduk filtering of low-complexity regions
                     // - polyx_len: minimum length of polyX runs to filter out with bbduk
    main:
        // 1. Enumerate every assembly under the viral root in a single
        //    `datasets summary` call. No genome data is fetched here.
        enum_ch = ENUMERATE_VIRAL_ACCESSIONS(virus_taxid, assembly_source, datasets_summary_extra_args)
        // 2. Filter accessions by host infection status and assembly status
        //    (hard-excluded taxids are already absent from virus_db's
        //    infection_status_* columns), then chunk the kept accessions.
        filter_ch = FILTER_VIRAL_GENBANK_METADATA(
            enum_ch.metadata, virus_db, other_params.host_taxa_screen,
            chunk_size, "virus-genome"
        )
        // 3. Download genomes per chunk in parallel. `flatten()` turns the
        //    list of chunk files into one channel emission per chunk so
        //    Nextflow can fan out tasks. Runs on /scratch (see module).
        chunk_ch = filter_ch.accession_chunks.flatten()
        download_ch = DOWNLOAD_VIRAL_GENOMES(chunk_ch, assembly_source, datasets_download_extra_args, 5)
        // 4. Match downloaded files to filtered metadata, populate
        //    species_taxid + local_filename, and emit symlinked dir + paths.
        prepare_ch = PREPARE_VIRAL_METADATA(
            filter_ch.db, virus_db, download_ch.genomes.collect()
        )
        // 5. Add per-sequence genome IDs by reading FASTA headers.
        gid_ch = ADD_GENBANK_GENOME_IDS(prepare_ch.metadata, prepare_ch.genomes, "virus-genome")
        // 6. Concatenate matching genomes.
        genome_concat_ch = CONCATENATE_GENOME_FASTA(prepare_ch.genomes, prepare_ch.paths)
        // 7. Filter to remove undesired/contaminated genomes by sequence-header
        //    pattern (genome_patterns_exclude only matchable post-download).
        filter_genome_ch = FILTER_GENOME_FASTA(genome_concat_ch, other_params.genome_patterns_exclude, "virus-genomes-filtered")
        // 8. Mask to remove adapters, low-entropy regions, and polyX.
        mask_params = other_params + [name_pattern: "virus-genomes"]
        mask_ch = MASK_GENOME_FASTA(filter_genome_ch, other_params.adapters, mask_params)
    emit:
        fasta = mask_ch.masked
        metadata = gid_ch
}
