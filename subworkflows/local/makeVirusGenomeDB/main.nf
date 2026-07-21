/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { ENUMERATE_VIRAL_ACCESSIONS } from "../../../modules/local/enumerateViralAccessions"
include { FILTER_VIRAL_GENBANK_METADATA } from "../../../modules/local/filterViralGenbankMetadata"
include { DOWNLOAD_VIRAL_GENOMES } from "../../../modules/local/downloadViralGenomes"
include { PREPARE_VIRAL_METADATA } from "../../../modules/local/prepareViralMetadata"
include { CONCATENATE_GENOME_FASTA } from "../../../modules/local/concatenateGenomeFasta"
include { FILTER_GENOME_FASTA } from "../../../modules/local/filterGenomeFasta"
include { MASK_GENOME_FASTA } from "../../../modules/local/maskGenomeFasta"
include { GZIP_FILE_BARE } from "../../../modules/local/gzipFile"

/***********
| WORKFLOW |
***********/

workflow MAKE_VIRUS_GENOME_DB {
    take:
        virus_taxid // Top-level taxid to enumerate viral assemblies for
        assembly_source // Assembly source: "genbank", "refseq", or "all"
        virus_db // TSV giving taxonomic structure and host infection status of virus taxids
        other_params // Map containing:
                     // - datasets_summary_extra_args: Additional args for `datasets summary genome taxon`
                     // - datasets_download_extra_args: Additional args for `datasets download genome accession`
                     // - viral_accession_chunk_size: Max accessions per parallel download chunk
                     // - genome_patterns_exclude: File of sequence header patterns to exclude from genome DB
                     // - host_taxa_screen: Tuple of host taxa to include
                     // - adapters: FASTA file of adapters to mask
                     // - hdist: hdist (allowed mismatches) to use for bbduk adapter masking
                     // - entropy: entropy cutoff for bbduk filtering of low-complexity regions
                     // - polyx_len: minimum length of polyX runs to filter out with bbduk
    main:
        // 1. Enumerate every assembly under the viral root in a single
        //    `datasets summary` call. No genome data is fetched here.
        enum_ch = ENUMERATE_VIRAL_ACCESSIONS(virus_taxid, assembly_source, other_params.datasets_summary_extra_args, "assembly")
        // 1b. Publish the full pre-filter assembly metadata (gzipped).
        raw_metadata_ch = GZIP_FILE_BARE(enum_ch.metadata)
        // 2. Filter accessions by host infection status and assembly status
        //    (hard-excluded taxids are already absent from virus_db's
        //    infection_status_* columns), then chunk the kept accessions.
        filter_ch = FILTER_VIRAL_GENBANK_METADATA(
            enum_ch.metadata, virus_db, other_params.host_taxa_screen,
            other_params.viral_accession_chunk_size, "virus-genome"
        )
        // 3. Download genomes per chunk in parallel. `flatten()` turns the
        //    list of chunk files into one channel emission per chunk so
        //    Nextflow can fan out tasks. Each task emits a single combined
        //    FASTA plus an assembly_accession -> genome_id map.
        chunk_ch = filter_ch.accession_chunks.flatten()
        download_ch = DOWNLOAD_VIRAL_GENOMES(chunk_ch, assembly_source, other_params.datasets_download_extra_args, 5, "assembly")
        // 4. Merge the per-chunk maps, then join with the filtered metadata to
        //    add species_taxid and expand each assembly to one row per genome_id.
        merged_map_ch = download_ch.accession_map.collectFile(
            name: "accession_map.tsv", keepHeader: true, skip: 1
        )
        gid_ch = PREPARE_VIRAL_METADATA(filter_ch.db, virus_db, merged_map_ch).metadata
        // 5. Concatenate the per-chunk combined genome FASTAs (dedup by name).
        genome_concat_ch = CONCATENATE_GENOME_FASTA(download_ch.genomes.collect())
        // 6. Filter to remove undesired/contaminated genomes by sequence-header
        //    pattern (genome_patterns_exclude only matchable post-download).
        filter_genome_ch = FILTER_GENOME_FASTA(genome_concat_ch, other_params.genome_patterns_exclude, "virus-genomes-filtered")
        // 7. Mask to remove adapters, low-entropy regions, and polyX.
        mask_params = other_params + [name_pattern: "virus-genomes"]
        mask_ch = MASK_GENOME_FASTA(filter_genome_ch, other_params.adapters, mask_params)
    emit:
        fasta = mask_ch.masked
        metadata = gid_ch
        raw_metadata = raw_metadata_ch  // pre-filter assembly metadata, for benchmarking
}
