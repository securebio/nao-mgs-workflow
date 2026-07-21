/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

// The assembly and sequence branches each run ENUMERATE / FILTER / DOWNLOAD, so
// import per-branch aliases (a Nextflow process can only be invoked once).
include { ENUMERATE_VIRAL_ACCESSIONS as ENUMERATE_ASSEMBLY } from "../../../modules/local/enumerateViralAccessions"
include { ENUMERATE_VIRAL_ACCESSIONS as ENUMERATE_SEQUENCE } from "../../../modules/local/enumerateViralAccessions"
include { FILTER_SEQUENCE_TAXA } from "../../../modules/local/filterSequenceTaxa"
include { FILTER_VIRAL_GENBANK_METADATA as FILTER_ASSEMBLY } from "../../../modules/local/filterViralGenbankMetadata"
include { FILTER_VIRAL_GENBANK_METADATA as FILTER_SEQUENCE } from "../../../modules/local/filterViralGenbankMetadata"
include { DOWNLOAD_VIRAL_GENOMES as DOWNLOAD_ASSEMBLY } from "../../../modules/local/downloadViralGenomes"
include { DOWNLOAD_VIRAL_GENOMES as DOWNLOAD_SEQUENCE } from "../../../modules/local/downloadViralGenomes"
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
        virus_taxid // Top-level taxid to enumerate viral genomes for
        assembly_source // Source filter: "genbank", "refseq", or "all"
        virus_db // TSV giving taxonomic structure and host infection status of virus taxids
        virus_nodes // NCBI taxonomy nodes.dmp (used to exclude a clade from the sequence branch)
        other_params // Map containing:
                     // - virus_source: "assembly" | "sequence" | "both" (which branches to run; default "assembly")
                     // - datasets_summary_extra_args: Additional args for `datasets summary genome taxon` (assembly branch)
                     // - datasets_download_extra_args: Additional args for `datasets download genome accession` (assembly branch)
                     // - datasets_summary_seq_extra_args: Additional args for `datasets summary virus genome taxon` (sequence branch)
                     // - datasets_download_seq_extra_args: Additional args for `datasets download virus genome accession` (sequence branch)
                     // - sequence_exclude_taxid: Root taxid of the clade to drop from the sequence branch (e.g. 11308 = influenza)
                     // - viral_accession_chunk_size: Max accessions per parallel download chunk
                     // - genome_patterns_exclude: File of sequence header patterns to exclude from genome DB
                     // - host_taxa_screen: Tuple of host taxa to include
                     // - adapters: FASTA file of adapters to mask
                     // - hdist: hdist (allowed mismatches) to use for bbduk adapter masking
                     // - entropy: entropy cutoff for bbduk filtering of low-complexity regions
                     // - polyx_len: minimum length of polyX runs to filter out with bbduk
    main:
        // Which sourcing branches to run. NCBI genome assemblies (the historical
        // path) are frozen for non-influenza viruses (~2025); NCBI Virus / nuccore
        // sequence records recover the recent ones. "both" unions them; "assembly"
        // (the default) reproduces today's behavior exactly.
        virus_source = other_params.virus_source ?: "assembly"
        if (!(virus_source in ["assembly", "sequence", "both"])) {
            throw new IllegalArgumentException(
                "MAKE_VIRUS_GENOME_DB: invalid virus_source '${virus_source}' (expected 'assembly', 'sequence', or 'both')")
        }
        run_assembly = virus_source in ["assembly", "both"]
        run_sequence = virus_source in ["sequence", "both"]

        // Per-branch outputs are collected into these channels, then unioned.
        raw_meta_ch = Channel.empty()    // pre-filter enumerate metadata (for benchmarking)
        filtered_db_ch = Channel.empty() // host/status-filtered metadata
        genomes_ch = Channel.empty()     // per-chunk combined genome FASTAs
        maps_ch = Channel.empty()        // per-chunk assembly_accession -> genome_id maps

        // --- Assembly branch: NCBI genome assemblies (GCA/GCF). ---
        if (run_assembly) {
            enum_a = ENUMERATE_ASSEMBLY(
                virus_taxid, assembly_source, other_params.datasets_summary_extra_args, "assembly"
            )
            filter_a = FILTER_ASSEMBLY(
                enum_a.metadata, virus_db, other_params.host_taxa_screen,
                other_params.viral_accession_chunk_size, "virus-genome"
            )
            download_a = DOWNLOAD_ASSEMBLY(
                filter_a.accession_chunks.flatten(), assembly_source,
                other_params.datasets_download_extra_args, 5, "assembly"
            )
            raw_meta_ch = raw_meta_ch.mix(enum_a.metadata)
            filtered_db_ch = filtered_db_ch.mix(filter_a.db)
            genomes_ch = genomes_ch.mix(download_a.genomes)
            maps_ch = maps_ch.mix(download_a.accession_map)
        }

        // --- Sequence branch: NCBI Virus / nuccore records, flu excluded. ---
        if (run_sequence) {
            enum_s = ENUMERATE_SEQUENCE(
                virus_taxid, assembly_source, other_params.datasets_summary_seq_extra_args, "sequence"
            )
            // Drop the influenza clade: NCBI keeps flu on grouped assemblies
            // (captured by the assembly branch), so the sequence branch would
            // otherwise re-add thousands of ungrouped flu segments.
            seqfilt_s = FILTER_SEQUENCE_TAXA(
                enum_s.metadata, virus_nodes, other_params.sequence_exclude_taxid
            )
            filter_s = FILTER_SEQUENCE(
                seqfilt_s.metadata, virus_db, other_params.host_taxa_screen,
                other_params.viral_accession_chunk_size, "virus-genome"
            )
            download_s = DOWNLOAD_SEQUENCE(
                filter_s.accession_chunks.flatten(), assembly_source,
                other_params.datasets_download_seq_extra_args, 5, "sequence"
            )
            raw_meta_ch = raw_meta_ch.mix(enum_s.metadata)
            filtered_db_ch = filtered_db_ch.mix(filter_s.db)
            genomes_ch = genomes_ch.mix(download_s.genomes)
            maps_ch = maps_ch.mix(download_s.accession_map)
        }

        // Publish the (unioned) pre-filter enumerate metadata, gzipped. The fixed
        // name keeps the `virus-genome-metadata-raw.tsv.gz` output contract and
        // works whether one or both branches ran.
        raw_merged_ch = raw_meta_ch.collectFile(
            name: "virus-genome-metadata-raw.tsv", keepHeader: true, skip: 1
        )
        raw_metadata_ch = GZIP_FILE_BARE(raw_merged_ch)

        // Merge the per-chunk maps and per-branch filtered metadata, then join to
        // add species_taxid and expand each accession to one row per genome_id
        // (cross-source duplicates collapse, preferring the assembly-branch row).
        merged_map_ch = maps_ch.collectFile(
            name: "accession_map.tsv", keepHeader: true, skip: 1
        )
        merged_db_ch = filtered_db_ch.collectFile(
            name: "filtered-metadata.tsv", keepHeader: true, skip: 1
        )
        gid_ch = PREPARE_VIRAL_METADATA(merged_db_ch, virus_db, merged_map_ch).metadata

        // Concatenate every branch's per-chunk combined FASTA (dedup by name).
        genome_concat_ch = CONCATENATE_GENOME_FASTA(genomes_ch.collect())
        // Filter to remove undesired/contaminated genomes by sequence-header
        // pattern (genome_patterns_exclude only matchable post-download).
        filter_genome_ch = FILTER_GENOME_FASTA(genome_concat_ch, other_params.genome_patterns_exclude, "virus-genomes-filtered")
        // Mask to remove adapters, low-entropy regions, and polyX.
        mask_params = other_params + [name_pattern: "virus-genomes"]
        mask_ch = MASK_GENOME_FASTA(filter_genome_ch, other_params.adapters, mask_params)
    emit:
        fasta = mask_ch.masked
        metadata = gid_ch
        raw_metadata = raw_metadata_ch  // pre-filter enumerate metadata, for benchmarking
}
