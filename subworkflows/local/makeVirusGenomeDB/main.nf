/***************************
| MODULES AND SUBWORKFLOWS |
***************************/

include { ENUMERATE_CHILD_TAXA } from "../../../modules/local/enumerateChildTaxa"
include { DOWNLOAD_VIRAL_GENOMES } from "../../../modules/local/downloadViralGenomes"
include { CONCATENATE_TSVS } from "../../../modules/local/concatenateTsvs"
include { PREPARE_VIRAL_METADATA } from "../../../modules/local/prepareViralMetadata"
include { FILTER_VIRAL_GENBANK_METADATA } from "../../../modules/local/filterViralGenbankMetadata"
include { ADD_GENBANK_GENOME_IDS } from "../../../modules/local/addGenbankGenomeIDs"
include { CONCATENATE_GENOME_FASTA } from "../../../modules/local/concatenateGenomeFasta"
include { FILTER_GENOME_FASTA } from "../../../modules/local/filterGenomeFasta"
include { MASK_GENOME_FASTA } from "../../../modules/local/maskGenomeFasta"

/***********
| WORKFLOW |
***********/

workflow MAKE_VIRUS_GENOME_DB {
    take:
        virus_taxid // Taxid to enumerate child taxa for parallel genome downloads
        assembly_source // Assembly source: "genbank" or "refseq"
        datasets_extra_args // Additional arguments passed to `datasets download genome taxon` (e.g. "--assembly-level complete")
        virus_db // TSV giving taxonomic structure and host infection status of virus taxids
        taxonomy_nodes // NCBI taxonomy nodes.dmp file
        other_params // Map containing:
                     // - genome_patterns_exclude: File of sequence header patterns to exclude from genome DB
                     // - host_taxa_screen: Tuple of host taxa to include
                     // - adapters: FASTA file of adapters to mask
                     // - hdist: hdist (allowed mismatches) to use for bbduk adapter masking
                     // - entropy: entropy cutoff for bbduk filtering of low-complexity regions
                     // - polyx_len: minimum length of polyX runs to filter out with bbduk
    main:
        // 1. Enumerate child taxa for parallel download
        ENUMERATE_CHILD_TAXA(taxonomy_nodes, virus_taxid)
        child_taxids_ch = ENUMERATE_CHILD_TAXA.out.taxids
            .splitText().map { it.trim() }.filter { it }

        // 2. Download genomes per child taxon in parallel
        DOWNLOAD_VIRAL_GENOMES(child_taxids_ch, assembly_source, datasets_extra_args)

        // 3. Merge per-taxon metadata using existing CONCATENATE_TSVS
        CONCATENATE_TSVS(
            DOWNLOAD_VIRAL_GENOMES.out.metadata.collect(),
            "ncbi-viral-metadata-raw"
        )

        // 4. Prepare final metadata (add species_taxid, local_filename)
        PREPARE_VIRAL_METADATA(
            CONCATENATE_TSVS.out.output,
            virus_db,
            DOWNLOAD_VIRAL_GENOMES.out.genomes.collect()
        )

        // 5. Filter genome metadata by taxid to identify genomes to retain
        meta_ch = FILTER_VIRAL_GENBANK_METADATA(PREPARE_VIRAL_METADATA.out.metadata, virus_db, other_params.host_taxa_screen, "virus-genome")
        // 6. Add genome IDs to Genbank metadata file
        gid_ch = ADD_GENBANK_GENOME_IDS(meta_ch.db, PREPARE_VIRAL_METADATA.out.genomes, "virus-genome")
        // 7. Concatenate matching genomes
        concat_ch = CONCATENATE_GENOME_FASTA(PREPARE_VIRAL_METADATA.out.genomes, meta_ch.path)
        // 8. Filter to remove undesired/contaminated genomes
        filter_ch = FILTER_GENOME_FASTA(concat_ch, other_params.genome_patterns_exclude, "virus-genomes-filtered")
	// 9. Mask to remove adapters, low-entropy regions, and polyX
	mask_params = other_params + [name_pattern: "virus-genomes"]
	mask_ch = MASK_GENOME_FASTA(filter_ch, other_params.adapters, mask_params)
    emit:
        fasta = mask_ch.masked
        metadata = gid_ch
}
