# Configuration files

Nextflow configuration is controlled by `.config` files, which specify parameters and other options used in executing the pipeline.

All configuration files used in the pipeline are stored in the `configs` directory. To configure a specific pipeline run, copy the appropriate config file for that pipeline mode (e.g. `run.config` or `run_ont.config`) into the launch directory, rename it to `nextflow.config`, and edit it as appropriate. That config file will in turn call other, standard config files included in the `configs` directory.

The rest of this page describes the specific options present in each config file, with a focus on those intended to be copied and edited by users.

## Run workflow configuration (`configs/run.config` and `configs/run_ont.config`)

This configuration file controls the pipeline's main RUN workflow. Its options are as follows:

- `params.mode = "run"` [str]: This instructs the pipeline to execute the [core run workflow](./workflows/run.nf).
- `params.platform` [str] = The sequencing platform that generated the data. Currently only `illumina`, `aviti`, and `ont` are fully implemented.
- `params.base_dir` [str]: Path to the parent directory for the pipeline working and output directories.
- `params.ref_dir` [str]: Path to the directory containing the outputs of the [`index` workflow](./index.md).
- `params.sample_sheet` [str]: Path to the [sample sheet](./usage.md#11-the-sample-sheet) used for the pipeline run.
- `params.adapters` [str]: Path to the adapter file for adapter trimming (default [`ref/adapters.fasta`](./ref/adapters.fasta).
- `params.n_reads_profile` [int]: The number of reads per sample to run through taxonomic profiling (default 1 million).
- `params.bt2_score_threshold` [float]: The length-normalized Bowtie2 score threshold above which a read is considered a valid hit for a host-infecting virus (typically 15 or 20).
- `params.bracken_threshold` [int]: Minimum number of reads that must be assigned to a taxon for Bracken to include it. (default 1)
- `params.host_taxon` [str]: Host taxon to use for host-infecting virus identification with Kraken2. (default "vertebrate")
- `random_seed` [str]: Seed for non-deterministic processes. If left blank; a random seed will be chosen; we generally recommend setting a value for reproducibility.
- `process.queue` [str]: The [AWS Batch job queue](./batch.md) to use for this pipeline run.

## Index workflow (`configs/index.config`)

- `params.mode = "index"` [str]: This instructs the pipeline to execute the [index workflow](./workflows/index.nf).
- `params.base_dir` [str]: Path to the parent directory for the pipeline working and output directories.
- `params.taxonomy_url` [str]: URL for the NCBI taxonomy dump to be used in index generation.
- `params.virus_host_db_url` [str]: URL for Virus-Host DB.
- `params.human_url` [str]: URL for downloading the human genome in FASTA format, which is used in index construction for contaminant screening.
- `params.genome_urls` [list(str)]: URLs for downloading other common contaminant genomes.
- `params.ssu_url` [str]: URL for the SILVA SSU reference database, used in ribosomal classification.
- `params.lsu_url` [str]: URL for the SILVA LSU reference database, used in ribosomal classification.
- `params.host_taxon_db` [str]: Path to a TSV mapping host taxon names to taxids (default: [`ref/host-taxa.tsv`](./ref/host-taxa.tsv).
- `params.contaminants` [str]: Path to a local file containing other contaminant genomes to exclude during contaminant filtering (default [`ref/contaminants.fasta.gz`](./ref/contaminants.fasta.gz).
- `params.adapters` [str]: Path to the adapter file for adapter masking during reference DB generation (default [`ref/adapters.fasta`](./ref/adapters.fasta).
- `params.genome_patterns_exclude` [str]: Path to a text file specifying string patterns to hard-exclude genomes during viral genome DB generation (e.g. transgenic sequences) (default [`ref/hv_patterns_exclude.txt`](./ref/hv_patterns_exclude.txt).
- `params.kraken_db` [str]: Path to pre-generated Kraken2 reference database (we use the Standard database by default)
- `params.blast_db_name` [str]: The name of the BLAST database to use for optional validation of taxonomic assignments (should match the run workflow's `params.blast_db_prefix`).
- `params.ncbi_viral_params` [str]: Parameters to pass to ncbi-genome-download when generating viral genome DB. Must at a minimum specify `--section genbank` or `--section refseq`.
- `params.virus_taxid` [int]: The NCBI taxid for the Viruses taxon (currently 10239).
- `params.viral_taxids_exclude` [str]: Space-separated string of taxids to hard-exclude from the list of host-infecting viruses. Currently includes phage taxa that Virus-Host DB erroneously classifies as human-infecting.
- `params.host_taxa_screen`: Space-separated list of host taxon names to screen for when building the viral genome database. Should correspond to taxa included in `params.host_taxon_db`.
