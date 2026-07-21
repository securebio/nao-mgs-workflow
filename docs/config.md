# Configuration files

Nextflow configuration is controlled by `.config` files, which specify parameters and other options used in executing the pipeline.

All configuration files used in the pipeline are stored in the `configs` directory. You can reference the appropriate config file directly with `-c` (e.g. `-c configs/run.config`), or copy it into the launch directory as `nextflow.config` if you need to customize non-default settings.

Any `params.*` value can be overridden on the command line using `--<param> <value>` (e.g. `--queue my-batch-queue`, `--base_dir s3://my-bucket/run1`). This is the recommended way to set per-run values like `base_dir`, `ref_dir`, `platform`, and `queue`.

The rest of this page describes the specific options present in each config file.

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
- `params.random_seed` [str]: Seed for non-deterministic processes. If left blank; a random seed will be chosen; we generally recommend setting a value for reproducibility.
- `params.queue` [str]: The [AWS Batch job queue](./batch.md) to use for this pipeline run. For [spot instance fallback](./batch.md#spot-instance-fallback) using a Groovy closure, edit `process.queue` directly in the config file.

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
- `params.host_infection_overrides` [str]: Path to a JSON file of manual per-taxid host-group overrides forcing `MATCH` (status=1) in the annotated virus DB (default: [`ref/host-infection-overrides.json`](./ref/host-infection-overrides.json)). Each entry lists a viral `taxid` and the `hosts` (names from `params.host_taxon_db`) for which it should be force-included; only the listed taxon is marked directly, with descendants and ancestors picking up their statuses through the normal propagation phases (i.e. the override behaves like a direct Virus-Host DB match). The override is applied after `params.viral_taxids_exclude_hard` so includes win on conflict at the override target. Use to repair taxa that upstream Virus-Host DB misses or misannotates. See [`docs/annotation.md`](./annotation.md) for where in the algorithm the override is applied.
- `params.contaminants` [str]: Path to a local file containing other contaminant genomes to exclude during contaminant filtering (default [`ref/contaminants.fasta.gz`](./ref/contaminants.fasta.gz).
- `params.adapters` [str]: Path to the adapter file for adapter masking during reference DB generation (default [`ref/adapters.fasta`](./ref/adapters.fasta).
- `params.genome_patterns_exclude` [str]: Path to a text file specifying string patterns to hard-exclude genomes during viral genome DB generation (e.g. transgenic sequences) (default [`ref/hv_patterns_exclude.txt`](./ref/hv_patterns_exclude.txt).
- `params.kraken_db` [str]: Path to pre-generated Kraken2 reference database (we use the Standard database by default)
- `params.blast_db_name` [str]: The BLAST database to download for optional validation of taxonomic assignments — either an `update_blastdb.pl` name (e.g. `core_nt`) or an `http(s)` `.tar.gz` URL (used for CI tests). INDEX publishes it under a fixed `results/blast_db/` directory with a `blast_db` alias.
- `params.assembly_source` [str]: Source filter for viral genomes via NCBI datasets CLI. Valid values: `"genbank"`, `"refseq"`, or `"all"`. Default: `"all"`. In the sequence branch, `"refseq"` maps to `--refseq` and `"genbank"` warns and enumerates all sources (the NCBI Virus dataset has no GenBank-only filter).
- `params.virus_source` [str]: Which sourcing branches build the viral genome DB. Valid values: `"assembly"` (NCBI genome assemblies — the historical path), `"sequence"` (NCBI Virus / nuccore sequence records), or `"both"` (union of the two). Default: `"assembly"`. NCBI froze non-influenza viral genome assemblies (~2025), so `"both"`/`"sequence"` recover recent non-flu genomes that the assembly path can no longer see; `"assembly"` reproduces prior behavior exactly. In `"both"`, cross-source duplicates collapse (FASTA by `seqkit rmdup`, metadata by `genome_id`) and influenza is excluded from the sequence branch (see `sequence_exclude_taxid`).
- `params.datasets_summary_extra_args` [str]: Additional arguments passed to `datasets summary genome taxon` in the assembly branch of `ENUMERATE_VIRAL_ACCESSIONS`. Default: `""`. Use this for upstream filters that bound the set of enumerated assemblies (e.g. `--assembly-level complete`).
- `params.datasets_download_extra_args` [str]: Additional arguments passed to `datasets download genome accession` in the assembly branch of `DOWNLOAD_VIRAL_GENOMES`. Default: `""`. The accession list is already filtered upstream, so most filters belong on `datasets_summary_extra_args`; reserve this for download-side options. To use an NCBI API key for higher rate limits, set the `NCBI_API_KEY` environment variable before launching the pipeline.
- `params.datasets_summary_seq_extra_args` [str]: Additional arguments passed to `datasets summary virus genome taxon` in the sequence branch. Default: `"--complete-only --released-after 2024-01-01"`. The release-date floor bounds the (very large) sequence result set to the recent genomes the assembly branch misses; lower it to widen historical coverage, at the cost of volume. Only used when `virus_source` is `"sequence"` or `"both"`.
- `params.datasets_download_seq_extra_args` [str]: Additional arguments passed to `datasets download virus genome accession` in the sequence branch. Default: `""`. Only used when `virus_source` is `"sequence"` or `"both"`.
- `params.sequence_exclude_taxid` [str]: Root taxid of a clade dropped from the sequence branch (all its descendants are excluded, computed from the taxonomy `nodes.dmp`). Default: `"11308"` (Orthomyxoviridae / influenza), which stays on the assembly branch because NCBI still publishes grouped flu assemblies; including it in the sequence branch would re-add thousands of ungrouped flu segments. Only used when `virus_source` is `"sequence"` or `"both"`.
- `params.viral_accession_chunk_size` [int]: Max accessions per parallel `DOWNLOAD_VIRAL_GENOMES` task. Default: `10000`.
- `params.virus_taxid` [int]: The NCBI taxid for the Viruses taxon, used for building the virus taxonomy DB (currently 10239).
- `params.download_virus_taxid` [str]: Taxid to enumerate viral assemblies for in `ENUMERATE_VIRAL_ACCESSIONS`. Defaults to `params.virus_taxid` if empty. Override in test configs to download a smaller subset (e.g., `"2847173"` for Hepatitis D virus 1).
- `params.viral_taxids_exclude` [str]: Space-separated string of taxids to hard-exclude from the list of host-infecting viruses. Currently includes phage taxa that Virus-Host DB erroneously classifies as human-infecting.
- `params.viral_taxids_exclude_hard` [str]: Space-separated string of taxids to hard-exclude from the viral genome database entirely — a stronger exclusion than `params.viral_taxids_exclude` (which only drops taxa from the host-infecting list). Applied before `params.host_infection_overrides`, which can re-include a specific target taxid. Currently covers phage classes and viral families routinely misannotated as vertebrate-infecting (e.g. Smacoviridae, Picobirnaviridae).
- `params.host_taxa_screen`: Space-separated list of host taxon names to screen for when building the viral genome database. Should correspond to taxa included in `params.host_taxon_db`.
- `params.nucleaze_k` [int]: K-mer length used to build the Nucleaze viral-screen index (`virus-genomes-masked.nucleaze.bin`). RUN reads this value back from the index's `input/index-params.json` so the screen-time `k` always matches the index it screens against. Default: `24`.
