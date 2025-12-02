# v3.0.1.6-dev

- Modified filterTsvColumnByValue to correctly handle quotation characters in FASTQ quality strings.

# v3.0.1.5
- Fixed `SPLIT_VIRAL_TSV_BY_SELECTED_TAXID` failing when input TSV has no data rows. Empty partition files (`partition_empty_*`) are now filtered out before downstream processing. (#524)
- Converted many nf-test tests to Pytest to ease pre-release review.

# v3.0.1.4
- Removed `nextflow.preview.output` statement from `main.nf` for compatibility with Nextflow 25.10
- Updated Github Actions to use Nextflow 25.10.0.
- Updated container and dependency management
    - Implemented container dependency scanning with Trivy (`bin/scan_containers.py`) and wrote corresponding Github Actions test (currently expected to fail).
    - Implemented programmatic generation of Wave containers from YAML configuration files (`bin/build_wave_container.py` and `bin/build_wave_containers.py`) and replaced Docker Hub containers with generated Wave Containers.
    - Deleted obsolete container specifications in `docker` directory.
    - Updated non-results-affecting software versions to reduce vulnerabilities.
- Enabled ONT support for `DOWNSTREAM` post-hoc validation (`VALIDATE_VIRAL_ASSIGNMENTS`).
- Fixed broken documentation links in `docs/run.md`, `docs/virus_hits_final.md`, and `docs/README.md` that were using incorrect relative paths (e.g., `docs/lca.md` instead of `./lca.md`), causing 404 errors on GitHub (#506)
- Fixed typo in `docs/run.md` ("time and and compute" → "time and compute") (#506)
- Updated maintainer references in `docs/developer.md` to reflect current team members (#506)

# v3.0.1.3
- Updated `docs/batch.md` with a deprecation warning linking to private internal infrastructure for SecureBio users.
- Refactored `VALIDATE_VIRAL_ASSIGNMENT` in `DOWNSTREAM` to iterate through clusters within groups, reducing channel elements from N_groups × N_clusters to N_groups (#477)
  - Updated modules to run loops internally rather than having Nextflow iterate through individual elements
  - Created new processes with `_LIST` suffix for backwards compatibility, with tests in `main_list.nf.test` files
  - Removed `CONCATENATE_FILES_ACROSS_SELECTED_TAXID` and `CONCATENATE_TSVS_ACROSS_SELECTED_TAXID` subworkflows which were reduced to single modules by this refactoring

# v3.0.1.2
- `INDEX` workflow now uses daily releases of Virus-Host DB and NCBI taxonomy database, and the Kraken DB was updated to the latest version. (#445)
- Updated virus exclusion list (`ref/hv_patterns_exclude.txt`) used by `INDEX` workflow to exclude additional viruses that were causing false positives. (#445)
- Fixed `FILTER_VIRAL_SAM` handling of multiple secondary alignments for CP/DP pairs with identical grouping keys (reference genome, mate reference genome, position range, template length, alignment scores) - now selects first forward/reverse alignment pair. (#447)
- Fixed `VALIDATE_GROUPING` to include header in the output file that indicates samples with no viral hits. (#449)
- Added Python development dependencies to `pyproject.toml` for easier testing and linting setup. (#451)
- Updated developer docs with Python environment setup instructions using uv. (#451)
- Updated `bin/test_component_dependencies.py` to test subcomponents of a component by default. Only applicable if the component is a workflow/subworkflow. (#291)

# v3.0.1.1
- Added bugfix for `VALIDATE_GROUPING` which allows viral hits tables to have samples that are not found in the groupings file. This previously raised an error, causing `DOWNSTREAM` to not run.
- Updated developer docs (`docs/developer.md`) to reflect new norms and best practices:
    - Updated our branch naming convention.
    - Updated our PR process.
    - Updated our release process.
    - Added preference for using pytest over nf-test for Python unit tests.
- Added `pyproject.toml` to the top level directory to standardize our Python file formatting and type checking rules.

# v3.0.1.0

### Key changes (impacting most users)
- Improved database handling: added caching of large reference files to reduce AWS Batch loading times.
    - Processes on the same compute node can now share Kraken2 and BLAST databases, as well as Minimap2 and Bowtie2 indexes.
    - This reduces pipeline runtime and cost, particularly for workloads with many small `.fastq` files.
    - Core logic implemented in `bin/download-db.sh`.
- Testing changes: Running `nf-test` tests now requires exporting AWS credentials (see `docs/developer.md`).
- Stability improvements:
    - Fixed frequent out-of-memory errors in PROCESS_VIRAL_MINIMAP2_SAM by adjusting resource requirements.
    - Fixed issue where the DOWNSTREAM workflow failed on samples with no viral hits.
    - Fixed bug in ANNOTATE_VIRUS_INFECTION that incorrectly assigned certain viruses to specific hosts (e.g. porcine respiratory coronavirus mislabeled as human-infecting; resolves issue #311).

### Other changes (relevant mainly to developers)
- Bug fixes:
    - RAISE_TAXONOMY_RANKS: Adjusted for updated classification of "Viruses" taxon in NCBI taxonomy database.
    - FILTER_VIRAL_SAM: Now correctly handles concordant pairs with identical positions but differing alignment scores.
    - VALIDATE_GROUPING: Fixed output file name collisions.
    - nf-test file FILTER_VIRAL_SAM: Fixed invalid test inputs.
    - nf-test files for LOAD_SAMPLESHEET and LOAD_DOWNSTREAM_DATE: Fixed line iteration bug.
- Container updates:
    - Introduced new custom containers to support reference file caching (new Dockerfiles in `docker` directory)
    - Added `bin/build-push-docker.sh` to build and push Docker images to Dockerhub.
- Code quality:
    - Converted modules and subworkflows with >5 positional arguments to use parameter maps (reduces risk of argument errors).
    - Added more unit tests in the pytest file for ANNOTATE_VIRUS_INFECTION.
    - Updated Github Actions to retry downloading `nf-test` (it often fails on the first attempt due transient 403 errors).

# v3.0.0.1
- Added bugfix for `RAISE_TAXONOMY_RANKS` to account for change in classification of "Viruses" taxon in NCBI taxonomy database.

# v3.0.0.0

### Breaking changes

- Changed `RUN` workflow viral taxonomic assignment from Kraken2 + aligner ensemble to aligner-only with multiple alignments + LCA algorithm:
    - Replaces `virus_hits_all.tsv.gz` with two new intermediate files: `aligner_hits_all.tsv.gz` (all viral alignments) and `lca_hits_all.tsv.gz` (LCA-processed reads)
    - Updates `virus_hits_final.tsv.gz` columns: removes Kraken2 columns, adds `aligner_` prefix columns for LCA assignments and `prim_align_` prefix columns for primary alignment details
    - Integrates EXTRACT_VIRAL_READS_SHORT_LCA functionality directly into EXTRACT_VIRAL_READS_SHORT (similarly for ONT)
    - No changes to input files or parameters required
- Removed `trace.txt` from expected pipeline outputs (as we have changed the trace filename to include a timestamp)

### Other changes

- Added clade counting to DOWNSTREAM. Added a module COUNT_READS_PER_CLADE, which counts the number of LCA-assigned reads in each viral clade. This module:
    - creates a new clade count output file `results_downstream/{sample}_clade_counts.tsv.gz`
    - does not modify any existing output.
    - is called directly in the DOWNSTREAM workflow. If we need more modules for clade counting in the future, will create a subworkflow.
- Updated EXTRACT_VIRAL_READS_SHORT and EXTRACT_VIRAL_READS_ONT for DOWNSTREAM compatibility:
    - Adds primary/secondary/supplementary alignment status tracking to enable duplicate marking in DOWNSTREAM
    - Creates PROCESS_LCA_ALIGNER_OUTPUT subworkflow to merge alignment information with LCA output
    - Ensures `virus_hits_final.tsv.gz` contains necessary columns for downstream duplicate analysis
- Updated DOWNSTREAM to handle LCA assignments above species level for BLAST validation:
    - Previously grouped reads by species taxid for BLAST validation, but LCA can assign reads to genus/family/higher ranks
    - Now groups reads by species taxid assignment if below species level, or by LCA taxid assignment if above species level
    - Renamed processes from "_SPECIES" to "_SELECTED_TAXID" to reflect this broader taxonomic grouping
    - Also updated RUN_VALIDATION to accept the new LCA output format
- Updated SORT_FASTQ to sort alphanumerically
- Updated documentation for LCA integration:
    - Added `docs/lca.md` explaining the LCA algorithm and how it assigns taxonomic IDs to reads with multiple viral alignments
    - Added `docs/lca_intermediates.md` documenting the columns in new intermediate files (`aligner_hits_all.tsv.gz` and `lca_hits_all.tsv.gz`)
    - Updated `docs/run.md` and `docs/virus_hits_final.md` with new column descriptions and workflow changes

# v2.10.0.1
- Removed extremely long reads (>500000bp) before FASTQC on ONT data, and upped memory resources for FASTQC, to avoid out-of-memory errors.
- Made separate run_illumina.config and run_ont.config files to record correct BLAST defaults for each.

# v2.10.0.0
- Moved all outputs to main workflow for compatibility with Nextflow 25.04, and made pipeline compliant with new strict syntax.
    - Pipeline is now *incompatible* with Nextflow 24.
- Changed column names in `virus_hits_final.tsv` for consistency between Illumina and ONT output:
    - Added `docs/virus_hits_final.md` with full documentation of column names.
    - Column prefixes `bowtie2_` and `minimap2_` changed to `aligner_`.
    - Removed columns `bowtie2_fragment_length_fwd/rev`, `minimap2_query_sequence`, `minimap2_read_length`, `minimap2_ref_start/end`, `minimap2_alignment_start/end`.
    - Added boolean columns `query_rc_by_aligner` and `query_rc_by_aligner_rev` to keep track of when the aligner reverse-complements a read; updated `query_seq` to undo the reverse complement operation.
    - Changed column prefixes from `kraken_` to `kraken2_`.
- Made more processes compatible with ONT/other unpaired data:
    - `run_validation` workflow now runs on ONT/other unpaired data.
    - Updated EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED to infer endedness based on the input file and to work correctly on both unpaired and paired-end data.
    - Updated BOWTIE2 and PROCESS_VIRAL_BOWTIE2_SAM to handle unpaired input data.
- Overhauled MARK_ALIGNMENT_DUPLICATES:
    - Increased computational efficiency:
        - Added multithreaded processing of easily parallelizable steps.
        - Reworked assignment of reads to duplicate groups to avoid slow all-vs-all comparisons.
    - Made MARK_ALIGNMENT_DUPLICATES explicitly handle NAs:
        - Now if the forward reads match and the reverse read alignments are NA, reads will be marked as duplicates. This is more conservative than the previous approach, which excluded reads from duplicate groups if either alignment was NA.
- Completed work on post-hoc validation and integrated into DOWNSTREAM workflow:
    - Updated VALIDATE_VIRAL_ASSIGNMENTS to concatenate across species before rather than after BLAST_VIRAL, dramatically reducing per-process fixed costs of running BLAST. (Involved updates to PROPAGATE_VALIDATION_INFORMATION as well as new CONCATENATE_FASTA_ACROSS_SPECIES subworkflow and CONCATENATE_FASTN_LABELED process.)
    - Updated COMPUTE_TAXID_DISTANCE to compute distance from each taxid to their LCA rather than a single relative distance.
    - Modified COMPUTE_TAXID_DISTANCE and VALIDATE_CLUSTER_REPRESENTATIVES to use parameter maps.
    - Added VALIDATE_VIRAL_ASSIGNMENTS to the DOWNSTREAM workflow and wrote associated tests.
- Preparatory work for implementing LCA (lowest common ancestor) analysis:
    - Added FILTER_VIRAL_SAM process for consolidated preprocessing of viral alignments before converting to a TSV to run LCA on.
    - Created new temporary workflow EXTRACT_VIRAL_READS_SHORT_LCA, that will eventually replace EXTRACT_VIRAL_READS_SHORT. In this workflow:
        - Changed Bowtie2 to run with multiple alignments.
        - Conducted contaminant and score filtering of Bowtie2 reads before running LCA.
        - Removed the TAXONOMY subworkflow (effectively removing our usage of Kraken2 in identifying viral reads).
        - Updated EXTRACT_VIRAL_READS_SHORT_LCA such that the output viral hits table is compatible with the DOWNSTREAM workflow
- Other updates:
    - Added developer documentation (docs/developer.md).
    - Switched to a defined release from [VirusHostDB](https://www.genome.jp/virushostdb), as the previous link (https://www.genome.jp/virushostdb/virushostdb.tsv) is currently broken.
    - Made trace files generated by Nextflow for RUN and DOWNSTREAM unique across runs by adding timestamps to the filenames (prevents overwriting when running multiple attempts in the same directory).

# v2.9.0.4
- Updated markAlignmentDuplicates module to reduce memory overhead and increase memory allocation (which collectively should avoid out-of-memory errors in DOWNSTREAM on large read groups).

# v2.9.0.3
- Make sure field per_tile_sequence_quality is always present in multiqc output summary file, to allow pipeline to run successfully on a mix of empty and non-empty files
- Add set -eou pipefail to all ONT processes with pipes; make MASK_FASTQ_READS robust to empty files; add empty file tests for MASK_FASTQ_READS and MINIMAP2

# v2.9.0.2
- Continued working on post-hoc validation of putative viral hits in the DOWNSTREAM workflow
    - Implemented VALIDATE_CLUSTER_REPRESENTATIVES subworkflow for comparing Bowtie2 and BLAST-LCA assignments, including new SELECT_TSV_COLUMNS and COMPUTE_TAXID_DISTANCE processes
    - Implemented PROPAGATE_VALIDATION_INFORMATION subworkflow to merge cluster-representative validation information back into raw hits TSV
    - Implemented CHECK_TSV_DUPLICATES process and added to SPLIT_VIRAL_TSV_BY_SPECIES to prevent many-to-many joins during post-hoc validation
    - Implemented CONCATENATE_TSVS_ACROSS_SPECIES subworkflow for reconstructing grouped viral hits TSV from species-specific TSVs
- Modified SORT_TSV behavior to avoid out-of-memory errors.
- Updated trace path for DOWNSTREAM workflow to avoid overwriting RUN workflow trace.

# v2.9.0.1
- Modified Github Actions to pull specific Nextflow version (rather than "latest")
- Fixed missing-columns bug for empty files in SUMMARIZE_MULTIQC
- Restructured SORT_TSV process to improve memory efficiency
- Continued working on post-hoc validation of putative viral hits in the DOWNSTREAM workflow
    - Split out core of BLAST_VIRAL subworkflow into a new BLAST_FASTA subworkflow that is called by both BLAST_VIRAL and VALIDATE_VIRAL_ASSIGNMENTS
    - Added tests for BLAST_FASTA and updated tests for VALIDATE_VIRAL_ASSIGNMENTS
    - Implemented basic algorithm for computing the lowest common ancestor of sets of taxids in tabular TSV data (LCA_TSV), including special handling of artificial and unclassified taxids
    - Integrated LCA_TSV into BLAST_FASTA subworkflow and updated tests

# v2.9.0.0
- Implemented ONT analysis in the RUN workflow
    - Combined run_dev_se.nf with run.nf
    - Renamed `hits_filtered` outputs of short-read workflow and `hits_hv` outputs of ONT workflow to `hits_final` for consistency across platforms
    - Also renamed `hits_all` output of short-read pipeline to `hits_unfiltered`
    - Added end-to-end tests for ONT to github actions
- Prepared DOWNSTREAM workflow for running with internal mgs-orchestrator repo
    - Added `expected-outputs-downstream.txt` file containing list of expected output files for the DOWNSTREAM workflow
    - Modified output paths for non-results DOWNSTREAM outputs to avoid overwriting RUN outputs
    - Changed strict-join of hits and grouping TSVs across sample names to inner-join (to drop samples that are not present in both TSVs)
- Began development of post-hoc validation of putative viral hits in the DOWNSTREAM workflow
    - Split viral hits TSV by assigned species and extract read sequences (SPLIT_VIRAL_TSV_BY_SPECIES)
    - Cluster within species with VSEARCH and obtain representative sequences (CLUSTER_VIRAL_ASSIGNMENTS)
    - Split out merge/join part of TAXONOMY workflow into its own subworkflow (MERGE_JOIN_READS) that can be used by both TAXONOMY and post-hoc validation (with associated tests)
- Added a development_mode parameter to LOAD_SAMPLESHEET to allow testing on non-implemented platform/endedness
- Get rid of lingering references to human viruses/HV in comments, variable names, etc.
- Updated SUBSET_FASTQ to handle plaintext and FASTA input (and renamed to SUBSET_FASTN)
- Modified various RUN workflow components to correctly handle empty input files (which previously caused failures).
- Added `test_component_dependencies.py` script to test all modules, subworkflows, and workflows that depend on a given component (e.g., BBDUK, or TAXONOMY)

# v2.8.3.2
- Modified FASTQ_LABELED to use fixed cpus and memory, and added `--memory` parameter to make full use of available memory.
- Added pass/fail test for FASTQC_LABELED.
- Removed unused QC processes.
- Added rank-raised taxids to viral taxonomy DB output by INDEX workflow.

# v2.8.3.1
- Added `expected-outputs-run.txt` file containing list of expected output files for the `RUN` workflow (excluding BLAST validation).
- Minor updates to logging filenames.

# v2.8.3.0
- **Lowered Bracken read threshold for taxon classification**

# v2.8.2.0
- **Increased runtime Bowtie2 score threshold for viral read identification**
- Updated Github Actions to use NAO secrets to access buckets containing test data
- Removed generate-samplesheet.py, as functionality has moved to internal mgs-metadata repo
- Added ability to set BLAST parameters `qcov_hsp_perc` and `perc_id`
- Added MINIMAP2 classification of ONT reads to PROFILE subworkflow
- Replaced boolean `params.ont` with string `params.platform` and added platform checking to LOAD_SAMPLESHEET
- Fixed bug in running RUN_VALIDATION workflow with a FASTQ file

# v2.8.1.2
- Made Cutadapt mismatch rate parameter configurable
- Fixed issues with BLAST bitscore filtering
- Increased memory allocation for EXTRACT_VIRAL_HITS_TO_FASTQ
- Implemented version compatibility checking between pipeline and index
- Added ONT virus identification support:
    - Created new EXTRACT_VIRAL_READS_ONT subworkflow for processing ONT reads
    - Renamed original EXTRACT_VIRAL_READS workflow to EXTRACT_VIRAL_READS_SHORT to differentiate from ONT processing
    - Added non-streaming version of MINIMAP2 alignment process
    - Added new modules for ONT-specific processing:
        - MASK_FASTQ_READS for masking low complexity regions in reads
        - EXTRACT_SHARED_FASTQ_READS for extracting reads shared between FASTQ files
        - PROCESS_VIRAL_MINIMAP2_SAM for adding reference taxids and clean read information
    - Edited FILTLONG to accept customizable parameters (min_length, max_length, min_mean_q)
    - Added new low-complexity fastq test file.

# v2.8.1.1
- Modified Kraken2 DB handling in index workflow to avoid staging
- Updated defaults in index configs

# v2.8.1.0
- Added downstream duplicate marking functionality via new DOWNSTREAM workflow
    - Fixed JOIN_TSVS to correctly handle many-to-one joins
    - Added strict join mode to JOIN_TSVS
    - Altered PROCESS_VIRAL_BOWTIE2_SAM to make ordering of genome IDs for split alignments predictable (necessary for downstream duplicate marking)
- Updated ANNOTATE_VIRUS_INFECTION to better handle taxa that are missing from Virus-Host DB, and added corresponding tests and documentation.
- Began implementing pipeline components for analyzing ONT data:
    - Added generation of minimap2 indices to INDEX workflow (human, viral, contaminant, and ribosomal).
    - Added LSU and SSU tags to respective small and large ribosomal subunit genomes in the composite ribosomal reference fasta.
    - Added MINIMAP2_INDEX and MINIMAP2 processes for indexing reference genomes and aligning reads to them.
- Added documentation on running the pipeline reproducibly
- Fixed some local unit tests

# v2.8.0.0
- Major changes to many parts of the pipeline as part of a general performance overhaul
    - Modified most processes in the RUN and RUN_VALIDATION workflows to stream data in and out rather than reading whole files
    - As part of the previous change, modified most processes in the RUN and RUN_VALIDATION workflows to work with interleaved rather than paired sequence data
    - Modified BLASTN filtering to take into account bitscore ratio versus best hit for each query
    - Replaced many specific tabular manipulation processes with basic operations: JOIN_TSVS, CONCATENATE_TSVS, ADD_FIXED_COLUMN, etc
    - Removed grouping and group-dependent functionality (in particular, deduplication and clade counting); entire pipeline now operates on a per-sample basis
    - Added unit tests for many processes and workflows
    - Added configurable seeding for testing non-deterministic processes via `params.random_seed`
    - Made Bracken read threshold configurable via `params.bracken_threshold`
    - Removed numerous orphaned modules and processes
- Large changes to outputs:
    - Main output directory no longer contains FASTA files for viral hits (interleaved FASTQ file now saved to intermediates)
    - Clade counts are no longer produced
    - QC and BLAST outputs now show statistics for interleaved files rather than showing forward and reverse reads separately
    - Added new intermediate outputs, including unfiltered viral hits and interleaved FASTQ from EXTRACT_VIRAL_READS
    - Viral hits TSV moved from `virus_hits_db.tsv.gz` to `virus_hits_filtered.tsv.gz`
    - Numerous changes to column names in viral hits TSV, mainly to improve clarity
- Minor changes and fixes:
    - Updated mislabeled processes
    - Fixed bug where multiqc doesn't output sequence length stats if all sequences are the same length
    - Unzipped files in `test-data` directory
    - Added new script, `bin/run_parallel_test.sh`, that allows users to run nf-test tests locally in parallel
    - Assorted updates to documentation
    - Removed some defaults from config files
    - Fixed mislabeled parameter in RUN_VALIDATION workflow

# v2.7.0.3
- Fixing link to configuration file in `README.md`

# v2.7.0.2
- Updated `pipeline-version.txt`

# v2.7.0.1
- Fixed index-related issues from v2.7.0.0:
    - Updated `EXTRACT_VIRAL_READS` to expect updated path to viral genome DB
    - Added `adapters` param to the index config file used to run our tests
    - Updated `RUN` and `RUN_VALIDATION` tests to use up-to-date test index (location: `s3://nao-testing/index/20250130`)

# v2.7.0.0
- Implemented masking of viral genome reference in index workflow with MASK_GENOME_FASTA to remove adapter, low-entropy and repeat sequences.
- Removed TRIMMOMATIC and BBMAP from EXTRACT_VIRAL_READS.
- Restructured subworkflows to take advantage of new viral genome masking:
    - Split PROFILE workflow into SUBSET_TRIM, RUN_QC, and PROFILE subworkflows
    - Moved FASTP read cleaning downstream of BBDUK_HITS (in EXTRACT_VIRAL_READS) and subsetting (in SUBSET_TRIM)
    - Moved FASTQC and MultiQC to after subsetting (in RUN_QC)
    - Removed RAW, CLEAN, and PROCESS_OUTPUT subworkflows
    - Added COUNT_TOTAL_READS subworkflow to count the total number of reads in each sample.
- Replace generate_samplesheet.sh with generate_samplesheet.py
- Fixed bug in extractUnconcReadID that would cause the pipeline to fail if it contained the string 'YT' in the read id.
- Remove `params.quality_encoding` as it was used only by TRIMMOMATIC
- Added length distribution information to QC output
- **Renamed QC output files to reflect the fact that they now only contain QC information on a subset of reads (e.g. `qc_basic_stats.tsv.gz` -> `subset_qc_basic_stats.tsv.gz`)**
- **New QC output files: `read_counts.tsv.gz`, `subset_qc_length_stats.tsv.gz`**

# v2.6.0.0
- Updated version to reflect the new versioning scheme, which is described in `docs/version_schema.md`.

# v2.5.4
- Fixed fatal bug in `configs/run_validation.config` that prevents users from running the `RUN_VALIDATION` workflow.

# v2.5.3
- Added new LOAD_SAMPLESHEET subworkflow to centralize samplesheet processing
- Updated tags to prevent inappropriate S3 auto-cleanup
- Testing infrastructure
  - Split up the tests in `End-to-end MGS workflow test` so that they can be run in parallel on Github Actions.
  - Implemented an end-to-end test that checks if the RUN workflow produces the correct output. The correct output for the test has been saved in `test-data/gold-standard-results` so that the user can diff the output of their test with the correct output to check where their pipeline might be failing.
- Began development of single-end read processing (still in progress)
    - Restructured RAW, CLEAN, QC, TAXONOMY, and PROFILE workflows to handle both single-end and paired-end reads
    - Added new FASTP_SINGLE, TRUNCATE_CONCAT_SINGLE, BBDUK_SINGLE, CONCAT_GROUP_SINGLE, SUBSET_READS_SINGLE and SUBSET_READS_SINGLE_TARGET processes to handle single-end reads
    - Created separate end-to-end test workflow for single-end processing (which will be removed once single-end processing is fully integrated)
    - Modified samplesheet handling to support both single-end and paired-end data
    - Updated generate_samplesheet.sh to handle single-end data with --single_end flag
    - Added read_type.config to handle single-end vs paired-end settings (set automatically based on samplesheet format)
    - Created run_dev_se.config and run_dev_se.nf for single-end development testing (which will be removed once single-end processing is fully integrated)
    - Added single-end samplesheet to test-data

# v2.5.2
- Changes to default read filtering:
    - Relaxed FASTP quality filtering (`--cut_mean_quality` and `--average_qual` reduced from 25 to 20).
    - Relaxed BBDUK viral filtering (switched from 3 21-mers to 1 24-mer).
- Overhauled BLAST validation functionality:
    - BLAST now runs on forward and reverse reads independently
    - BLAST output filtering no longer assumes specific filename suffixes
    - Paired BLAST output includes more information
    - RUN_VALIDATION can now directly take in FASTA files instead of a virus read DB
    - Fixed issues with publishing BLAST output under new Nextflow version
- Implemented nf-test for end-to-end testing of pipeline functionality
    - Implemented test suite in `tests/main.nf.test`
    - Reconfigured INDEX workflow to enable generation of miniature index directories for testing
    - Added Github Actions workflow in `.github/workflows/end-to-end.yml`
    - Pull requests will now fail if any of INDEX, RUN, or RUN_VALIDATION crashes when run on test data.
    - Generated first version of new, curated test dataset for testing RUN workflow. Samplesheet and config file are available in `test-data`. The previous test dataset in `test` has been removed.
- Implemented S3 auto-cleanup:
    - Added tags to published files to facilitate S3 auto-cleanup
    - Added S3 lifecycle configuration file to `ref`, along with a script in `bin` to add it to an S3 bucket
- Minor changes
    - Added logic to check if `grouping` variable in `nextflow.config` matches the input samplesheet, if it doesn't, the code throws an error.
    - Externalized resource specifications to `resources.config`, removing hardcoded CPU/memory values
    - Renamed `index-params.json` to `params-index.json` to avoid clash with Github Actions
    - Removed redundant subsetting statement from TAXONOMY workflow.
    - Added --group_across_illumina_lanes option to generate_samplesheet

# v2.5.1
- Enabled extraction of BBDuk-subset putatively-host-viral raw reads for downstream chimera detection.
- Added back viral read fields accidentally being discarded by COLLAPSE_VIRUS_READS.

# v2.5.0
- Reintroduced user-specified sample grouping and concatenation (e.g. across sequencing lanes) for deduplication in PROFILE and EXTRACT_VIRAL_READS.
- Generalised pipeline to detect viruses infecting arbitrary host taxa (not just human-infecting viruses) as specified by `ref/host-taxa.tsv` and config parameters.
- Configured index workflow to enable hard-exclusion of specific virus taxa (primarily phages) from being marked as infecting ost taxa of interest.
- Updated pipeline output code to match changes made in latest Nextflow update (24.10.0).
- Created a new script `bin/analyze-pipeline.py` to analyze pipeline structure and identify unused workflows and modules.
- Cleaned up unused workflows and modules made obsolete in this and previous updates.
- Moved module scripts from `bin` to module directories.
- Modified trace filepath to be predictable across runs.
- Removed addParams calls when importing dependencies (deprecated in latest Nextflow update).
- Switched from nt to core_nt for BLAST validation.
- Reconfigured QC subworkflow to run FASTQC and MultiQC on each pair of input files separately (fixes bug arising from allowing arbitrary filenames for forward and reverse read files).

# v2.4.0
- Created a new output directory where we put log files called `logging`.
- Added the trace file from Nextflow to the `logging` directory which can be used for understanding cpu, memory usage, and other infromation like runtime. After running the pipeline, `plot-timeline-script.R` can be used to generate a useful summary plot of the runtime for each process in the pipeline.
- Removed CONCAT_GZIPPED.
- Replaced the sample input format with something more similar to nf-core, called `samplesheet.csv`. This new input file can be generated using the script `generate_samplesheet.sh`.
- Now run deduplication on paired-ends reads using clumpify in the taxonomic workflow.
- Fragment length analysis and deduplication analysis.
  - BBtools: Extract the fragment length as well as the number of duplicates from the taxonomic workflow and add them to the `hv_hits_putative_collapsed.tsv.gz`.
  - Bowtie2: Conduct a duplication analysis on the aligned reads, then add the number of duplicates and fragment length to the `hv_hits_putative_collapsed.tsv.gz`.

# v2.3.3
- Added validation workflow for post-hoc BLAST validation of putative HV reads.

# v2.3.2
- Fixed subsetReads to run on all reads when the number of reads per sample is below the set threshold.

# v2.3.1

- Clarifications to documentation (in README and elsewhere)
- Re-added "joined" status marker to reads output by join_fastq.py

# v2.3.0
- Restructured run workflow to improve computational efficiency, especially on large datasets
    - Added preliminary BBDuk masking step to HV identification phase
    - Added read subsampling to profiling phase
    - Deleted ribodepletion and deduplication from preprocessing phase
    - Added riboseparation to profiling phase
    - Restructured profiling phase output
    - Added `addcounts` and `passes` flags to deduplication in HV identification phase
- Parallelized key bottlenecks in index workflow
- Added custom suffix specification for raw read files
- Assorted bug fixes

# v2.2.1
- Added specific container versions to `containers.config`
- Added version & time tracking to workflows
- Added index reference files (params, version) to run output
- Minor changes to default config files

# v2.2.0
- Major refactor
- Start of changelog
