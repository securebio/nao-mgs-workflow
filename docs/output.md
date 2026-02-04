# Outputs

If the pipeline runs to completion, the following output files are expected. In the future, we will add more specific information about the outputs, including in-depth descriptions of the columns in the output files.

All pipeline output can be found in the `output` directory, which is broken into four subdirectories:

- `input`: Directory containing saved input information (useful for trying to reproduce someone else's results)
- `logging`: Log files containing meta-level information about the pipeline run itself.
- `intermediates`: Intermediate files produced by key stages in the run workflow, saved for nonstandard downstream analysis.
- `results`: Directory containing processed results files for standard downstream analysis.

## Run workflow

Main heading represents the folder name, and subheadings represent a description of the file's usage. If the file is not in the heading folder name, the relative path is given.

### `input/`

- `adapters.fasta`: FASTA file of adapter sequences used for adapter screening.
- `params-index.json`: JSON file giving parameters used to generate index directory (`params.ref_dir`).
- `params-run.json`: JSON file giving all the parameters passed to the pipeline.
- `samplesheet.csv`: Copy of the samplesheet file used to configure the pipeline (specified by `params.sample_sheet`).

### `logging/`

- `pyproject.toml`: Project configuration file containing the pipeline version and compatibility version constraints (copied from repository).
- `pyproject-index.toml`: Project configuration file from the index directory, containing the index's pipeline version and compatibility constraints (copied from index directory).
- `time.txt`: Start time of the run.
- `trace_<timestamp>.tsv`: Tab delimited log of all the information for each task run in the pipeline including runtime, memory usage, exit status, etc. Can be used to create an execution timeline using the the script `bin/plot-timeline-script.R` after the pipeline has finished running. More information regarding the trace file format can be found [here](https://www.nextflow.io/docs/latest/reports.html#trace-file).

### `intermediates/`

- `virus_hits_final.fastq.gz`: Filtered viral hits in FASTQ format.
- `aligner_hits_all.tsv.gz`: List of all putative viral alignments (primary, secondary and supplementary) from the aligner used in the `RUN` workflow (bowtie2 for EXTRACT_VIRAL_READS_SHORT or minimap2 for EXTRACT_VIRAL_READS_ONT) with modified columns from the [SAM specification](https://samtools.github.io/hts-specs/SAMv1.pdf).
- `lca_hits_all.tsv.gz`: List of putative viral reads after having applied LCA to `aligner_hits_all.tsv.gz`, along with columns representing summary statistics.
- `reads/raw_viral/*`: Directory containing raw reads corresponding to those reads that survive initial viral screening with BBDuk. (Note: this is not currently produced for ONT data.)

### `results/`

#### QC
- `{sample}_read_counts.tsv`: Total number of raw reads for a given sample.
- `{sample}_qc_adapter_stats_raw.tsv.gz` and `{sample}_qc_adapter_stats_cleaned.tsv.gz`: Adapter statistics calculated by FASTQC for subset sample before (`raw`) and after (`cleaned`) adapter trimming, given as a percentage of reads containing adapter content (`pc_adapters`) at each position along the read (`position`) for each adapter detected (`adapter`) for each read in the read pair (`read_pair`).
- `{sample}_qc_basic_stats_raw.tsv.gz` and `{sample}_qc_basic_stats_cleaned.tsv.gz`: Summary statistics for each subset sample before (`raw`) and after (`cleaned`) adapter trimming, including:
    - GC content (`percent GC`);
    - Average read length (`mean_seq_len`);
    - Number of read pairs (`n_read pairs`);
    - Approximate number of base pairs in reads (`n_bases_approx`);
    - Percent duplicates as measured by FASTQC (`percent_duplicates`);
    - Pass/fail scores for each test conducted by FASTQC.
- `{sample}_qc_length_stats_raw.tsv.gz` and `{sample}_qc_length_stats_cleaned.tsv.gz`: Per-read length statistics calculated by FASTQC for subset sample before (`raw`) and after (`cleaned`) adapter trimming, given as the number of reads (`n_sequences`) with a given read length (`read_length`) for each read in the read pair (`read_pair`).
- `{sample}_qc_quality_base_stats_raw.tsv.gz` and `{sample}_qc_quality_base_stats_cleaned.tsv.gz`: Per-base read-quality statistics calculated by FASTQC for subset sample before (`raw`) and after (`cleaned`) adapter trimming, given as the mean Phred score (`mean_phred_score`) at each position along the read (`position`) for each read in the read pair (`read_pair`).
- `{sample}_qc_quality_sequence_stats_raw.tsv.gz` and `{sample}_qc_quality_sequence_stats_cleaned.tsv.gz`: Per-sequence read-quality statistics calculated by FASTQC for subset sample before (`raw`) and after (`cleaned`) adapter trimming, given as the number of reads (`n_sequences`) with a given mean Phred score (`mean_phred_score`) for each read in the read pair (`read_pair`).

#### Viral identification
- `virus_hits_final.tsv.gz`: TSV output from EXTRACT_VIRAL_READS, giving information about each read pair assigned to a host-infecting virus, using the LCA taxid assignment as the source of truth. Contains both LCA-based taxonomic assignments (columns with `aligner_` prefix) that utilize multiple alignments per read, and read sequence information plus primary alignment details (columns with `prim_align_` prefix) for the DOWNSTREAM workflow. See [virus_hits_final.md](./virus_hits_final.md) for documentation of column names.

#### Taxonomic identification
- `bracken_reports_merged.tsv.gz`: Bracken output reports in TSV format, labeled by sample and ribosomal status, for subset samples produced by SUBSET_TRIM.
- `kraken_reports_merged.tsv.gz`: Kraken output reports in TSV format, labeled by sample and ribosomal status, for subset samples produced by SUBSET_TRIM.

#### BLAST
- `merged_blast_filtered.tsv.gz`: Filtered tabular BLASTN output for putative vertebrate viral reads.
- `merged_blast_input_subset.fasta.gz`: Subset interleaved FASTA used as input to BLASTN (useful for identifying which reads were included in the subset for downstream analysis).

## Index workflow

Main heading represents the folder name, and subheadings describes the tool that consumes the file. Files that are consumed by multiple tools or are not consumed by any tools are put in the `General` subheading. If the file is not in the heading folder name, the relative path is given.

### `input/`

- `index-params.json`: JSON file giving all the parameters passed to the pipeline (useful for trying to reproduce someone else's results).

### `logging/`

- `pyproject.toml`: Project configuration file containing the pipeline version and compatibility version constraints (copied from repository).
- `time.txt`: Start time of index workflow run.
- `trace.txt`: Nextflow trace file containing logging information for each process performed during the workflow run.

### `results/`

#### General

- `total-virus-db-annotated.tsv.gz`: Database generated from NCBI taxonomy and Virus-Host-DB giving taxonomy and host-infection information for each viral taxon.
- `taxonomy-nodes.dmp`: Taxonomy dump file from NCBI mapping between taxids and their parents in the NCBI taxonomy tree structure.
- `taxonomy-names.dmp`: Taxonomy dump file from NCBI mapping between taxids and taxon names.

#### BLAST

- `core_nt`: Directory containing extracted BLAST database files for BLASTing against core_nt.

#### Bowtie2

- `bt2-virus-index`: Directory containing Bowtie2 index for host-infecting viral genomes.
- `bt2-human-index`: Directory containing Bowtie2 index for the human genome.
- `bt2-other-index`: Directory containing Bowtie2 index for other contaminant sequences.
- `virus-genome-metadata-gid.tsv.gz`: Genome metadata file generated during download of vertebrate viral genomes[^vertebrate] from viral Genbank, annotated additionally with Genome IDs used by Bowtie2 (allowing mapping between genome ID and taxid).

[^vertebrate]: We say "vertebrate-infecting viruses" here and throughout the documentation for convenience, as the pipeline currently looks for vertebrate-infecting viruses by default. However, which viruses the pipeline looks for is configurable based on how you set up the index workflow.

#### Minimap2

- `mm2-virus-index`: Directory containing minimap2 index for host-infecting viral genomes.
- `mm2-human-index`: Directory containing minimap2 index for the human genome.
- `mm2-other-index`: Directory containing minimap2 index for other contaminant sequences.
- `mm2-ribo-index`: Directory containing minimap2 index for ribosomal reference sequences.

#### Kraken2

- `kraken_db`: Directory containing Kraken2 reference database (default: Most recent version of Standard).

#### BBduk

- `virus-genomes-masked.fasta.gz`: FASTA file containing host-infecting viral genomes downloaded from viral Genbank (filtered to remove transgenic, contaminated, or erroneous sequences).
- `ribo-ref-concat.fasta.gz`: Reference database of ribosomal LSU and SSU sequences from SILVA.
