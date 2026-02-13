# RUN WORKFLOW

This page describes the structure and function of the `run` workflow, which is responsible for the primary analysis of the pipeline. For usage instructions, see [here](./usage.md).

## Workflow structure

```mermaid
---
title: RUN WORKFLOW
config:
  layout: horizontal
---
flowchart LR
A(Raw reads) --> B[LOAD_SAMPLESHEET]
B --> C[COUNT_TOTAL_READS] & E[SUBSET_TRIM]
B --> |Short reads|D1[EXTRACT_VIRAL_READS_SHORT]
B --> |ONT reads|D2[EXTRACT_VIRAL_READS_ONT]
E --> I(Subset reads)
E --> J(Subset trimmed reads)
J --> G[RUN_QC] & H[PROFILE]
I --> G[RUN_QC]
%% Adjust layout by placing subgraphs in specific order
subgraph "Viral identification"
D1
D2
end
subgraph "Taxonomic Profile"
H
end
subgraph "QC"
C
G
end
```

At a high level, the `run` workflow carries out two main analyses on raw input reads: sensitive and specific identification of vertebrate-infecting viral reads[^vertebrate], and high-level taxonomic profiling.

[^vertebrate]: We say "vertebrate-infecting viruses" here and throughout the documentation for convenience, as the pipeline currently looks for vertebrate-infecting viruses by default. However, which viruses the pipeline looks for is configurable based on how you set up the index workflow.

To perform these functions, the workflow runs a series of subworkflows responsible for different tasks:

1. [Setup subworkflows](#setup-subworkflows): Prepare the input data for analysis
    - [Load data into channels (LOAD_SAMPLESHEET)](#load-data-into-channels-load_samplesheet)
    - [Subset and trim reads (SUBSET_TRIM)](#subset-and-trim-reads-subset_trim)
2. [Helper subworkflows](#helper-subworkflows): Helper subworkflows that are used by other subworkflows in this list
    - [Process Lca Aligner Output (PROCESS_LCA_ALIGNER_OUTPUT)](#process-lca-aligner-output-process_lca_aligner_output)
    - [Taxonomic assignment (TAXONOMY)](#taxonomic-assignment-taxonomy)
    - [QC (QC)](#qc-qc)
3. [Analysis subworkflows](#analysis-subworkflows): Perform the primary analysis
    - An EXTRACT_VIRAL_READS subworkflow that depends on the read type
        - [Viral identification for Illumina reads and other short reads (EXTRACT_VIRAL_READS_SHORT)](#viral-identification-short-read-version)
        - [Viral identification for ONT reads and other long reads (EXTRACT_VIRAL_READS_ONT)](#viral-identification-ont-version)
    - [Taxonomic profiling (PROFILE)](#taxonomic-profiling-profile)
4. [QC subworkflows](#qc-subworkflows): Conduct quality control on the analysis results
    - [Count total reads (COUNT_TOTAL_READS)](#count-total-reads-count_total_reads)
    - [QC and output (RUN_QC)](#qc-and-output-run_qc)

## Setup subworkflows

### Load data into channels (LOAD_SAMPLESHEET)
This subworkflow loads the samplesheet and creates a channel containing the samplesheet data, in the structure expected by the pipeline. It also derives the endedness for the pipeline run (single- vs paired-end) from the structure of the samplesheet, and checks that the specified sequencing platform is (a) valid and (b) compatible with the specified endedness. (No diagram is provided for this subworkflow.)

### Subset and trim reads (SUBSET_TRIM)
This subworkflow uses [Seqtk](https://github.com/lh3/seqtk) to randomly subsample the input reads to a target number[^target] (default 1 million read pairs per sample) to save time and compute on downstream steps while still providing a reliable statistical picture of the overall sample. Following downsampling, read pairs are combined into a single interleaved file, which then undergoes adapter trimming and quality screening with [FASTP](https://github.com/OpenGene/fastp).

[^target]: More precisely, the subworkflow uses the total read count and target read number to calculate a fraction *p* of the input reads that should be retained, then keeps each read from the input data with probability *p*. Since each read is kept or discarded independently of the others, the final read count will not exactly match the target number; however, it will be very close for sufficiently large input files.

```mermaid
---
title: SUBSET_TRIM
config:
  layout: horizontal
---
flowchart LR
A(Raw paired reads) --> B[Interleave reads]
B --> C[Subset with Seqtk]
C --> D[Trim with FASTP]
C --> E(Subset reads)
D --> F(Subset trimmed reads)
style A fill:#fff,stroke:#000
style E fill:#000,color:#fff,stroke:#000
style F fill:#000,color:#fff,stroke:#000
```

## Helper subworkflows

### Process LCA Aligner Output (PROCESS_LCA_ALIGNER_OUTPUT)

This subworkflow joins the output of LCA with the processed aligner output to create the final viral hits table.

```mermaid
---
title: PROCESS_LCA_ALIGNER_OUTPUT
config:
  layout: horizontal
---
flowchart LR
A(LCA output) --> C(Sort and join)
B(Processed aligner output) --> C
C --> D(Filter rows to primary alignments, then subset and rename columns)
D --> E(Viral hits table)
D --> F(LCA output table)
D --> G(Processed aligner output table)

style A fill:#fff,stroke:#000
style B fill:#fff,stroke:#000
style E fill:#000,color:#fff,stroke:#000
style F fill:#000,color:#fff,stroke:#000
style G fill:#000,color:#fff,stroke:#000
```

1. The LCA output is sorted so that it can be joined by the `seq_id` column with processed aligner output (which is already sorted).
2. We then filter the joined table to only keep the primary alignment information for each read as this is necessary for the `DOWNSTREAM` workflow.
3. After that, we subset the columns and add a prefix to the primary alignment columns to make interpreting the output of the final viral hits table easier.
4. Finally, the viral hits table, LCA output and processed aligner output go from separate files per sample, to one big file for each output. The viral hits table is emitted by the `RUN` workflow as a main output, whereas the LCA output and processed aligner output are emitted in the intermediates folder.

To learn more about the LCA output and processed aligner output, see `docs/lca_intermediates.md`.

### Taxonomic assignment (TAXONOMY)

This subworkflow performs taxonomic assignment on a set of reads using [Kraken2](https://ccb.jhu.edu/software/kraken2/). It is called by the [PROFILE](#taxonomic-profiling-profile) subworkflow.

```mermaid
---
title: TAXONOMY
config:
  layout: horizontal
---
flowchart LR
A(Interleaved reads) --> B[BBMERGE]
B --> C(Merging summary output)
B --> D[KRAKEN]
D --> E[Process Kraken output]
E --> F(Kraken output)
D --> G[BRACKEN]
G --> H[Process Bracken output]
H --> I(Bracken output)
subgraph "Merging read pairs"
B
end
subgraph "Taxonomic profiling"
D
G
end
subgraph "Processing output"
E
H
end
style A fill:#fff,stroke:#000
style C fill:#000,color:#fff,stroke:#000
style F fill:#000,color:#fff,stroke:#000
style I fill:#000,color:#fff,stroke:#000
```

1. Input read pairs are processed with [BBMerge](https://jgi.doe.gov/data-and-tools/software-tools/bbtools/bb-tools-user-guide/bbmerge-guide/)[^merge] to create single sequences. Any pairs that cannot be merged are joined end-to-end with an "N" base between them.
2. Reads are taxonomically classified using [Kraken2](https://ccb.jhu.edu/software/kraken2/) with the reference database from the index workflow. 
3. [Bracken](https://ccb.jhu.edu/software/bracken/) then processes these results to generate detailed taxonomic abundance estimates.
4. Finally, the outputs from Kraken2 and Bracken are processed and combined into well-formatted TSVs for downstream processing.

[^merge]: Which aligns and merges read pairs with significant overlap.

### QC (QC)
This phase conducts quality control on any set of reads. It is called by [RUN_QC](#qc-and-output-run_qc) twice, once for the subset reads and once for the subset trimmed reads.

```mermaid
---
title: QC
config:
  layout: horizontal
---
flowchart LR
A(Interleaved reads) --> B[FASTQ]
B --> C[MULTIQC]
C --> D[SUMMARIZE_MULTIQC]
D --> F(QC basic stats) & G(QC adapter stats) & H(QC quality base stats) & I(QC quality sequence stats) & J(QC length stats)
style A fill:#fff,stroke:#000
style F fill:#000,color:#fff,stroke:#000
style G fill:#000,color:#fff,stroke:#000
style H fill:#000,color:#fff,stroke:#000
style I fill:#000,color:#fff,stroke:#000
style J fill:#000,color:#fff,stroke:#000
```

1. Input reads are run through [FASTQC](https://www.bioinformatics.babraham.ac.uk/projects/fastqc/) to generate quality-control metrics.
2. The output of FASTQC is then run through [MultiQC](https://multiqc.info/) to extract QC information into a more usable form.
3. The output of MultiQC is then processed and summarized into a series of summary TSV files.

## Analysis subworkflows

### Viral identification (short read version)
#### EXTRACT_VIRAL_READS_SHORT

The goal of this subworkflow is to sensitively, specifically, and efficiently identify reads arising from host-infecting viruses. It takes as input paired-end short-reads and uses a multi-step process designed to keep computational costs low while minimizing false-positive and false-negative errors.

```mermaid
---
title: EXTRACT_VIRAL_READS_SHORT
config:
  layout: horizontal
---
flowchart LR
A(Raw reads) --> B["BBDuk <br> (viral index)"]
B --> M(BBDuk-screened raw reads)
B --> C[FASTP]
C --> E["Bowtie2 <br> (viral index)"]
E --> F["Bowtie2 <br> (human index)"]
F --> G["Bowtie2 <br> (other contaminants index)"]
G --> H[Filter by alignment score and read subset]
E --> H
H --> I[LCA]
H --> J
I --> J[PROCESS_LCA_ALIGNER_OUTPUT]
J --> K(Viral hits table)

subgraph "Filter for viral reads"
B
E
end
subgraph "Trim and clean reads"
C
end
subgraph "Remove contaminants"
F
G
end
style A fill:#fff,stroke:#000
style K fill:#000,color:#fff,stroke:#000
```
1. To begin with, the raw reads are screened against a database of vertebrate-infecting viral genomes generated from Genbank by the index workflow. This initial screen is performed using [BBDuk](https://jgi.doe.gov/data-and-tools/software-tools/bbtools/bb-tools-user-guide/bbduk-guide/), which flags any read that contains at least one 24-mer matching any vertebrate-infecting viral genome. The purpose of this initial screen is to rapidly and sensitively identify putative vertebrate-infecting viral reads while discarding the vast majority of non-viral reads, reducing the cost associated with the rest of this phase.
2. Surviving reads undergo adapter and quality trimming with [FASTP](https://github.com/OpenGene/fastp) to remove adapter contamination and low-quality/low-complexity reads.
3. Next, reads are aligned to the previously-mentioned database of vertebrate-infecting viral genomes with [Bowtie2](https://bowtie-bio.sourceforge.net/bowtie2/index.shtml) using quite permissive parameters that allow multiple alignments to be returned and are designed to capture as many putative vertebrate viral reads as possible. The output files are processed to generate new read files containing any read pair for which at least one read matches the vertebrate viral database.
4. The output of the previous step is passed to a further filtering step, in which reads matching a series of common contaminant sequences are removed. This is done by aligning surviving reads to these contaminants using Bowtie2 in series. Contaminants to be screened against include reference genomes from human, cow, pig, carp, mouse and *E. coli*, as well as various genetic engineering vectors.
5. The reads that survive contaminant filtering then go through an alignment score filtering step to get rid of low quality alignments. 
6. The reads that make it through the score filter are then run through our [custom LCA algorithm](./lca.md). The LCA taxid assignment is what we use to classify reads in the final viral hits table.
7. The LCA and processed bowtie2 output are run through [PROCESS_LCA_ALIGNER_OUTPUT](#process-lca-aligner-output-processlcaaligneroutput) to organize and clean the output viral hits table.

### Viral identification (ONT version)
#### EXTRACT_VIRAL_READS_ONT

The goal of this subworkflow is to sensitively, specifically, and efficiently identify reads arising from host-infecting viruses. It takes as input ONT (oxford nanopore) reads. Due to the smaller size of ONT datasets compared to most short-read datasets, EXTRACT_VIRAL_READS_ONT currently uses a simpler workflow than EXTRACT_VIRAL_READS_SHORT, and is less optimized for low computational costs.

```mermaid
---
title: EXTRACT_VIRAL_READS_ONT
config:
  layout: horizontal
---
flowchart LR
A(Raw reads) --> B[FILTLONG]
B --> C["BBMask <br> (entropy masking)"]
C --> D["Minimap2 <br> (human index)"]
D --> E["Minimap2 <br> (other contaminants index)"]
E --> F["Minimap2 <br> (viral index)"]
F --> G[LCA]
F --> H
G --> H[PROCESS_LCA_ALIGNER_OUTPUT]
H --> I(Viral hits table)

subgraph "Filter and mask"
B
C
end
subgraph "Remove contaminants"
D
E
end
subgraph "Identify viral reads"
F
end
style A fill:#fff,stroke:#000
style I fill:#000,color:#fff,stroke:#000
```

1. First, reads are filtered for length and quality with [Filtlong](https://github.com/rrwick/Filtlong), and low-complexity regions are masked with [BBMask](https://archive.jgi.doe.gov/data-and-tools/software-tools/bbtools/bb-tools-user-guide/bbmask-guide/) (entropy masking).
2. Next, common contaminant sequences are removed, by aligning reads to contaminants with [Minimap2](https://github.com/lh3/minimap2) in a series. Contaminants to be screened against include reference genomes from human, cow, pig, carp, mouse and *E. coli*, as well as various genetic engineering vectors.
    - Note that, unlike for EXTRACT_VIRAL_READS_SHORT, contaminant removal is done before viral read identification. EXTRACT_VIRAL_READS_ONT is frequently used on swab samples (not just on wastewater samples); we avoid analyzing human reads from swab samples for privacy/compliance reasons, so we wish to discard human reads as early in the workflow as possible.
3. Then, reads are aligned to our database of vertebrate-infecting viral genomes using Minimap2 while allowing multiple alignments to be returned. (As noted above, the viral database is generated from Genbank by the index workflow.)
4. After that, these reads are run through our [custom LCA algorithm](./lca.md). The LCA taxid assignment is what we use to classify reads in the final viral hits table.
  - Note that, unlike EXTRACT_VIRAL_READS_SHORT, we don't do any alignment score filtering as our experiments revealed that misclassified reads couldn't be distinguished by setting a simple score threshold.
5. Finally, the LCA and processed minimap2 output are run through [PROCESS_LCA_ALIGNER_OUTPUT](#process-lca-aligner-output-processlcaaligneroutput) to organize and clean the output viral hits table.

### Taxonomic profiling (PROFILE)

The goal of this subworkflow is to give an overview of the taxonomic composition of the cleaned and subset reads from the preprocessing phase. In particular, it gives an estimate of (a) the fraction of ribosomal reads in the dataset, (b) the taxonomic breakdown of the dataset at the domain level[^eukarya], and (c) more detailed abundance estimates for lower-level taxa.

```mermaid
---
title: PROFILE
config:
  layout: horizontal
---
flowchart LR
A("Subset trimmed reads <br> (SUBSET_TRIM)") --> B["BBDuk <br> (SILVA index)"]
B --> |Ribosomal reads| C[TAXONOMY]
B --> |Non-ribosomal reads| D[TAXONOMY]
C --> E[Combine output]
D --> E
E --> F(Combined Kraken reports)
E --> G(Combined Bracken reports)
subgraph "Ribosomal classification"
B
end
style A fill:#fff,stroke:#000
style F fill:#000,color:#fff,stroke:#000
style G fill:#000,color:#fff,stroke:#000
```

To do this, reads from SUBSET_CLEAN are separated into ribosomal and non-ribosomal read groups using BBDuk, by searching for ribosomal k-mers from the SILVA database generated by the index workflow. The ribosomal and non-ribosomal reads are then passed separately to [TAXONOMY workflow](#taxonomic-assignment-taxonomy), which returns back the Kraken2 and Bracken outputs. These are then annotated and merged across samples to produce single output files.

[^eukarya]: As human is the only eukaryotic genome included in the Standard reference database for Kraken2, all sequences assigned to that domain can be assigned to *Homo sapiens*.

## QC workflows

### Count total reads (COUNT_TOTAL_READS)
This subworkflow counts the total number of reads in the input files, then merges read counts from all samples into one output TSV. (No diagram is provided for this subworkflow.)

### QC and output phase (RUN_QC)
This subworkflow calls [QC](#qc-qc) on the raw and cleaned read subsets output by SUBSET_TRIM, then concatenates the outputs across the two pipeline stages.

```mermaid
---
title: RUN_QC
config:
  layout: horizontal
---
flowchart LR
A[Subset reads] --> B[QC]
C(Subset trimmed reads) --> D[QC]
B & D --> E[Process & merge output]
E --> F(Combined QC basic stats)
E --> G(Combined QC adapter stats)
E --> H(Combined QC quality base stats)
E --> I(Combined QC quality sequence stats)
E --> J(Combined QC length stats)
style A fill:#fff,stroke:#000
style C fill:#fff,stroke:#000
style F fill:#000,color:#fff,stroke:#000
style G fill:#000,color:#fff,stroke:#000
style H fill:#000,color:#fff,stroke:#000
style I fill:#000,color:#fff,stroke:#000
style J fill:#000,color:#fff,stroke:#000
```
