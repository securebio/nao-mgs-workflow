# Index workflow

The index workflow generates static index files from reference data. These reference data and indices don't depend on the reads processed by the run workflow, so a set of indices built by the index workflow can be used by multiple instantiations of the run workflow — no need to rebuild indices for every set of reads. The index workflow should be run once per reasonable length of time, balancing two considerations: Many of these index/reference files are derived from publicly available reference genomes or other resources, and should thus be updated and re-run periodically as new versions of these become available; however, to keep results comparable across datasets analyzed with the run workflow, this should be done relatively rarely.

Key [inputs](./config.md) to the index workflow include:
- A TSV specifying names and taxonomic IDs (taxids) for host taxa for which to search for vertebrate-infecting viruses[^vertebrate].
- A URL linking to a suitable Kraken2 database for taxonomic profiling (typically the [latest release](https://benlangmead.github.io/aws-indexes/k2) of the `k2_standard` database).
- URLS for up-to-date releases of reference genomes for various common contaminant species that can confound the identification of vertebrate viral reads (currently [human](https://www.ncbi.nlm.nih.gov/datasets/genome/?taxon=9606), [cow](https://www.ncbi.nlm.nih.gov/datasets/genome/?taxon=9913), [pig](https://www.ncbi.nlm.nih.gov/datasets/genome/?taxon=9823), [carp](https://www.ncbi.nlm.nih.gov/datasets/genome/?taxon=7962)[^carp], [mouse](https://www.ncbi.nlm.nih.gov/datasets/genome/?taxon=10090), [*E. coli*](https://www.ncbi.nlm.nih.gov/datasets/genome/?taxon=562)).
- URLs to sequence databases for small and large ribosomal subunits from [SILVA](https://www.arb-silva.de/download/arb-files/).
- Up-to-date links to [VirusHostDB](https://www.genome.jp/virushostdb) and [NCBI Taxonomy](https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump_archive/).

[^vertebrate]: We say "vertebrate-infecting viruses" here and throughout the documentation for convenience, as the pipeline currently looks for vertebrate-infecting viruses by default. However, which viruses the pipeline looks for is configurable based on how you set up the index workflow. 

[^carp]: The carp genome is included as a last-ditch attempt to [capture any remaining Illumina adapter sequences](https://dgg32.medium.com/carp-in-the-soil-1168818d2191) before moving on to virus identification. I'm not especially confident this is helpful.

Given these inputs, the index workflow:
- Generates a TSV of viral taxa, incorporating taxonomic information from NCBI, and annotates each taxon with infection status for each host taxon of interest (using Virus-Host-DB).
- Makes Minimap2 and Bowtie2 indices from (1) all vertebrate-infecting viral genomes in Genbank[^genbank], (2) the human genome, (3) common non-human contaminants, plus BBMap indices for (2) and (3).
- Builds a [Nucleaze](https://github.com/jackdougle/nucleaze) k-mer index over the same set of vertebrate-infecting viral genomes. In contrast to Minimap2 and Bowtie2 indices, however, it uses BBDuk to N-mask any k-mer shared with the human genome (CHM13).
- Downloads and extracts local copies of (1) the BLAST nt database, (2) the specified Kraken2 DB, (3) the SILVA rRNA reference files.
- Makes a Minimap2 index from the joined SILVA rRNA reference files.

[^genbank]: Excluding transgenic, contaminated, or erroneous sequences, which are excluded according to a list of sequence ID patterns specified in the config file.

Run the index workflow by setting `mode = "index"` in the relevant config file. For more information, see `workflows/index.nf` and the associated subworkflows and modules.
