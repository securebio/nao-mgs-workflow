# SecureBio Detection Viral Metagenomics Pipeline

> [!IMPORTANT]
> As described in more detail in our [blog post](https://securebio.org/blog/taking-our-core-computational-pipeline-private/) on October 15, 2026, we are in the process of moving development of this pipeline to a private repository. In the future, this repository will be archived but will continue to remain public. All referenced test data and ECR images will also be preserved. Members of SecureBio can access the private repository at [`viral-identification`](https://github.com/securebio/det-viral-identification).

This Nextflow pipeline is designed to process metagenomic sequencing data, characterize overall taxonomic composition, and identify and quantify reads mapping to viruses infecting certain host taxa of interest. It was developed by [SecureBio Detection](https://securebio.org/detection/) (formerly the Nucleic Acid Observatory).

The pipeline currently consists of three workflows:

- [`INDEX`](./docs/index.md): Creates indices and reference files used by the `RUN` and `RUN_VALIDATION` workflows[^1].
- [`RUN`](./docs/run.md): Performs the main analysis, including QC, viral identification, taxonomic profiling, and optional BLAST validation.
- `RUN_VALIDATION`: Performs part of the `run` workflow dedicated to validation of taxonomic classification with BLAST[^2].
- [`DOWNSTREAM`](./docs/downstream.md): Performs downstream analysis of the results from the `run` workflow [^3].

[^1]: The `INDEX` workflow is intended to be run first, after which many instantiations of the `RUN` workflow can use the same index output files. 
[^2]: The `RUN_VALIDATION` workflow is intended to be run after the `RUN` workflow if the optional BLAST validation was not selected during the `RUN` workflow. Typically, this workflow is run on a subset of the host viral reads identified in the `RUN` workflow, to evaluate the sensitivity and specificity of the viral identification process.
[^3]: The `DOWNSTREAM` workflow is designed to handle tasks that require cross-read comparisons, including potentially across multiple runs, e.g., marking duplicate reads.

## Documentation

- **Installation and usage:**
    - [Installation instructions](docs/installation.md)
    - [AWS Batch setup](docs/batch.md)
    - [Usage instructions](docs/usage.md)
    - [Troubleshooting](docs/troubleshooting.md)
- **Workflow details:**
    - [INDEX workflow](docs/index.md)
    - [RUN workflow](docs/run.md)
    - [DOWNSTREAM workflow](docs/downstream.md)
- **Configuration and output:**
    - [Configuration files](docs/config.md)
    - [Pipeline outputs](docs/output.md)
- **Other:**
    - [Versioning](docs/versioning.md)
    - [Developer docs](docs/developer.md)
    - [Viral infection status annotation](docs/annotation.md)
    - [Reproducibility](docs/reproducibility.md)
