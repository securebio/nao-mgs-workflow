# Pipeline Usage

This page describes the process of running the pipeline's [core workflow](./run.md) on available data.

> [!IMPORTANT]
> Before following the instructions on this page, make sure you have followed the [installation and setup instructions](./installation.md), including running the [index workflow](./index.md) or otherwise having a complete and up-to-date index directory in an accessible location.

> [!IMPORTANT]
> Currently, the pipeline accepts paired short-read data (Illumina and Aviti), and Oxford Nanopore data. Note that Oxford Nanopore version has not been fully benchmarked/optimized; use at your own risk. (Single-end short-read support has some development but is not ready for general use.)

## 1. Preparing input files

To run the workflow on new data, you need:

1. Accessible **raw data** files in Gzipped FASTQ format, named appropriately.
2. A **sample sheet** file specifying the samples to be analyzed, along with paths to the forward and reverse read files for each sample.

> [!TIP]
> We recommend starting each Nextflow pipeline run in a clean launch directory, containing your sample sheet.

### 1.1. The sample sheet

The sample sheet must be an uncompressed CSV file with the following headers in the order specified:

For paired data:
- `sample` (1st column): Sample ID
- `fastq_1` (2nd column): Path to FASTQ file 1 which should be the forward read for this sample
- `fastq_2` (3rd column): Path to FASTQ file 2 which should be the reverse read for this sample

For single-end data (ONT):
- `sample` (1st column)
- `fastq` (2nd column)

If you're working with NAO data, [mgs-metadata](https://github.com/naobservatory/mgs-metadata) (private) generates these and puts them in S3 alongside the data.

### 1.2. The config file

The config file specifies default parameters and other configuration options used by Nextflow in executing the pipeline. Choose the appropriate config file for your platform: `configs/run.config` for Pacbio/Illumina/Aviti, or `configs/run_ont.config` for ONT. You can reference it directly with `-c` when running the pipeline — no need to copy or edit it, since per-run values (base directory, reference directory, queue, etc.) can be supplied as `--<param>` flags on the command line. See [Running the pipeline](#3-running-the-pipeline) below.

If you do need to customize non-default settings (e.g. `bt2_score_threshold`, `n_reads_profile`), copy the config file into your launch directory as `nextflow.config` and edit it there. See [here](./config.md) for a full description of all config parameters.

## 2. Choosing a profile

The pipeline can be run in multiple ways by modifying various configuration variables specified in `configs/profiles.config`. Currently, three profiles are implemented, all of which assume the workflow is being launched from an AWS EC2 instance:

- `batch (default)`:  **Most reliable way to run the pipeline**
  - This profile is the default and attempts to run the pipeline with AWS Batch. This is the most reliable and convenient way to run the pipeline, but requires significant additional setup (described [here](./batch.md)). Before running the pipeline using this profile, make sure you specify `--queue` on the command line or set `params.queue` in your config file to the correct Batch job queue.
  - Note that this profile uses automatic reference file caching (in the `/scratch` directory on the instance), which significantly reduces large database load times. 
      - To turn off file caching, remove the `aws.batch.volumes = ['/scratch:/scratch']` line from the relevant profile.
- `ec2_local`: **Requires the least setup, but is bottlenecked by your instance's compute, memory and storage.**
  - This profile attempts to run the whole pipeline locally on your EC2 instance, storing all files on instance-linked block storage.
  - This is simple and can be relatively fast, but requires large CPU, memory and storage allocations: at least 128GB RAM, 64 CPU cores, and 256GB local storage are recommended, though the latter in particular is highly dependent on the size of your dataset.
- `ec2_s3`: **Avoids storage issues on your EC2 instance, but is still constrained by local compute and memory.**
  - This profile runs the pipeline on your EC2 instance, but attempts to read and write files to a specified S3 directory. This avoids problems arising from insufficient local storage, but (a) is significantly slower and (b) is still constrained by local compute and memory allocations.

To run the pipeline with a specified profile, run

```
nextflow run <PATH_TO_REPO_DIR> -profile <PROFILE_NAME> -resume
```

Calling the pipeline without specifying a profile will run the `batch` profile by default. Future example commands in this README will assume you are using Batch; if you want to instead use a different profile, you'll need to modify the commands accordingly.

## 3. Running the pipeline

After creating your sample sheet and choosing a profile, navigate to a clean launch directory. You can then run the pipeline as follows:

```
nextflow run <PATH/TO/PIPELINE/DIR> \
  -c <PATH/TO/PIPELINE/DIR>/configs/run.config \
  -resume \
  --base_dir <BASE_DIR> \
  --ref_dir <REF_DIR> \
  --platform <PLATFORM> \
  --queue <BATCH_QUEUE_NAME>
```

where `<PATH/TO/PIPELINE/DIR>` specifies the path to the directory containing the pipeline files from this repository. Any `params.*` value in the config file can be overridden with `--<param>` on the command line. If you copied the config file into the launch directory as `nextflow.config`, you can omit the `-c` flag.

> [!TIP]
> If you are running the pipeline with its default profile (`batch`) you can omit the `-profile` declaration. To use a different profile, add `-profile <PROFILE_NAME>` to the command.

> [!TIP]
> It's highly recommended that you always run `nextflow run` with the `-resume` option enabled. It doesn't do any harm if you haven't run a workflow before, and getting into the habit will help you avoid much sadness when you want to resume it without rerunning all your jobs.

Once the pipeline has finished, output and logging files will be available in the `output` subdirectory of the base directory specified in the config file.

## 4. Cleaning up

> [!IMPORTANT]
> To avoid high storage costs, make sure not to skip this step.

Running nextflow pipelines will create a large number of files in the working directory. To avoid high storage costs, **it's important you clean up these files when they are no longer needed**. You can do this manually, or by running the `nextflow clean` command in the launch directory.

If you are running the pipeline using `ec2_local` or `ec2_s3` profiles, you will also want to clean up the docker images and containers created by the pipeline as these can take up a lot of space. This can be done by running `docker system prune -a` which will remove all unused docker images and containers on your system.
