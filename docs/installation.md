# Installation & setup

This page describes how to install the pipeline and configure your computing environment to run it.

> [!IMPORTANT]
> This pipeline has been developed to run on Linux environments, primarily on AWS EC2 instances. It has not been tested or debugged on MacOS or Windows machines, and is not guaranteed to work on those environments.

## 1. Install dependencies 

To run this workflow with full functionality, you need access to the following dependencies:

1. **SDKMAN!:** To install the SDKMAN! Java SDK manager, follow the installation instructions available [here](https://sdkman.io/install).
2. **Nextflow:** To install the workflow management framework, follow the installation instructions available [here](https://www.nextflow.io/docs/latest/getstarted.html), beginning by installing a recommended Java distribution through SDKMAN!. Pipeline version 2.10.0+ requires Nextflow version 25.04.0+.
2. **Docker:** To install Docker Engine for command-line use, follow the installation instructions available [here](https://docs.docker.com/engine/install/) (or [here](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-docker.html) for installation on an AWS EC2 instance).
3. **AWS CLI:** If not already installed, install the AWS CLI by following the instructions available [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
4. **Git:** To install the Git version control tool, follow the installation instructions available [here](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).
5. **nf-test**: To install nf-test, follow the install instructions available [here](https://www.nf-test.com/installation/).

## 2. Configure AWS & Docker

To run the workflow using AWS S3 for the working and output directories, you first need to [configure AWS access](https://www.nextflow.io/docs/latest/aws.html). To do this, you need to create a file at `~/.aws/config` **or** `~/.aws/credentials` specifying your access key ID and secret access key, e.g.

`~/.aws/config`:
```
[default]
region = us-east-1
output = table
tcp_keepalive = true
aws_access_key_id = <ACCESS_KEY_ID>
aws_secret_access_key = <SECRET_ACCESS_KEY>
```

`~/.aws/credentials`:
```
[default]
aws_access_key_id = <ACCESS_KEY_ID>
aws_secret_access_key = <SECRET_ACCESS_KEY>
```

Then, export the keys as environment variables before running nextflow:
```
eval "$(aws configure export-credentials --format env)"
```

Next, you need to make sure your user is configured to use Docker. To do this, create the `docker` user group and add your current user to it:

```
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker
docker run hello-world
```

## 3. Create Seqera account

Create a Seqera account to increase your API rate limits:

1. Create a free Seqera account at [cloud.seqera.io](http://cloud.seqera.io/)
2. Click on the user icon, then click the "User tokens" section
3. Click "Add Token" to create a token for your account. Name it whatever you want (e.g., "my-nextflow-token"). Copy the token and store it somewhere safe.
4. Export your token as an environment variable before running the pipeline:

```bash
export TOWER_ACCESS_TOKEN=<your-token>
```

Alternatively, you can set `tower.accessToken` in your Nextflow configuration file (we recommend adding this to `configs/profiles.config` to apply to all subsequent pipeline runs).

## 4. Configure Seqera ECR credentials

This pipeline's containers are pulled from a public AWS ECR repository. You need to configure AWS ECR registry credentials in your Seqera account to allow Wave to pull container images.

1. Log in to your Seqera Platform account
2. Navigate to your workspace credentials
3. Add AWS registry credentials following the [Seqera documentation](https://docs.seqera.io/platform-cloud/credentials/aws_registry_credentials)
4. Ensure the credentials have access to `public.ecr.aws`

## 5. Clone this repository

Clone this repo into a new directory as normal.

## 6. Run tests

If possible, we recommend validating the pipeline's basic functionality in your hands by running our test suite. To do this, you'll need sufficient resources on your machine to run our tests locally. You'll need:
- 4 CPU cores
- 14GB RAM
- At least 32GB storage space

> [!TIP]
> We recommend running the test suite on an `m5.xlarge`, to most closely match the conditions under which our CI/CD tests run with Github Actions. However, any Linux machine with sufficient resources should work.

To run the tests, clone this repository onto your machine, navigate to the repo directory, and run

```bash
nf-test test
```

## 7. Run index/reference workflow

> [!TIP]
> If someone else in your organization already uses this pipeline, it's likely they've already run the index workflow and generated an output directory. If this is the case, you can reduce costs and increase reproducibility by using theirs instead of generating your own. If you want to do this, skip this step, and edit `configs/run.config` (or `configs/run_ont.config`) such that `params.ref_dir` points to `INDEX_DIR/output`.

> [!NOTE]
> **For repository maintainers:** The recommended way to rebuild the benchmark index is via the "Rebuild benchmark index" workflow in GitHub Actions (`rebuild-benchmark-index.yml`). This runs INDEX nf-tests as a preflight check, builds the index to the test bucket, and verifies freshness automatically. See the [CI documentation](./ci.md#benchmark-index-management) for details.

Create a new directory outside the repo directory and copy over the index workflow config file as `nextflow.config` in that directory:

```
mkdir index
cd index
cp REPO_DIR/configs/index.config nextflow.config
```

Next, edit `nextflow.config` such that `params.base_dir` points to the directory (likely on S3) where you want to store your index files. (You shouldn't need to modify anything else about the config file at this stage. However, if you'd like to, you can [learn more about what each parameter does here](./config.md).)

Next, call `nextflow run` pointing at the repo directory:

```
nextflow run PATH_TO_REPO_DIR -resume
```

> [!TIP]
> You don't need to point `nextflow run` at `main.nf` any other workflow file; pointing to the directory will cause Nextflow to automatically run `main.nf` from that directory.

Wait for the workflow to run to completion; this is likely to take several hours at least.

## 8. Run the pipeline on test data

To confirm that the pipeline works in your hands, we recommend running it on a small test dataset, such as the one provided at `s3://nao-testing/gold-standard-test/raw/`, before running it on larger input data. To do this with our test dataset, follow the instructions below, or do it yourself according to the directions given [here](./usage.md).

1. Prepare the launch directory:
    - Create a clean launch directory outside the repository directory.
    - Copy over the run workflow config file to a new file in the launch directory labeled `nextflow.config`.
        - Example below shows `run.config`; for the ONT platform, remember to use `run_ont.config` instead
    - Copy the test-data sample sheet from the repository directory to the launch directory.

```
mkdir launch
cd launch
cp REPO_DIR/configs/run.config nextflow.config 
cp REPO_DIR/test-data/samplesheet.csv samplesheet.csv
```

2. Edit the config file (`nextflow.config`):
    - Edit `params.ref_dir` to point to the index directory you chose or created above (specifically `PATH_TO_REF_DIR/output`)
    - Edit `params.base_dir` to point to where you would like the pipeline to save intermediate and final pipeline outputs.
3. Choose a profile as described [here](./usage.md).
4. Run the pipeline from the launch directory:

```
nextflow run -resume -profile <PROFILE> REPO_DIR
```

Once the pipeline is complete, output and logging files will be available in the `output` subdirectory of the base directory specified in the config file.
