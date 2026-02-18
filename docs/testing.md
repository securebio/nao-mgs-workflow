# Testing

We currently use [`nf-test`](https://www.nf-test.com/) for unit, integration, and end-to-end tests for many modules and all workflows. However, `nf-test` is very slow, so we're transitioning away from using it in cases where other, faster testing libraries are available. Wherever possible, a module should have a single `nf-test` test that confirms it runs end-to-end without crashing, while detailed functionality should be tested elsewhere. For Python processes, we recommend writing unit tests in [`pytest`](https://docs.pytest.org/en/stable/index.html).

## nf-test

### Organization

`nf-test` tests are organized in the `tests/` directory following the same structure as the main code:

```
tests/
├── modules/local/
│   └── toolName/
│       └── main.nf.test
├── subworkflows/local/
│   └── workflowName/
│       └── main.nf.test
└── workflows/
    └── workflow_name.nf.test
```

Config files for tests are organized as follows:
- `tests/nextflow.config` is the default config file for `nf-test` and specifies resources for each process based on labels.
- `tests/config/` has config files used by each workflow test.
- `configs/index-for-run-test.config` (NOT in the `tests/` directory) is used to run the `INDEX` workflow (in non-test mode) to generate the index used for `RUN` workflow tests.

The `test-data/` directory (and organization of test data in general) is described in the "Test datasets" section below.

### Writing tests

[Documentation for `nf-test`](https://www.nf-test.com/docs/getting-started/) and Nextflow's [`nf-test` training](https://training.nextflow.io/2.1.3/side_quests/nf-test/) are helpful resources.

In cases where a module is a thin wrapper around a script in another language, comprehensive unit testing should be done using an appropriate testing library for that language (e.g. pytest for Python scripts). In this case, `nf-test` tests for the module should be kept to a minimum, typically a single end-to-end test that confirms the module runs without crashing. We are currently in the process of moving as many modules as possible to this paradigm; however, some modules are harder to transition than others. For modules that retain major functionality in the Nextflow process itself, a comprehensive `nf-test` suite is still needed. In this case, we recommend the following guidelines:

- At a minimum, write a test that the process completes successfully on valid input data (`assert process.success`).
- When relevant, processes should be tested on both single- and paired-end data.
- Use descriptive test names and appropriate tags.
- When practical:
     - Validate that output files exist and have expected properties.
     - Include both positive tests (expected success) and edge case/error tests.

`tests/modules/local/fastqc/main.nf.test` is an example of bare-minimum tests for a process, and `tests/modules/local/vsearch/main.nf.test` is an example of really good, comprehensive testing!

### Test datasets

#### Current test data

- Small (uncompressed) test data files are in `test-data/`; larger test datasets are in S3:
    - Currently there is no set organization of the `test-data/` directory. It will be organized in the future; see issue [#349](https://github.com/naobservatory/mgs-workflow/issues/349).
    - Small "toy" data files (uncompressed, generally ~1KB or less) may be added freely to the repo in `test-data/toy-data`.
- Public test datasets are stored in `s3://nao-testing/tiny-test/`. (Older test files, no longer in active use, can be found in `s3://nao-testing/gold-standard-test/` and `s3://nao-testing/ont-ww-test/`.)
- Results of workflow runs on the test datasets from S3 are in the repo in `test-data/results/`

To make a new test dataset on S3, copy the test dataset to `s3://nao-testing/<name-of-test-dataset>`. A pipeline maintainer (e.g. willbradshaw or katherine-stansifer) can give you permission to add to the bucket.

```
aws s3 cp /path/to/my_dataset s3://nao-testing/my_dataset/ --acl public-read
```

> [!NOTE]
> Any time you update a test dataset, you must make it public again.

#### Tiny test data

In order to cut down on the time it takes to run our test suite, we have switched much of it from larger test data stored in S3 to small test datafiles stored locally. The following instructions detail how to generate this new test data:

1. Create new reference datasets using `bin/build_tiny_test_databases.py`. The defaults provided should suffice in most cases.
2. Generate the new test index:
    a. Create a fresh launch directory and copy the config file: `cp configs/index-for-run-test.config LAUNCH_DIR/nextflow.config`.
    b. Edit the config file to specify a base directory (`params.base_dir`) and Batch job queue (`process.queue`).
    c. Execute the workflow from the launch directory: `nextflow run PATH_TO_REPO_DIR`. (This usually takes about 10 minutes.)
    d. Copy the tiny index from S3 to the repo: `aws s3 cp --recursive BASE_DIR/output test-data/tiny-index/output`, followed by `rm -r test-data/tiny-index/output/logging/trace*` to remove run-specific information we don't want in the repo.
3. Generate test input data (simulated ONT & Illumina reads):
    a. Set up an environment with appropriate versions of InSilicoSeq and NanoSim, e.g. with Conda: `conda env create -f test-data/tiny-index/reads/env.yml; conda activate GenerateTestData`
    b. Run `bin/prepare_tiny_test_data.py` and commit the results to this repository.
    c. Remember to return to your normal computing environment after you're done.
4. Commit new test index and input data to the repo. This should not generate any new files, just replace existing ones.

### Running tests

To run the tests locally, you need to make sure that you have a powerful enough compute instance (at least 4 cores, 14GB of RAM, and 32GB of storage). On AWS EC2, we recommend the `m5.2xlarge`. Note that you may want a more powerful instance when running tests in parallel (as described below).

> [!NOTE]
> Before running tests, to allow access to testing datasets/indexes on AWS, you will need to set up AWS credentials as described in [installation.md](installation.md), and then export them as described in the installation doc:
>
> ```
> eval "$(aws configure export-credentials --format env)"
> ```

In running tests, we don't recommend calling `nf-test test` directly, as this can easily run into issues with permissions and environment configuration. Instead, we recommend using `bin/run-nf-test.sh`, which wraps `nf-test` with the necessary `sudo` permissions and environment variables. For parallel execution, use the `--num-workers` flag. Due to the overhead associated with parallelization, we only recommend parallel execution for running large portions of the test suite.

```
bin/run-nf-test.sh tests/main.test.nf # Runs all tests in the main.test.nf file
bin/run-nf-test.sh --num-workers 8 tests # Runs all tests in parallel across 8 threads.
bin/run-nf-test.sh --num-workers 8 tests --tag expect_failure # Run failure tests only across 8 threads
```

After tests finish, you should clean up by running `bin/clean-nf-test.sh`.

> [!NOTE]
> Periodically delete docker images to free up space on your instance. Running the following command will delete all docker images on your system:
> ```
> docker kill $(docker ps -q) 2>/dev/null || true
> docker rm $(docker ps -a -q) 2>/dev/null || true
> docker rmi $(docker images -q) -f 2>/dev/null || true
> docker system prune -af --volumes
> ```

### Updating snapshots

Our test suite includes end-to-end tests of the `RUN` and `DOWNSTREAM` workflows that verify their outputs on test datasets have not changed unexpectedly. These tests use `nf-test`'s [snapshots](https://www.nf-test.com/docs/assertions/snapshots/).

> [!CAUTION]
> Only update snapshots after verifying that the output changes are intentional and correct. Never update snapshots just to make failing tests pass — doing so can silently nullify the tests and hide real bugs.

The process of checking and updating snapshots when output has changed is a bit fiddly:
- These tests will fail when any output has changed, with the error message like:
```
Test [7677da69] 'RUN workflow output should match snapshot'
  java.lang.RuntimeException: Different Snapshot:
  <md5 sums of previous output and new output>
```
- First, make sure the changes are expected/desired:
    - Look at the md5 checksums to determine which files have changes; make sure they are what you expect.
    - Then, find the new output files and compare them to previous output files in `test-data/results`. Make sure the changes are expected based on your code changes.
        - Previous output files are in `test-data/results`.
        - New output files are in `.nf-test/tests/<hash>/output`. (`<hash>` is shown when the test runs; e.g., in the example error message above, the test hash begins with `7677da69`).
    - Once you are happy with the changes to the output:
        - Update output files in `test-data` by copying changed output files from the `.nf-test` directory to the appropriate location in `test-data`, uncompressing the files, and committing the changes.
        - Update `nf-test` snapshots by running `bin/run-nf-test.sh <path to test that failed> --update-snapshot`; this will update the appropriate `*.snapshot` file in `tests/workflows`. Commit the changed snapshot file.
        - Flag in PR comments that the snapshot has changed, and explain why. (Without such a comment, it's easy for reviewers to miss the updated snapshot.)

## `pytest`

`pytest` tests should be located in the same directory as their corresponding Python script, and should be named by prepending `test_` to the script's filename (e.g., `test_my_script.py` for a script named `my_script.py`).

Unlike nf-test, `pytest` tests are very fast and cheap to run. Consequently, we recommend being as comprehensive as reasonably possible when writing `pytest` suites.

## Automated testing

We run the full test suite automatically on each pull request using GitHub Actions with larger runners and parallelization, as specified by configuration files in `.github/workflows`. This includes all `pytest` tests and all `nf-test` tests (modules, subworkflows, and workflows). We also run [Trivy](https://trivy.dev/) on all containers used by the workflow to check for security vulnerabilities.

Because the CI runs the complete test suite, **running tests locally before pushing is no longer required**. However, running relevant tests locally can still be useful for faster feedback during development, especially when iterating on a specific module or workflow.

For comprehensive documentation of all CI workflows, see [ci.md](ci.md).
