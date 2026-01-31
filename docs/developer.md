# Developer guide
This section is solely for developers of the pipeline. We thank you greatly for your work! It includes guidance on:
- Coding style
- Containers
- Testing
- Continuous integration (CI)
- GitHub issues
- Pull requests (PRs)
- New releases

The pipeline is written primarily in [Nextflow](https://www.nextflow.io/docs/latest/index.html), with select scripts in Python, Rust, and R. 

## Coding style guide

These guidelines represent best practices to implement in new code, though some legacy code may not yet conform to all conventions.

### Nextflow
- Code organization
    - We use a workflow of workflows organization (`main.nf` -> workflow -> subworkflow -> process). 
    - Aim for one process per module, in a `main.nf` file.
    - Avoid creating duplicate processes. If you need a slight variation on existing behavior, parameterize or otherwise tweak an existing process.
    - Avoid very large `script` or `shell` blocks in Nextflow processes where possible.
        - If the block gets bigger than about 20 lines, we probably want to split it into multiple processes, or move functionality into a Rust or Python script. 
- Documentation
    - Extensive comments are encouraged. 
    - Each workflow, subworkflow, or process should begin with a descriptive comment explaining what it does.
    - Each workflow should have a `<workflow_name>.md` document in `docs/`.
- Process conventions (see `modules/local/vsearch/main.nf` for an example of a well-written process that follows these conventions):
    - All processes should have a label specifying needed resources (e.g. `label "small"`). Resources are then specified in `configs/resources.config`.
    - All processes should have a label specifying the Docker container to use (e.g. `label "BBTools"`). Containers are then specified in `configs/containers.config`.
    - Any processes that are used only for testing should have `label "testing"`.
    - All processes should emit their input (for testing validation); use `ln -s` to link the input to the output.
    - Most processes have two output channels, `input` and `output`. If a process emits multiple types of output, use meaningful emit names describing the output types (e.g. `match`, `nomatch`, and `log` from `process BBDUK`).
    - Most, but not all, processes are *labeled* (input is a tuple of a sample name and file path). If input is labeled, output should also be labeled.
- Naming:
    - Use `lower_snake_case` for variable and channel names.
    - Use `UPPER_SNAKE_CASE` for process, subworkflow, and workflow names.
    - Use `camelCase` for module and subworkflow directory names.
    - Use lowercase with hyphens for other file names (config and test data files).
- Performance conventions:
    - Design processes for streaming: avoid loading significant data into memory.
    - Use compressed intermediate files to save disk space.

### Other languages (Python, Rust, R)
- Add non-Nextflow scripts only when necessary; when possible, use existing bioinformatics tools and shell commands rather than creating custom scripts.
    - Though, as noted above, if a process's `script`/`shell` block is getting longer than about 20 lines, it may make sense to move the functionality to a script.
- New scripts should be in Rust (preferred) or Python (acceptable if performance is not critical).  We have a few legacy scripts in R but discourage adding new R scripts.
- Organization:
    - Python and R scripts for a module go in `resources/usr/bin/`
    - Rust source code lives in the centralized `rust-tools/` directory at the repository root, organized as a Cargo workspace. Compiled binaries are NOT stored in the repository; they are built into the `nao-rust-tools` container via CI.
- Rely on the standard library as much as possible. 
    - Widely used 3rd-party libraries (e.g. `pandas` or `boto3`) are OK on a case-by-case basis if they allow for much cleaner or more performant code. Please flag use of these libraries in PR comments so the reviewer can assess.
    - Avoid third-party libraries that are not widely used, or that bring in a ton of dependencies of their own.
- Include proper error handling and logging
- Performance conventions:
    - Always process large files line-by-line or in manageable chunks.
    - Support compressed file formats (.gz, .bz2, .zst). 
- Python style: 
    - Loosely follow PEP 8 conventions.
    - Type hints are encouraged but not currently required.
    - Linting and type checking are encouraged (our go-to tools are `ruff` for linting and `mypy` for type checking), but not currently required.
    - Formatting applied (including in nested directories) will follow the configuration in `pyproject.toml`.
    - After making any formatting changes, carefully review the diff to ensure no unintended modifications were introduced that could affect functionality.

### Rust

All Rust tools live in the centralized `rust-tools/` directory, organized as a Cargo workspace. Compiled binaries are NOT stored in the repository; instead, they are built into the `nao-rust-tools` container via GitHub Actions CI.

**Adding a new Rust tool:**
1. Create your Rust project in `rust-tools/{tool_name}/` with `Cargo.toml` and `src/main.rs`
2. Add it as a workspace member in `rust-tools/Cargo.toml`
3. Update `docker/nao-rust-tools.Dockerfile`:
   - The builder stage already builds the entire workspace, so you shouldn't have to change anything here.
   - In the runtime stage: add `COPY --from=builder <path to binary in builder> /usr/local/bin/` to include the new binary.
4. Use `label "rust_tools"` in your Nextflow process
5. Add a comment above the process noting: `// Tool source: rust-tools/{tool_name}/`

**Local development:**
```bash
# After making changes to Rust source:
./bin/build-rust-local.sh

# Run workflow with local container:
nextflow run main.nf -profile rust_dev ...
```

**Testing on AWS Batch:**
```bash
# Build, tag with your username, and push to ECR:
./bin/build-rust-local.sh
docker tag nao-rust-tools:local public.ecr.aws/q0n1c7g8/nao-mgs-workflow/rust-tools:dev-$(whoami)
docker push public.ecr.aws/q0n1c7g8/nao-mgs-workflow/rust-tools:dev-$(whoami)

# Run on Batch with your container:
nextflow run main.nf --rust_tools_version dev-$(whoami) -profile batch ...
```

**Note:** The container is automatically rebuilt by GitHub Actions when Rust source files change on `dev` or `main`. Use `--rust_tools_version dev` to test against the dev branch build.

## Containers

Where possible, we use [Seqera Wave containers](https://docs.seqera.io/wave), managed programmatically via YAML files in the `containers` directory. To build a new Wave container:

1. Write a placeholder statement in `configs/containers.config` specifying a label corresponding to your new container (e.g. `withlabel: foo { container = "bar" }`).
2. Write a YAML file in the `containers` directory specifying the dependencies to include. Always specify exact versions.
3. Run `bin/build_wave_container.py PATH_TO_YAML_FILE` to initiate the container build on Wave. The script will automatically update `configs.containers.config` with the appropriate container path.
4. Wait a few minutes for the container to build before calling processes that depend on it.

If you need a container with functionality beyond what's possible with Conda and Wave, you can build a custom Docker container and host it on [Docker Hub](https://hub.docker.com/). To do this, create a new Dockerfile in the `docker` directory. The name should have the prefix `nao-` followed by a descriptive name containing lowercase letters and hyphens, e.g. `docker/nao-blast-awscli.Dockerfile`. Once the Dockerfile is created, a repo maintainer can build and push it to Docker Hub using the script `bin/build-push-docker.sh`. (This should be done by a repo maintainer as it requires being logged in to DockerHub with the securebio username.) 

## Python Development Setup

### Recommended: Using uv

For Python development, we recommend using [uv](https://docs.astral.sh/uv/), a fast Python package and project manager. It automatically manages Python versions and dependencies without requiring manual virtual environment setup.

**Installation:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Running Python tools:**
You can run Python tools directly without installing them first:
```bash
uv run pytest           # Run tests
uv run ruff check .     # Lint code
uv run mypy .           # Type checking
```

Alternatively, you can sync the environment once and then use the tools directly:
```bash
uv sync                 # Install all dependencies from pyproject.toml
source .venv/bin/activate
pytest                  
ruff check .
mypy .
```

## Post Processing

The `post-processing/` directory contains standalone Python scripts for additional analyses that can be run on workflow outputs. These scripts are not yet integrated into the main pipeline but provide useful functionality for tasks like similarity-based duplicate marking. See [post-processing/README.md](../post-processing/README.md) for details on available scripts and usage.

## Testing

We currently use [`nf-test`](https://www.nf-test.com/) for unit, integration, and end-to-end tests for many modules and all workflows. However, `nf-test` is very slow, so we're transitioning away from using it in cases where other, faster testing libraries are available. Wherever possible, a module should have a single `nf-test` test that confirms it runs end-to-end without crashing, while detailed functionality should be tested elsewhere. For Python processes, we recommend writing unit tests in [`pytest`](https://docs.pytest.org/en/stable/index.html).

### nf-test

#### Organization

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

#### Writing tests

[Documentation for `nf-test`](https://www.nf-test.com/docs/getting-started/) and Nextflow's [`nf-test` training](https://training.nextflow.io/2.1.3/side_quests/nf-test/) are helpful resources.

In cases where a module is a thin wrapper around a script in another language, comprehensive unit testing should be done using an appropriate testing library for that language (e.g. pytest for Python scripts). In this case, `nf-test` tests for the module should be kept to a minimum, typically a single end-to-end test that confirms the module runs without crashing. We are currently in the process of moving as many modules as possible to this paradigm; however, some modules are harder to transition than others. For modules that retain major functionality in the Nextflow process itself, a comprehensive `nf-test` suite is still needed. In this case, we recommend the following guidelines:

- At a minimum, write a test that the process completes successfully on valid input data (`assert process.success`).
- When relevant, processes should be tested on both single- and paired-end data.
- Use descriptive test names and appropriate tags.
- When practical:
     - Validate that output files exist and have expected properties.
     - Include both positive tests (expected success) and edge case/error tests.

`tests/modules/local/fastqc/main.nf.test` is an example of bare-minimum tests for a process, and `tests/modules/local/vsearch/main.nf.test` is an example of really good, comprehensive testing!

#### Test datasets 

##### Current test data

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

##### Tiny test data

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

#### Running tests

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

#### Updating snapshots

Our test suite includes end-to-end tests of the `RUN` and `DOWNSTREAM` workflows that verify their outputs on test datasets have not changed unexpectedly. These tests use `nf-test`'s [snapshots](https://www.nf-test.com/docs/assertions/snapshots/).

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

### `pytest`

`pytest` tests should be located in the same directory as their corresponding Python script, and should be named by prepending `test_` to the script's filename (e.g., `test_my_script.py` for a script named `my_script.py`).

Unlike nf-test, `pytest` tests are very fast and cheap to run. Consequently, we recommend being as comprehensive as reasonably possible when writing `pytest` suites.

### Automated testing

We run the full test suite automatically on each pull request using GitHub Actions with larger runners and parallelization, as specified by configuration files in `.github/workflows`. This includes all `pytest` tests and all `nf-test` tests (modules, subworkflows, and workflows). We also run [Trivy](https://trivy.dev/) on all containers used by the workflow to check for security vulnerabilities.

Because the CI runs the complete test suite, **running tests locally before pushing is no longer required**. However, running relevant tests locally can still be useful for faster feedback during development, especially when iterating on a specific module or workflow.

For comprehensive documentation of all CI workflows, see [ci.md](ci.md). 

## GitHub issues
We use [GitHub issues](https://github.com/naobservatory/mgs-workflow/issues) to track any issues with the pipeline: bugs, cleanup tasks, and desired new features. 
Opening issues:
- All issues should be self-contained: the description should include enough detail to understand what needs to be done.
- We use labels to prioritize and track:
    - `enhancement`, `time&cost`, `bug`, and `documentation` to describe the type of issue.
    - `priority_1` (highest), `priority_2`, and `priority_3` to mark importance.
    - `in-progress` when actively being worked on, and `done` when resolved but changes are not yet merged to `main`.
         - only close issues when the fix/enhancement is merged to `main`.

## Pull requests (PRs)

### Creating branches and making changes
To contribute to the pipeline, start by creating a new branch off of `dev`. Branch names follow this convention:

**Format:** `branch_type/owner_name/issue_id-description` (all lowercase)

**Components:**
- `branch_type`: The type of work (see below)
- `owner_name`: Your first name or developer handle
- `issue_id`: The GitHub issue number (required for all branches - if an issue doesn't exist, create one first)
- `description`: Short description (2-4 words, [kebab-case](https://developer.mozilla.org/en-US/docs/Glossary/Kebab_case))

**Branch types:**
- `feature`: New functionality or enhancements
- `bugfix`: Resolving non-critical defects
- `hotfix`: Critical production fixes (merged to both main and dev)
- `release`: Preparing releases, version bumps, stabilization
- `maintenance`: Non-feature changes (refactoring, dependencies, CI/CD, docs)
- `evergreen`: Exploratory or experimental work

**Example:** `bugfix/harmon/461-fix-lca-sorting`


>[!CAUTION]
> Do not make pull requests from the `dev` or `main` branches.

Please keep PRs small--a single feature or bugfix. For complex features, split across multiple PRs in small, logical chunks if at all possible. It's OK if a PR lays groundwork for a new feature but doesn't implement it yet.  

Feel free to use AI tools (Cursor, GitHub Copilot, Claude Code, etc.) to generate code and tests. The author is responsible for reviewing and understanding all AI-generated code before sending it for review.

### Sending PRs for review

> [!NOTE]
> During a release, new feature PRs are not merged into `dev`. Please check with a maintainer if a release is in progress.

1. **Write new tests** for the changes that you make if those tests don't already exist. At the very least, these tests should check that the new implementation runs to completion; tests that also verify the output on the test dataset are strongly encouraged. As discussed above, we recommend writing end-to-end tests in `nf-test` and unit tests in language-specific testing libraries like `pytest`.
    - If you make any changes that affect the output of the pipeline, list/describe the changes that occurred in the pull request.
2. **Update the `CHANGELOG.md` file** with the changes that you are making, and update the `pipeline-version.txt` and `pyproject.toml` file with the new version number.
    - More information on how to update the `CHANGELOG.md` file can be found [here](./versioning.md). Note that, before merging to `main`, version numbers should have the `-dev` suffix. This suffix should be used to denote development versions in `CHANGELOG.md`, `pipeline-version.txt`, and `pyproject.toml`, and should only be removed when preparing to merge to `main`.
3. **Update the expected-output-{run,downstream}.txt files** with any changes to the output of the RUN or DOWNSTREAM workflows.
4. **Pass automated tests on GitHub Actions**. These run automatically when you open a pull request.
5. **Write a meaningful description** of your changes in the PR description and give it a meaningful title.
    - In comments, feel free to flag any open questions or places where you need careful review.
6. **Request review** from a maintainer on your changes. Current maintainers are jeffkaufman, willbradshaw, and katherine-stansifer.
    - Make sure to assign the PR to the desired reviewer so that they see your PR (put them in the "Assignees" section on GitHub as well as in the "Reviewers" section).
        - If the reviewer is not satisfied and requests changes, they should then change the "Assignee" to be the person who originally submitted the code. This may result in a few loops of "Assignee" being switched between the reviewer and the author.
7. To merge, you must **have an approving review** on your final changes, and all conversations must be resolved. After merging, please delete your branch!

### Squash merging

We use squash merges for all PRs to maintain a clean, linear history on `main`. 

**How to squash merge:** Instead of clicking "Merge pull request" on GitHub, click the dropdown arrow next to it and select "Squash and merge". Make sure the squash commit title includes the PR number followed by the description (e.g., "#424 Add viral read filtering").

**Dealing with dependent branches after squash merging:**

This situation commonly arises when:
1. You create branch A with multiple commits
2. Submit branch A for review  
3. Fork branch B from branch A and work on a new feature
4. Branch A gets edited for reviewer feedback, then squash-merged to `dev`
5. You merge `dev` into branch B

Branch B will then contain both the original unsquashed commits from branch A AND the new squash commit, creating an intimidating PR with duplicate commit chains.

**Recommended approach:**
Try rebasing branch B onto `dev` first (`git rebase dev`). If rebasing doesn't work cleanly, just merge `dev` and don't worry about the commits in the PR (`git merge dev`). The diff should be fine and the commits will get squashed anyway when merged. 

**Note:** Squash merging should only be used for feature branches merging into `dev`. When merging from `dev` to `main` for releases, use regular (non-squash) merges to preserve the development history. 

## New releases

By default, all changes are made on individual branches, and merged into `dev`. Periodically, a collection of `dev` changes are merged to `main` as a new release. New releases are fairly frequent (historically, we have made a new release every 2-4 weeks).

Only pipeline maintainers should author a new release. The process for going through a new release is as follows:

1. Stop approving new feature PRs into `dev`.
2. Create a release branch `release/USER_HANDLE/X.Y.Z.W` (see [here](./versioning.md) for information on our versioning system). In this branch:

    1. Review and consolidate additions to `CHANGELOG.md`; these often get somewhat disjointed across many small PRs to `dev`.
    2. Update the version number in `CHANGELOG.md` and `pyproject.toml` to remove any `-dev` suffix and reflect the magnitude of changes (again, see [here](./versioning.md) for information on the versioning schema).
    3. Check if the changes in the release necessitate a new index version. Most releases do not; the primary reason one would is if changes to processes or workflows would cause an incompatibility between the contents of the index and the expectations of the RUN or DOWNSTREAM workflows. If this is the case:

        1. Check for new releases of reference databases and update `configs/index.config`[^refs].
        2. Update `index-min-pipeline-version` and `pipeline-min-index-version` in the `[tool.mgs-workflow]` section of `pyproject.toml` to reflect any changes to compatibility restrictions.
        3. Delete `s3://nao-testing/mgs-workflow-test/index-latest`, then run the `INDEX` workflow to generate a new index at that location. (This will update the index used by relevant Github Actions checks.)

3. Open a PR to merge the release branch into `dev`, wait for CI tests to complete, and resolve any failing tests. Then:

    1. Squash-merge the PR into `dev`, then open a new PR from `dev` into `main` entitled "Merge dev to main -- release X.Y.Z.W".
    2. Quickly review the PR changes to ensure the changed files are consistent with the changes noted in `CHANGELOG.md` (no need to review file contents deeply at this stage).
    3. Double check that documentation and tests have been updated to stay consistent with changes to the pipeline.
    4. Wait for additional long-running pre-release checks to complete in Github Actions.
    5. If any issues or test failures arise in the preceding steps, fix them with new bugfix PRs into `dev`, then rebase the release branch onto `dev`.

4. Once all checks pass, merge the PR into main **without squashing**[^approval]. A Github Actions workflow will automatically create and tag a new release and reset other branches (`dev` & `ci-test`, plus `stable` if only the fourth version number has changed) to match `main`.

    1. Non-point releases are NOT automatically merged to `stable`. To update `stable` with a non-point release, a repo admin must manually reset the branch.

[^refs]: For reference genomes, check for updated releases for human, cow, pig, and mouse; do not update carp; update *E. coli* if there is a new release for the same strain. Check [SILVA](https://www.arb-silva.de/download/archive/) for rRNA databases and [here](https://benlangmead.github.io/aws-indexes/k2) for Kraken2 databases.
[^approval]: Note that, to streamline the release process, we no longer require an approving review for PRs into `main`. (We still require an approving review for `release` PRs into `dev`.)
