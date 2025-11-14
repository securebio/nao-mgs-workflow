# Developer guide
This section is solely for developers of the pipeline. We thank you greatly for your work! It includes guidance on:
- Coding style
- Containers
- Testing
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
    - Rust source code for a module goes in `src` and the binary (compiled with `cargo build --release`) goes in `resources/usr/bin`
        - Note that additional files created by Cargo (`Cargo.lock`, `Cargo.toml`) are not stored in the repo.
        - Note that in the future we will remove binaries from the repo and implement a build process instead; see Issue [#164](https://github.com/naobservatory/mgs-workflow/issues/164). Feel free to recompile existing binaries as source code is updated, but please check with a maintainer before adding new binaries to the repo.
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

## Testing

We use [nf-test](https://www.nf-test.com/) for unit, integration, and end-to-end tests. We intend to transition to using [pytest](https://docs.pytest.org/en/stable/index.html#) for unit tests of Python processes, and developers should therefore write unit tests in pytest for any processes that use Python.

### Organization of tests 

Tests themselves are organized in the `tests/` directory following the same structure as the main code. 
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

We also have `.github/workflows/end-to-end.yml` which specifies which tests run automatically on PRs through Github Actions. 

Config files for tests are organized as follows:
- `tests/nextflow.config` is the default config file for `nf-test` and specifies resources for each process based on labels.
- `tests/config/` has config files used by each workflow test.
- `configs/index-for-run-test.config` (NOT in the `tests/` directory) is used to run the `INDEX` workflow (in non-test mode) to generate the index used for `RUN` workflow tests.

The `test-data/` directory (and organization of test data in general) is described in the "Test datasets" section below.

### Writing tests

[Documentation for nf-test](https://www.nf-test.com/docs/getting-started/) and Nextflow's [nf-test training](https://training.nextflow.io/2.1.3/side_quests/nf-test/) are helpful resources.

Here are some guidelines:
- At a minimum, write a test that the process completes successfully on valid input data (`assert process.success`).
- When relevant, processes should be tested on both single- and paired-end data.
- Use descriptive test names and appropriate tags.
- When practical:
     - Validate that output files exist and have expected properties.
     - Include both positive tests (expected success) and edge case/error tests.

`tests/modules/local/fastqc/main.nf.test` is an example of bare-minimum tests for a process, and `tests/modules/local/vsearch/main.nf.test` is an example of really good, comprehensive testing!

### Test datasets 

Organization of test data 
- Small (uncompressed) test data files are in `test-data/`; larger test datasets are in S3:
    - Currently there is no set organization of the `test-data/` directory. It will be organized in the future; see issue [#349](https://github.com/naobservatory/mgs-workflow/issues/349).
- Larger public test datasets are stored in `s3://nao-testing` (publicly available). 
    - These are the "gold standard test dataset" and the "ONT wastewater test dataset", described below.
- Results of workflow runs on the test datasets from S3 are in the repo in `test-data/<dataset>-results-<workflow>`. 

#### Gold standard test dataset (`s3://nao-testing/gold-standard-test/`)
This is the default test dataset that we use for testing the `RUN` and `DOWNSTREAM` workflows on short-read data. It is a small dataset that contains 165 reads from the [Yang 2020](https://www.sciencedirect.com/science/article/abs/pii/S0048969720358514?via%3Dihub) study. 

#### ONT wastewater test dataset (`s3://nao-testing/ont-ww-test/`)
This is the default, small test dataset that we use for testing the `RUN` workflow on ONT (long-read) data.

### Adding new test data

Small "toy" data files (uncompressed, generally ~1KB or less) may be added freely to the repo in `test-data/toy-data`.

To make a new test dataset on S3, copy the test dataset to `s3://nao-testing/<name-of-test-dataset>`. A pipeline maintainer (e.g. willbradshaw or katherine-stansifer) can give you permission to add to the bucket.

```
aws s3 cp /path/to/my_dataset s3://nao-testing/my_dataset/ --acl public-read
```

> [!NOTE]
> Any time you update a test dataset, you must make it public again.

### Running tests

To run the tests locally, you need to make sure that you have a powerful enough compute instance (at least 4 cores, 14GB of RAM, and 32GB of storage). On AWS EC2, we recommend the `m5.2xlarge`. Note that you may want a more powerful instance when running tests in parallel (as described below).

> [!NOTE]
> Before running tests, to allow access to testing datasets/indexes on AWS, you will need to set up AWS credentials as described in [installation.md](installation.md), and then export them as described in the installation doc: 
>
> ```
> eval "$(aws configure export-credentials --format env)"
> ``

To run specific tests, you can specify the tests by filename or by tag. Individual tests generally complete quickly (seconds to minutes):
```
nf-test test tests/main.test.nf # Runs all tests in the main.test.nf file
nf-test test --tag run # Runs test(s) with the "run" tag; this is the end-to-end test of the RUN workflow on short-read data
```

To run all tests in the `tests` directory, use the command `nf-test test tests`. 
- Running this full suite of local tests takes hours; we recommend using the script `bin/run_parallel_test` to parallelize. 
- Running the full test suite frequently hits API limits for Seqera container pulls; to resolve this, request a user token from Seqera as described in [troubleshooting.md](troubleshooting.md).

After tests finish, you may want to clean up by running `sudo rm -rf .nf-test/`.

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

The process of checking and updating snapshots when output has changed is a bit fiddly:
- These tests will fail when any output has changed, with the error message like:
```
Test [7677da69] 'RUN workflow output should match snapshot' 
  java.lang.RuntimeException: Different Snapshot:
  <md5 sums of previous output and new output>
```  
- First, make sure the changes are expected/desired:
    - Look at the md5 checksums to determine which files have changes; make sure they are what you expect.
    - Then, find the new output files and compare them to previous output files. Make sure the changes are expected based on your code changes.
        - Previous output files are in:
            - `test-data/gold-standard-results` (for short-read `RUN` test with the tag `run_output`)
            - `test-data/gold-standard-results-downstream` (for short-read `DOWNSTREAM` test with the tag `downstream`) 
            - `test-data/ont-ww-test-results` (for ONT `RUN` test with the tag `run_output_ont`)
        - New output files are in `.nf-test/tests/<hash>/output`. (`<hash>` is shown when the test runs; e.g., in the example error message above, the test hash begins with `7677da69`).
    - Once you are happy with the changes to the output:
        - Update output files in `test-data` by copying changed output files from the `.nf-test` directory to the appropriate location in `test-data`, uncompressing the files, and committing the changes.
        - Update `nf-test` snapshots by running `nf-test test <path to test that failed> --update-snapshot`; this will update the appropriate `*.snapshot` file in `tests/workflows`. Commit the changed snapshot file.
        - Flag in PR comments that the snapshot has changed, and explain why. (Without such a comment, it's easy for reviewers to miss the updated snapshot.)

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

1. **Write new tests** for the changes that you make using `nf-test` if those tests don't already exist. At the very least, these tests should check that the new implementation runs to completion; tests that also verify the output on the test dataset are strongly encouraged.
2. **Run all relevant tests locally** and make sure they pass. "Relevant" means: 1) Any tests of any process or workflow modified by the PR; 2) Any tests for any workflows that source any such process or workflow, and 3) Any tests that use any such process or workflow in setup.
    - For **nf-test**: You may run all existing tests as described in the "Testing" section above, or identify relevant tests by recursively grepping for the process name in the `workflows`, `subworkflows`, and `tests` directories.
    - For **pytest**: If you modify Python scripts, run `pytest` on the corresponding test files to ensure unit tests pass.
    - **Note which tests were run in your PR description.**
    - If you make any changes that affect the output of the pipeline, list/describe the changes that occurred in the pull request. 
3. **Update the `CHANGELOG.md` file** with the changes that you are making, and update the `pipeline-version.txt` and `pyproject.toml` file with the new version number.
    - More information on how to update the `CHANGELOG.md` file can be found [here](./versioning.md). Note that, before merging to `main`, version numbers should have the `-dev` suffix. This suffix should be used to denote development versions in `CHANGELOG.md`, `pipeline-version.txt`, and `pyproject.toml`, and should only be removed when preparing to merge to `main`.
4. **Update the expected-output-{run,downstream}.txt files** with any changes to the output of the RUN or DOWNSTREAM workflows.
5. **Pass automated tests on GitHub Actions**. These run automatically when you open a pull request.
6. **Write a meaningful description** of your changes in the PR description and give it a meaningful title. 
    - In comments, feel free to flag any open questions or places where you need careful review. 
7. **Request review** from a maintainer on your changes. Current maintainers are jeffkaufman, willbradshaw, and katherine-stansifer. 
    - Make sure to assign the PR to the desired reviewer so that they see your PR (put them in the "Assignees" section on GitHub as well as in the "Reviewers" section).
        - If the reviewer is not satisfied and requests changes, they should then change the "Assignee" to be the person who originally submitted the code. This may result in a few loops of "Assignee" being switched between the reviewer and the author.
8. To merge, you must **have an approving review** on your final changes, and all conversations must be resolved. After merging, please delete your branch!

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

Only a pipeline maintainer/member of the Nucleic Acid Observatory should author a new release. The process for going through a new release can be found in NAO private documentation; however, the general outline can be found below.

To start a new release:
- Stop approving new feature PRs into `dev`.
- Create a release branch (e.g., `release/<maintainer first name or handle>/X.X.X.X`) for release-related changes (e.g., changelog updates, version bumps).
- If any issues are found, fix them with new PRs that are squash merged into `dev`, and then pull these changes into your release branch.
- Once the release is approved, squash merge the release branch onto `dev`.
- Merge `dev` into `main` using a normal (non-squash) merge.
- Tag the release.
- Delete the release branch. 

