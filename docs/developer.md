# Developer guide
This section is solely for developers of the pipeline. We thank you greatly for your work! It includes guidance on:
- Coding style
- Containers
- GitHub issues
- Pull requests (PRs)
- New releases

See [testing.md](testing.md) for detailed guidance on testing.

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
    - Type hints are encouraged. When used, prefer Python 3.12+ native syntax (e.g. `list[str]`, `str | None`) over imports from the `typing` module.
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
    - More information on how to update version numbers can be found in [versioning.md](./versioning.md). Note that, before merging to `main`, version numbers should have the `-dev` suffix. This suffix should be used to denote development versions in `CHANGELOG.md`, `pipeline-version.txt`, and `pyproject.toml`, and should only be removed when preparing to merge to `main`.
    - CHANGELOG entries should be concise and action-oriented, starting with a verb (e.g., "Add ...", "Fix ...", "Remove ...", "Update ..."). Top-level entries should describe user-facing outcomes, not implementation details — for example, prefer "ONT and short-read validation hits now share the same schema and column set" over "Extended `ADD_FIXED_COLUMN` to accept comma-separated column names." Implementation details can be included as sub-bullets if helpful. Reference PR numbers with `(#NNN)` when relevant. Use `##` subheaders to group related changes when a release has many entries. The topmost heading must match the version in `pyproject.toml` (enforced by CI).
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

    1. Non-point releases are NOT automatically merged to `stable`. To update `stable` with a non-point release, use the "Manually reset stable branch to main" workflow in GitHub Actions (`manual-reset.yml`). This requires typing "reset stable" as confirmation.

[^refs]: For reference genomes, check for updated releases for human, cow, pig, and mouse; do not update carp; update *E. coli* if there is a new release for the same strain. Check [SILVA](https://www.arb-silva.de/download/archive/) for rRNA databases and [here](https://benlangmead.github.io/aws-indexes/k2) for Kraken2 databases.
[^approval]: Note that, to streamline the release process, we no longer require an approving review for PRs into `main`. (We still require an approving review for `release` PRs into `dev`.)

## Schemas

We are currently in the process of defining and enforcing [schemas](../schemas/) for our output files, using the [table schema standard](https://datapackage.org/standard/table-schema/) and [frictionless Python framework](https://framework.frictionlessdata.io/). Not all output files yet have schemas; those that have been added are used to validate test outputs in Github Actions to ensure that the output produced matches the schema.

### Policy

All new tabular output files should have a complete schema, and all existing schemas should be maintained. To add a new schema, create a corresponding JSON file in `schemas`. All schemas should have:

- A `fields` entry with subentries for each column in the associated output file (no missing columns). Each column subentry should at minimum have `name`, `type`, `title`, & `description` fields. Most should also have a `constraints` fields delimiting permitted values.
- A `primaryKey` entry describing a set of fields that are collectively guaranteed to uniquely identify each row (can often be a single field).
- A `missingValues` entry listing permitted null values (typically `""` and `"NA"`).

### Versioning and guarantees

Under our [versioning policy](./versioning.md), changes to schema `title` and `description` fields can be made in point (4th-number) releases. Any other schema change must be a schema (2nd-number) or major (1st-number) release, accompanied by explicit alerts to owners of dependent codebases.

**What the schema guarantees:** Consumers can rely on the schema to define the complete structure and validation rules for an output file. Any behavior encoded in the schema (column names, types, constraints) is guaranteed to be stable within a schema version.

**What the schema does not guarantee:** Formatting or semantics not explicitly encoded in schema constraints are not guaranteed. For example, if a string field contains a formatted value (like `taxid:name`) but no `pattern` constraint enforces this format, the format may change in a point release. If you need to parse a field's contents programmatically, request that appropriate constraints be added to the schema.

### Working with schemas

- If you are working on a change that affects pipeline outputs, review the schema files for affected outputs where available, to know what's expected for each column.
- If an input to DOWNSTREAM has no data, the `createEmptyGroupOutputs` module will generate header-only outputs based on schemas where available. Output files with no corresponding schema will be empty.
- To validate output files locally, run `bin/validate_schemas.py`.
- If you are developing code external to this repository that depends on its outputs, you should review the corresponding schemas to understand what guarantees you can expect.