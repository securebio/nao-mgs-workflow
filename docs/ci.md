# Continuous Integration (CI)

We use continuous integration (CI) via GitHub Actions to perform a number of automated tests on each PR, as well as other non-test actions in certain cases. Our CI workflows are configured in `.github/workflows`.

## Overview

### Types of CI

Our CI workflows fall into three broad categories:

1. **Development tests:** Quick tests that must pass before code can be merged into `dev` or `main`.
2. **Release tests:** Slow or expensive tests that must pass for a PR to be merged into `main`. To enable rapid development, these tests are not required on PRs into `dev`; however, they still run automatically after code has been merged into `dev` to help surface issues before a release.
3. **Non-test automation:** Workflows that perform actions like creating releases, managing branches, or tagging issues.

### Runners and billing

Most of our CI workflows use GitHub's standard `ubuntu-latest` runner for execution. We have 3,000 free minutes of execution with this runner per month across our entire SecureBio GitHub account, which is enough for most purposes. This runner is also quite throttled in its [resources](https://docs.github.com/en/actions/reference/runners/github-hosted-runners) and will struggle with demanding tasks.

For tests that demand multiple cores or other substantial resources, we use custom runners set up in the SecureBio Team account. These cost a small amount of money per minute of execution: for example, the `ubuntu-16` runner we use for several tests [costs](https://docs.github.com/en/billing/reference/actions-runner-pricing) about $0.04 per minute. These costs are small enough to generally not be a concern, but developers should take care if adding very long-running tests or doing extensive iterative testing of a new CI workflow.

Several workflows access external resources, including AWS S3 and AWS Batch, and will incur corresponding costs for compute and storage. As above, these costs are usually not large enough to be problematic, but developers should take care around changes that could substantially increase our resource load.

### Conditional execution

Many of our CI tests have two desiderata that are somewhat in tension with each other:

1. We want certain tests to run on every PR and to block merging if they fail; but
2. We also want to avoid running tests pointlessly if the files they test haven't been modified.

The simple approach of just setting run conditions in each CI file fails here; if these conditions aren't met, the test will not run, and the PR will be blocked by our branch protection rules.

To achieve both of these goals, many of our test workflows instead use the [dorny/paths-filter](https://github.com/dorny/paths-filter) action to check whether relevant files have changed. If no relevant files are modified, downstream steps in the workflow are skipped and the workflow succeeds trivially; otherwise, the downstream steps run and the test succeeds or fails based on their outcomes.

To see the files each CI test checks before executing, refer to the files in the `.github` directory.

### The `ci-test` branch

`ci-test` is a special branch used for testing CI workflows. All checks that would run on PR or merge into `dev` or `main` are also configured to run on PR into `ci-test`. This allows us to test execution of checks that would otherwise be skipped on a PR into `dev`.

If submitting a PR that affects our CI, the recommended process is:

1. Submit a PR to merge your working branch into `ci-test`;
2. Wait for all tests to complete and fix any that fail;
3. Get approval from a reviewer while still on `ci-test`;
4. After making any final changes and checking that all tests pass, switch the destination branch to `dev`;
5. Merge the PR.

## Development tests

These tests run on PRs to `main`, `dev`, `stable`, and `ci-test`. They must pass before code can be merged.

### nf-test

We have five nf-test workflows that test different parts of the pipeline:

| Workflow | Tests |
|----------|-------|
| `nf-test-modules.yml` | `tests/modules/` |
| `nf-test-subworkflows.yml` | `tests/subworkflows/` |
| `nf-test-workflows-index.yml` | `tests/workflows/index.nf.test` |
| `nf-test-workflows-run.yml` | `tests/workflows/run.nf.test` |
| `nf-test-workflows-downstream.yml` | `tests/workflows/downstream.nf.test` |

### Python unit tests (`pytest.yml`)

Runs our entire pytest suite across `bin`, `modules`, and `post-processing/tests/`.

### Trivy container scan (`trivy-scan.yml`)

Scans all containers defined in `configs/containers.config` for security vulnerabilities using [Trivy](https://trivy.dev/).

> [!NOTE]
> As of 2025-01-28, the Trivy test is expected to fail and is not required to pass to merge PRs.

### Version and changelog checks

These checks run unconditionally (no path filtering) to ensure version consistency across the codebase.

| Workflow | Description | Branches |
|----------|-------------|----------|
| `check-version.yml` | Runs `bin/check_version.py` to verify version numbers are consistent | all |
| `check-nextflow-version.yml` | Runs `bin/check_nextflow_version.py` to ensure Nextflow version is current | all |
| `check-changelog.yml` | Requires `CHANGELOG.md` update if non-documentation files changed | `dev`, `ci-test` only |

## Release tests

These tests run on PRs to `main`, `stable`, and `ci-test`, and also run automatically on push to `dev`. They are slower or more expensive than development tests and are not required for merging to `dev`, but must pass before merging to `main`.

### Integration test (`test-chained.yml`)

Runs the full pipeline on small test data using `bin/chain_workflows.py`, executing INDEX, RUN, and DOWNSTREAM workflows in sequence.

### Benchmark tests

These tests run the pipeline on larger benchmark datasets to verify performance and correctness at scale.

| Workflow | Dataset |
|----------|---------|
| `benchmark-illumina-100M.yml` | Illumina 100M reads |
| `benchmark-ont-100k.yml` | ONT 100k reads |

### Release readiness check (`check-release.yml`)

Runs only on PRs to `main`. Verifies that:
1. The version in `pyproject.toml` has a corresponding changelog section
2. The version has not already been released on GitHub

This check runs unconditionally (no path filtering).

## Non-test automation

These workflows perform automated actions rather than running tests.

### Create release (`create-release.yml`)

Triggered on push to `main`. Automatically creates a GitHub release and tag based on:
1. The version number extracted from `pyproject.toml`
2. The changelog section for that version extracted by `bin/extract_changelog.py`

### Reset branches (`reset-branches.yml`)

Triggered on push to `main`. After a release is merged, this workflow:
1. Resets `dev` to match `main` (force push)
2. Resets `ci-test` to match `main` (force push)
3. Conditionally resets `stable` to match `main` only for point releases (where the first three version numbers X.Y.Z match)

This uses a GitHub App token for authentication to allow force pushes to protected branches.

### Label issues (`label-issues.yml`)

Triggered when issues are opened. Automatically adds the `repo:mgs-workflow` label to new issues for tracking across the organization.
