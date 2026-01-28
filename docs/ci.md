# Continuous Integration (CI)

We use continuous integration (CI) via Github Actions to perform a number of automated tests on each PR, as well as other non-test actions in certain cases. Our CI workflows are configured in `.github/workflows`.

## Overview

### Types of CI

Our CI workflows fall into three broad categories:

1. **Development tests:** Quick tests that must pass before code can be merged into `dev` or `main`.
2. **Release tests:** Slow or expensive tests that must pass for a PR to be merged into `main`. To enable rapid development, these tests are not required on PRs into `dev`; however, they still run automatically after code has been merged into `dev` to help surface issues before a release.
3. **Non-test automation:** Workflows that perform actions like creating releases, managing branches, or tagging issues.

### Runners and billing

Most of our CI workflows use Github's standard `ubuntu-latest` runner for execution. We have 3,000 free minutes of execution with this runner per month across our entire SecureBio Github account, which is enough for most purposes. This runner is also quite throttled in its [resources](https://docs.github.com/en/actions/reference/runners/github-hosted-runners) and will struggle with demanding tasks.

For tests that demand multiple cores or other substantial resources, we use custom runners set up in the SecureBio Team account. These cost a small amount of money per minute of execution: for example, the `ubuntu-16` runner we use for several tests [costs](https://docs.github.com/en/billing/reference/actions-runner-pricing) about $0.04 per minute. These costs are small enough to generally not be a concern, but developers should take care if adding very long-running tests or doing extensive iterative testing of a new CI workflow.

Several workflows access external resources, including AWS S3 and AWS Batch, and will incur corresponding costs for compute and storage. As above, these costs are usually not large enough to be problematic, but developers should take care around changes that could substantially increase our resource load.

### Conditional execution

Many of our CI tests have two desiderata that are somewhat in tension with each other:

1. We want certain tests to run on every PR and to block merging if they fail; but
2. We also want to avoid running tests pointlessly if the files they test haven't been modified.

The simple approach of just setting run conditions in each CI file fails here; if these conditions aren't met, the test will not run, and the PR will be blocked by our branch protection rules.

To achieve both of these goals, many of our test workflows instead use the [dorny/paths-filter](https://github.com/dorny/paths-filter) action to check whether relevant files have changed. If no relevant files are modified, downstream steps in the workflow are skipped and the workflow succeeds trivially; otherwise, the downstream steps run and the test succeeds or fails based on their outcomes.

### The `ci-test` branch

`ci-test` is a special branch used for testing CI workflows. All checks that would run on PR or merge into `dev` or `main` are also configured to run on PR into `ci-test`. This allows us to test execution of checks that would otherwise be skipped on a PR into `dev`.

If submitting a PR that affects our CI, the recommended process is:

1. Submit a PR to merge your working branch into `ci-test`;
2. Wait for all tests to complete and fix any that fail;
3. Get approval from a reviewer while still on `ci-test`;
4. After making any final changes and checking that all tests pass, switch the destination branch to `dev`;
5. Merge the PR.
