# RFC: Migrate to Trunk-Based Development

## Summary

Replace the current 4-branch model (`dev` / `main` / `stable` / `ci-test`) with trunk-based development: a single `main` branch where all feature PRs land, with releases cut via short-lived release branches. This simplifies the branching model, eliminates post-release branch resets, and reduces contributor onboarding friction — without compromising release quality.

## Motivation

Our current branching model works but creates operational overhead:

- **Two-step release process:** Feature PRs merge to `dev`, then `dev` is periodically merged to `main` to create a release. This means two rounds of review/CI for every release.
- **Post-release branch resets:** After every release, `reset-branches.yml` force-pushes `dev` and `ci-test` to match `main`. This is error-prone and confusing for contributors with in-flight branches.
- **Four branches to understand:** Contributors must learn which branch to target (`dev` for features, `main` for releases, `ci-test` for running full tests, `stable` for long-term support). This is a common source of mistakes.
- **`ci-test` is a workaround:** It exists solely to let developers trigger release tests without opening a PR to `main`. This branch-as-a-test-runner pattern is unusual and hard to explain.

## The Core Constraint

Three release tests take ~2 hours each: `test-chained` (integration), `benchmark-illumina-100M`, and `benchmark-ont-100k`. These run on AWS Batch and cannot be meaningfully sped up. Today, they only run on PRs to `main`/`stable`/`ci-test`, keeping developer velocity high for PRs to `dev`.

**Any new model must preserve this property:** developers should not wait 2+ hours before merging feature PRs, but releases must still pass all tests.

## Proposed Model

### Overview

| Current | Proposed |
|---------|----------|
| `dev` — primary development branch | **Deleted.** `main` takes this role. |
| `main` — release branch | **`main` (trunk).** All feature PRs merge here directly. |
| `stable` — branch, force-pushed to `main` periodically | **`stable` tag.** Points to a specific release. |
| `ci-test` — branch for running all tests | **Deleted.** Replaced by manual workflow dispatch. |

### Feature PRs (day-to-day development)

No change to developer workflow, except the target branch:

1. Branch from `main`, open PR to `main`
2. Fast CI tests run and must pass (nf-test, pytest, mypy, trivy, version/changelog checks — same as today's PRs to `dev`)
3. Release tests **auto-skip** (they detect the branch isn't a `release/` branch and report success immediately)
4. Merge when reviews + CI pass

### Cutting a Release

We adopt the ["Branch for Release" pattern](https://trunkbaseddevelopment.com/branch-for-release/), the canonical approach recommended by trunkbaseddevelopment.com for teams with a weekly release cadence:

1. Maintainer creates `release/X.Y.Z.W` branch from `main`
2. The only change: remove the `-dev` suffix from the version in `pyproject.toml` and the heading in `CHANGELOG.md`
3. Open PR to `main`
4. The `release/` branch name triggers all 3 long-running release tests
5. Tests pass → merge the PR
6. A post-merge workflow detects the non-`-dev` version on `main` and creates the version tag
7. Tag creation triggers the existing `create-release.yml` → GitHub Release with changelog

**If a release test fails:** Fix the issue on `main` via a normal feature PR, rebase the release branch, re-run.

**Merge conflicts:** The release PR only touches ~2 lines. Conflicts can only arise if a feature PR merges during the ~2-hour test window and modifies the same version/changelog lines. Maintainers should hold off merging feature PRs during this window — a soft freeze similar to today's release process. If a conflict does occur, it's trivially resolved by rebasing.

### Running Release Tests On-Demand (Replacing `ci-test`)

Each release test workflow gains a `workflow_dispatch` trigger. Any developer can go to the Actions tab, pick a workflow, and run it against any branch. This is strictly more flexible than the `ci-test` branch (works on any branch, no force-pushing required).

### Stable Releases

`stable` becomes a **tag** instead of a branch. A `workflow_dispatch` workflow lets maintainers move the `stable` tag to any existing release. Quarterly stable updates = run the workflow, enter the target version. Nextflow's `-r` flag works identically with tags and branches, so `nextflow run securebio/nao-mgs-workflow -r stable` continues to work unchanged.

### Catching Regressions Between Releases

Release tests run on a weekly schedule (Saturday 2 AM ET) against `main` to catch regressions early — before a maintainer opens a release PR. On failure, a Slack notification posts to a designated channel with a link to the failed run. The webhook URL is stored as a GitHub Actions secret (encrypted, never exposed to fork PRs or logs — this is a standard pattern for public repos).

---

## CI Workflow Changes

### Release tests: conditional execution

The 3 release test workflows (`test-chained.yml`, `benchmark-illumina-100M.yml`, `benchmark-ont-100k.yml`) are updated to:

```yaml
on:
  pull_request:
    branches: [main]
  workflow_dispatch:
  schedule:
    - cron: '0 7 * * 6'  # Saturday 2 AM ET (7 AM UTC)

jobs:
  the-test:
    # Auto-skip for feature PRs; run for release PRs, manual dispatch, and schedule
    if: >-
      github.event_name == 'workflow_dispatch' ||
      github.event_name == 'schedule' ||
      startsWith(github.head_ref, 'release/')
    ...
    steps:
      ...
      - name: Notify Slack on scheduled failure
        if: failure() && github.event_name == 'schedule'
        uses: slackapi/slack-github-action@v2
        with:
          webhook: ${{ secrets.SLACK_WEBHOOK_URL }}
```

**Why this works with branch protection:** The job always "runs" on every PR — it just instantly skips its steps and reports success for non-release PRs. GitHub sees a passing check. No branch protection rule changes needed.

### `create-release.yml`: trigger on tag instead of push-to-main

Current: `on: push: branches: [main]` (fires on every merge to `main`).
New: `on: push: tags: ['[0-9]+.[0-9]+.[0-9]+.[0-9]+']` (fires only when a version tag is pushed).

Without this change, every feature PR merge would create a GitHub Release.

### New workflow: `tag-release.yml`

Triggers on push to `main`. Extracts the version from `pyproject.toml`; if it lacks a `-dev` suffix and the tag doesn't already exist, creates and pushes the version tag. This bridges the release PR merge to the tag-triggered `create-release.yml`. Small workflow, ~20 lines of logic.

### New workflow: `update-stable-tag.yml`

A `workflow_dispatch` workflow that moves the `stable` tag to a specified release. Takes a version string as input, verifies the release exists, and force-pushes the tag. Replaces `manual-reset.yml`.

### `check-version.py`: simplified rules

Current logic checks the base branch (`main`, `stable`, or `dev`) to determine `-dev` requirements. New logic only checks the head branch:
- `release/*` branches → must NOT have `-dev`
- All other branches → MUST have `-dev`

The base branch check becomes irrelevant since it's always `main`.

### Workflows to delete

| Workflow | Reason |
|----------|--------|
| `reset-branches.yml` | No `dev` or `ci-test` branches to reset |
| `manual-reset.yml` | Replaced by `update-stable-tag.yml` |

### Other workflow updates

| Workflow | Change |
|----------|--------|
| `check-index-age.yml` | Remove `stable` from trigger branches |
| `check-changelog.yml` | Remove `ci-test` from branch conditions if present |
| `rust-tools.yml` | Remove `dev` from push branches; only push ECR containers on `main` |
| `setup-rust-container` action | Remove `origin/dev` fallback; only check `origin/main` |

---

## Versioning

The 4-number versioning scheme and `-dev` suffix convention are unchanged:

1. **After release `3.2.1.0` merges:** `main` has version `3.2.1.0` (no `-dev`)
2. **First feature PR after release:** Bumps version to `3.2.1.1-dev` (or higher if warranted)
3. **Subsequent feature PRs:** Keep `3.2.1.1-dev` or bump if their changes justify a higher-level bump
4. **Release PR (`release/3.2.1.1`):** Removes `-dev`, triggering release tests
5. **After merge + tag:** Cycle repeats

The first feature PR after a release is responsible for adding the `-dev` suffix. This is the same coordination pattern as today, and `check-version.yml` enforces it in CI.

---

## What Gets Simpler

1. **One branch to think about** — always branch from `main`, PR to `main`
2. **One-step releases** — single release PR instead of two (feature→dev, then dev→main)
3. **No branch resets** — `reset-branches.yml` deleted entirely
4. **No `ci-test` branch** — `workflow_dispatch` is more flexible and requires no branch management
5. **No dev/main divergence** — eliminates a class of merge conflicts that occur during the dev→main merge
6. **Simpler Rust container management** — one ECR tag (`:main`) instead of two (`:dev` + `:main`)
7. **Less documentation** — fewer branching rules to explain to contributors

## What Gets Slightly More Complex

1. **Branch-conditional CI** — 3 release test workflows gain an `if` condition based on head branch name
2. **Post-merge tag creation** — new `tag-release.yml` workflow (~20 lines)
3. **Version state between releases** — `main` briefly has a non-`-dev` version right after a release until the next feature PR merges

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Release tests rarely run, regressions accumulate silently | Weekly scheduled test runs on `main` with Slack notification on failure |
| First PR after release must bump to `-dev` (coordination) | CI enforces this; same pattern as today |
| External tools/scripts reference `dev` branch | Keep `dev` as a read-only alias pointing at `main` for 2-4 weeks; grep codebase for all references before deletion |
| Accidental tag push creates unintended release | Use GitHub tag protection rules to restrict who can push version tags |
| Someone names a branch `release/` for a non-release purpose | Convention + code review; low risk in practice |
| Merge conflicts on release PR during the test window | Soft freeze on feature PR merges during the ~2-hour window, similar to today's release process |

---

## Migration Plan

1. **Communicate** the change to all contributors ahead of time
2. **Final release under the old model** — merge any pending work from `dev` to `main`
3. **Single migration PR to `main`** implementing all CI workflow changes described above
4. **Update documentation** — `CLAUDE.md`, `docs/developer.md`, `docs/versioning.md`, `docs/ci.md`
5. **Create `stable` tag** pointing at the current `main` HEAD
6. **Delete branches** — `ci-test` immediately; `stable` branch after the tag is created; `dev` after a 2-4 week grace period

---

## Documentation Updates Required

- `CLAUDE.md`: Change all references from "branch from `dev`" / "PR to `dev`" to `main`. Remove `ci-test` references. Update `--base dev` to `--base main`.
- `docs/developer.md`: Rewrite branching and release sections.
- `docs/versioning.md`: Update references to `master`/`dev` branches. Clarify `-dev` suffix rules for the new model.
- `docs/ci.md`: Rewrite to reflect new trigger model. Remove `ci-test` section. Add scheduled test and release test sections.
