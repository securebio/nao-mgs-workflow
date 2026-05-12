---
name: prepare-release
description: Cut a release PR into dev. Reads dev's accumulated `-dev` CHANGELOG entries, classifies the overall bump level per `docs/versioning.md`, rewrites the entries as a polished release note (mirroring v3.2.1.0 / v3.2.1.3's structure — grouped by `## New workflow outputs` / `## Cleanup and best practice` / `## Bugfixes` / etc., action-oriented, user-facing, with PR references), updates `pyproject.toml` + `CHANGELOG.md`'s top heading to drop `-dev` and reflect any higher-level bump, and opens a `release/<user>/<X.Y.Z.W>` PR into dev. This is the maintainer's first step in the release process described in `docs/developer.md`.
---

# Prepare a release PR into dev

For the **maintainer-side step 2** of the release process (`docs/developer.md` → "New releases"): create a `release/<user>/<X.Y.Z.W>` branch, consolidate and rewrite the `-dev` CHANGELOG entries into a release note, lock in the version, open the PR.

This skill **does not merge anything**. It opens a `release/<handle>/<version>` PR into dev as a draft and stops there for human review. The subsequent dev → main release PR is a separate maintainer step taken after this PR merges (see `docs/developer.md` § "New releases" step 3).

Index-version changes are **out of scope** for this skill. Most releases don't need one; when one does, the maintainer takes the additional steps in `docs/developer.md` § "New releases" step 2.3 (update `index-min-pipeline-version` / `pipeline-min-index-version` and kick off the rebuild-benchmark-index workflow) by hand, before or alongside this skill's output.

## When to use

- Maintainer is ready to cut a release and wants the boilerplate (branch creation, CHANGELOG cleanup, version bump, PR open) done in one pass.
- The agent has dev's full CHANGELOG-`-dev` history visible and can classify it correctly.

If you're updating the version mid-stream (a PR that promotes the `-dev` from one bump level to a higher one), use the `version-bump` agent instead — that's a different operation, and `version-bump` has lighter scope.

## Inputs

- `user_handle` (optional): the GitHub handle for the branch name (`release/<user_handle>/<version>`). If omitted, default to `coding-agent` when running as the `securebio-coding-agent` App (detect via `[ "$(git config user.name)" = "securebio-coding-agent" ]`); for human-authored releases, derive via `gh api user --jq .login`. Note: `gh api user` returns 403 when authenticated as a GitHub App, so don't fall back to it from agent contexts — hardcode `coding-agent`. The convention is "who pushed the release branch".
- `bump_override` (optional): one of `major`, `schema`, `results`, `point` to force a specific bump level. If omitted, classify per Step 2 below and pick the largest. When the agent's classification disagrees with the user-provided override, surface the disagreement before applying.

## Procedure

### Step 1 — Read dev's current state

- Current version from `pyproject.toml` (`[project] version`). Should be `X.Y.Z.W-dev`.
- The topmost `CHANGELOG.md` heading (must match the pyproject version).
- Every bullet under that heading, including sub-bullets.

The bullets are the raw material. Save them to a scratch buffer before rewriting.

### Step 2 — Classify each bullet by bump level

Read `docs/versioning.md` first (the four-number scheme: Major / Schema / Results / Point). For each bullet, decide which level its change falls under:

| Level | Trigger criteria — examples |
|---|---|
| **Major** (1st number) | Substantial pipeline rework requiring major downstream code changes. Almost never in a normal release. |
| **Schema** (2nd number) | Renames or removes files from `[tool.mgs-workflow] expected-outputs-*` in `pyproject.toml`, OR changes to `schemas/*.schema.json` beyond `title` / `description` fields. Use `git diff <last-release-tag>..origin/dev -- pyproject.toml schemas/` to spot these. **Tags are unprefixed** (`3.2.1.4`, not `v3.2.1.4`) even though `CHANGELOG.md` headings carry the `v` prefix — `git tag -l "3.*" | tail` to find the most recent. |
| **Results** (3rd number) | Results no longer comparable to previous versions — e.g. swapping an aligner, changing a threshold that affects which reads pass a filter, changing read content semantics. The bullet itself usually flags this ("Replace BBDuk with Nucleaze; the match-count drop varies per sample"). |
| **Point** (4th number) | Everything else: bugfixes, perf (results-preserving), CI / tooling / docs, off-by-default options, *additions* to `expected-outputs-*` lists, schema `title`/`description` updates. |

The overall release bump is the **maximum** classification across all bullets. If even one bullet is a Results change, the release is at least Results.

When unsure, escalate to the user with the specific bullet and the classification question — don't guess on Schema or Results without confirmation, since under-classifying breaks downstream compatibility tracking.

### Step 3 — Compute the target version

Starting from the current `X.Y.Z.W-dev`:

| Final bump level | New version |
|---|---|
| Point | `X.Y.Z.W` (drop the `-dev` suffix only) |
| Results | `X.Y.(Z+1).0` |
| Schema | `X.(Y+1).0.0` |
| Major | `(X+1).0.0.0` |

The `-dev` was already a notional Point increment from the last release; higher bumps reset the lower components to zero.

### Step 4 — Rewrite the CHANGELOG entry

This is the high-value part. The raw `-dev` bullets accumulated organically across many small PRs and are usually:

- Too long (paragraph-shaped instead of bullet-shaped).
- Implementation-detail-leaning ("Extended `ADD_FIXED_COLUMN` to accept comma-separated column names") rather than outcome-leaning ("ONT and short-read validation hits now share the same schema and column set").
- Ungrouped — unrelated changes interleaved.

Read v3.2.1.0 and v3.2.1.3 in `CHANGELOG.md` as exemplars of the target structure. Both group bullets under `##` subheadings and lead with user-facing outcomes.

**Grouping.** Pick subheadings based on what's actually in the bullets. Common ones, in roughly this order when present:

- `## New workflow outputs` — additions to published RUN/DOWNSTREAM outputs, new schemas, new pipeline steps that surface to users.
- `## Performance` — perf-only changes (use when there are several; otherwise fold into Cleanup).
- `## Bugfixes` — straight bug fixes.
- `## Cleanup and best practice` — refactors, CI, security/CVE updates, tooling, documentation, internal consolidation.
- `## Coding agents` — agent/skill/scripts additions under `.claude/`.

Don't reach for a subheader for a single bullet — flat list is fine when the release is small (e.g. v3.2.1.4 is unsubheadered, six bullets). The 3.2.1.3 / 3.2.1.0 pattern is for releases with enough volume that grouping helps the reader.

**Rule of thumb for `##` grouping:** use subheaders only when (a) total bullets ≥ 8, AND (b) at least one prospective section would hold ≥ 3 bullets. Below that, a flat list reads better than near-empty sections.

**Bullet rewriting.** For each raw bullet:

- Start with a verb: `Add ...`, `Fix ...`, `Replace ...`, `Update ...`, `Switch ...`, `Bump ...`, `Remove ...`.
- Lead with the user-facing outcome. Implementation details, file paths, function names go after the outcome or in a sub-bullet.
- Add `(#NNN)` **at the end of the bullet** (after the closing period), not embedded mid-sentence. For multi-PR bullets, list as `(#NNN, #MMM, #...)` at the end.
- Trace PR numbers via `git log <last-release-tag>..origin/dev --oneline` (unprefixed tag, e.g. `3.2.1.4..origin/dev`) to find the merge SHA and PR number for each `-dev` bullet.
- **Be aggressive about consolidation.** A perf PR that also adds container CVE waivers, a primary PR plus its companion follow-up, multiple CVE-waiver PRs against the same theme — all of these are *one* release-note bullet with their PR numbers listed at the end, not several. The `-dev` log accreted in PR-author chunks; the release entry reads better when grouped by user-facing theme. (Exemplar: v3.2.1.5 collapses five trivyignore PRs into one Cleanup bullet, and folds a check_version.py extension into the same bullet as the skill that drove it.)
- When a single PR is worth surfacing in two layers (primary outcome + a mechanism worth naming), use indented sub-bullets under the parent bullet. Don't use sub-bullets just to split a single conceptual change across multiple PRs — that's prose-shaped, not bullet-shaped.
- Cut implementation churn that doesn't survive the release: e.g. a `-dev` bullet describing a refactor of a function that was itself replaced before the release is just noise.
- Indent sub-bullets at 4 spaces (the repo's existing convention).

Don't lose information that affects users. A bullet that documents a behavior change (e.g. a default flipping) stays; a bullet that documents a renamed internal helper goes away.

### Step 5 — Apply the changes

- Update `pyproject.toml`'s `[project] version` to the target version.
- Replace the `# vX.Y.Z.W-dev` heading in `CHANGELOG.md` with `# vX.Y.Z.W` (matching the new pyproject version), then replace the bullet block with the rewritten one.
- Run `python bin/check_version.py` to verify pyproject ↔ CHANGELOG consistency.
- Don't touch any other files. The release PR's diff should be exactly `CHANGELOG.md` + `pyproject.toml`. Index-version updates, reference-DB config changes, bug fixes — all out of scope; handle separately if the release needs them.

### Step 6 — Branch and PR

```bash
git fetch origin dev --quiet
HANDLE="<user_handle>"   # see Inputs; default 'coding-agent' from agent context
VERSION="<X.Y.Z.W>"      # from Step 3

# Branch name: documented convention is release/<handle>/<version>.
# The securebio-coding-agent GitHub App pushes under coding-agent/* per
# the org's branch ruleset; the ruleset exempts coding-agent/release/*
# from the release/* creation restriction. `bin/check_version.py`
# recognizes both prefixes as release branches.
if [ "$HANDLE" = "coding-agent" ]; then
  BRANCH="coding-agent/release/${VERSION}"
else
  BRANCH="release/${HANDLE}/${VERSION}"
fi

git checkout -b "$BRANCH" origin/dev
git add CHANGELOG.md pyproject.toml
git commit -m "Prepare ${VERSION} for release"
git push -u origin "$BRANCH"

gh pr create --base dev --draft \
  --title "Prepare ${VERSION} for release" \
  --body "$(cat <<'EOF'
...release PR body...
EOF
)"
```

**PR body shape.** Past release PRs (#701, #707, #733, #746) ship with **empty bodies** — the CHANGELOG diff says everything. Follow that default. Do **not** paste the rewritten CHANGELOG into the body; it duplicates the diff.

Include body content only when there's something the diff doesn't carry:

- A bump classification that required judgment (Schema-vs-Point edge case, INDEX-only changes treated as Point, etc.) — flag for maintainer review in 2-3 lines.
- A branch-name divergence from the documented convention (e.g. the `coding-agent/release/<version>` form) — one sentence noting the ruleset reason so the maintainer knows why the path doesn't match.

If neither applies, leave the body empty (`gh pr create ... --body ""`). When body content is needed, append the `Generated with Claude Code` footer per `CLAUDE.md` § "Creating Pull Requests"; otherwise skip it.

**Stop after opening the draft PR.** Don't squash, merge, or open downstream PRs. The maintainer reviews this draft, marks ready-for-review when satisfied, and (per `docs/developer.md` step 3.1) squash-merges it into dev themselves. The next-stage dev → main release PR is also a separate maintainer step.

## Anti-patterns

- **Verbatim copy of `-dev` bullets into the release entry.** The rewrite is the whole value of this step. Mid-stream `-dev` bullets accrete in PR-author voice; release notes should be in pipeline-user voice.
- **Under-classifying the bump level.** Schema and Results bumps are reviewer-blocking commitments to downstream users; getting them wrong is worse than over-classifying. When in doubt, escalate.
- **Skipping the PR-number cross-reference.** `(#NNN)` lets a reader who hits a regression find the change. The bullets are easier to audit later when each is traceable to a merge.
- **Mixing release-prep with unrelated changes.** This PR should touch *exactly* `CHANGELOG.md` and `pyproject.toml`, nothing else. If a bug needs fixing as part of the release, fix it in a separate PR into dev first, then rebase the release branch. Index-version updates, reference-DB config bumps — same rule.
- **Merging or progressing the release past the draft PR.** The skill stops at "draft PR opened"; the maintainer takes it from there. Don't squash-merge, don't open dev → main, don't ship.
- **Picking up a release branch name that doesn't include the maintainer's handle.** `release/<handle>/<version>` is the documented convention; the handle attribution matters for who owns the release.

## Cross-references

- `docs/versioning.md` — bump-level criteria, the 4-number scheme, `-dev` semantics.
- `docs/developer.md` § "Sending PRs for review" — CHANGELOG entry conventions (action-oriented, user-facing, PR-numbered).
- `docs/developer.md` § "New releases" — the full release process; this skill implements step 2.
- `.claude/agents/version-bump.md` — the lighter-weight agent for mid-stream version bumps; *not* for cutting releases.
- `CHANGELOG.md` v3.2.1.0 and v3.2.1.3 entries — the structural exemplars for rewritten release notes.
- `bin/check_version.py` — validates pyproject ↔ CHANGELOG consistency.
