# Claude Code Guidelines

This file contains guidelines for Claude Code when working on this repository.

## GitHub Interaction Policies

### Branching and PR Targets

- **Always create new branches from `dev`** (not `main`)
- **PRs should target `dev` by default** (not `main`)
- Only maintainers merge `dev` to `main` for releases

### Creating Pull Requests

Use `gh pr create` with a HEREDOC for the body to ensure proper formatting:

```bash
gh pr create --base dev --title "Brief descriptive title" --body "$(cat <<'EOF'
Short prose summary explaining what this PR does and why.

**Changes:**
- Specific change 1: justification
- Specific change 2: justification
- Specific change 3: justification

Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

**PR description structure:**
1. **Prose summary**: A brief paragraph explaining the purpose and context
2. **Changes list**: Bulleted list of specific changes with their justifications
3. **Footer**: Always include the "Generated with Claude Code" attribution

**Important options:**
- Always use `--base dev` (PRs target `dev`, not `main`)
- Use `--assignee @me` to assign the PR to its creator for tracking
- Keep titles under 70 characters

### Stacked PRs

When decomposing large feature branches into smaller PRs:
1. Create PRs that build on each other: PR2 targets PR1's branch, PR3 targets PR2's branch, etc.
2. Document the dependency chain in PR descriptions
3. Update snapshot files by checking hashes from the full feature branch when needed

### Responding to Reviewers

When the user asks you to handle PR review comments, follow this workflow:

**1. Review all comments and suggest responses:**
Fetch and analyze all review comments, then present the user with a summary and your suggested response for each:
- Is it a valid concern that should be addressed?
- Is it a minor stylistic suggestion?
- Is it a reasonable, substantive suggestion that's out of scope? If so, propose creating a GitHub issue to track it.

**2. Get user approval:**
Wait for the user to approve your suggested responses before taking any action. The user may modify suggestions or provide different guidance. If you proposed creating issues for out-of-scope suggestions, confirm which ones the user wants created.

**3. Make any required fixes:**
If comments require code changes, implement them first and push the changes.

**4. Respond to comment threads:**
**CRITICAL: Always prefix comments with `[Claude Code]`** to make it clear the response is from Claude Code, not the user. The user's GitHub account is used for these interactions, so clarity is essential.

Response patterns (always include the prefix):
- For implemented fixes: `[Claude Code] Done`
- For out-of-scope suggestions: `[Claude Code] Reasonable suggestion but out of scope`
- For out-of-scope suggestions with issue created: `[Claude Code] Reasonable suggestion but out of scope for this PR. Opened #<issue_number> to track.`
- For declined minor suggestions: `[Claude Code] Minor stylistic nit, not done`

**5. Create issues for substantive out-of-scope suggestions:**
If the user approved creating issues, use `gh issue create` before responding to the thread. Issues must be self-contained (they sync to external project management tools), so quote the suggestion and explain it fully rather than just linking to the PR:
```bash
gh issue create --title "Brief description" --body "$(cat <<'EOF'
<Full explanation of the suggested improvement>

**Original suggestion from PR #<number> review:**
> <quoted suggestion text>

EOF
)"
```

**6. Resolve concluded threads:**
After responding, resolve each thread using the GraphQL API:

```bash
# Get thread IDs
gh api graphql -f query='
{
  repository(owner: "naobservatory", name: "mgs-workflow") {
    pullRequest(number: <PR_NUMBER>) {
      reviewThreads(first: 10) {
        nodes {
          id
          isResolved
          comments(first: 5) {
            nodes {
              id
              body
            }
          }
        }
      }
    }
  }
}'

# Reply to a thread
gh api graphql -f query='
mutation {
  addPullRequestReviewThreadReply(input: {
    pullRequestReviewThreadId: "<THREAD_ID>",
    body: "[Claude Code] Done"
  }) {
    comment { id }
  }
}'

# Resolve the thread
gh api graphql -f query='
mutation {
  resolveReviewThread(input: {
    threadId: "<THREAD_ID>"
  }) {
    thread { isResolved }
  }
}'
```

### Committing Changes

Follow the repository's standard commit practices from `docs/developer.md`:
- Use descriptive commit messages
- Include `Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>` for commits authored with Claude Code
- Stage specific files rather than using `git add -A`

### Checking Out Files from Other Branches

When decomposing branches, use `git checkout` to bring specific files from the source branch:

```bash
git checkout feature/source-branch -- path/to/file.nf path/to/other/file.py
```

This is more efficient than manually recreating changes and ensures consistency.

## Testing

### Running Tests

Use the wrapper script for nf-test. It accepts specific test files or path prefixes:
```bash
# Run a specific test file
bin/run-nf-test.sh tests/modules/local/kraken/main.nf.test

# Run all tests in a directory
bin/run-nf-test.sh tests/subworkflows/local/qc/

# Run all tests
bin/run-nf-test.sh tests/
```

### Updating Snapshots When Output Changes

When workflow output changes (new files, renamed files, or changed content), three things must be updated:

**1. Update expected outputs in `pyproject.toml`:**
The `[tool.mgs-workflow]` section lists expected output files for RUN and DOWNSTREAM workflows. Update these lists when files are added, removed, or renamed.

**2. Update snapshot files in `tests/`:**
```bash
bin/run-nf-test.sh tests/workflows/run.nf.test --update-snapshot
```
This updates the `.snap` files (e.g., `tests/workflows/run.nf.test.snap`) with new MD5 hashes.

**3. Update result files in `test-data/results/`:**
Copy the new output files from the nf-test working directory and decompress them:
```bash
# Find the test output directory (hash shown in test output)
ls .nf-test/tests/<hash>/output/

# Copy changed files to test-data/results/
cp .nf-test/tests/<hash>/output/results/<file>.tsv.gz test-data/results/run-short/

# Decompress for version control (we store uncompressed in repo)
cd test-data/results/run-short/
gunzip <file>.tsv.gz
```

**For stacked PRs:** When decomposing a feature branch, you can reference MD5 hashes from the full feature branch's snapshots to update dependent PR snapshots without re-running all tests.

## Code Style

Refer to `docs/developer.md` for comprehensive coding style guidelines. Key points:
- Nextflow: `lower_snake_case` for variables, `UPPER_SNAKE_CASE` for processes
- Keep PRs small and focused
- Avoid over-engineering; only make requested changes

### Python Style

For all Python scripts, follow the patterns established in `bin/build_tiny_test_databases.py`:

- Flexible `open_by_suffix()` pattern for handling files that may or may not be compressed
- Use Python 3.12+ native type hints, not the `typing` module (e.g. `list[str]` instead of `List[str]`, etc)
- Logging with the `logging` standard library and `UTCFormatter` class
- `parse_arguments()` function for argparse
- `main()` entry point with timing and logging
- `DESC` docstring at the top describing the script's purpose
- Use context managers (`with` statements) instead of try/finally where appropriate
- Section headers with `###` dividers, e.g.: 

```
###########
# IMPORTS #
###########
```

All Python scripts should have corresponding Pytest scripts in the same directory (`**/script.py` -> `**/test_script.py`). When writing tests:

- Make sure all functions and methods (except for argument parsing and logging) have at least basic test coverage.
- The order of tests should match the order of functions/methods in the source script.
- Use `@pytest.mark.parametrize` wherever possible to minimize redundancy between tests. After writing tests, always review them again to identify and remove unnecessary redundancy.
- Distinguish clearly between unit tests of high-level functions (which can be useful even if heavily mocked) and integration tests of the whole code stack (which should keep mocks to a minimum).

## Versioning and Changelog

**Both of these are required for PRs and CI will fail if they're missing.**

### Version bumping
- Every released change to the pipeline must be accompanied by a version bump in `pyproject.toml`. See `docs/versioning.md` for the versioning scheme and guidance on which version component to increment.
- If the current version in `pyproject.toml` is a non-development version (e.g. `A.B.C.D`), you should update it to a new development version (`X.Y.Z.W-dev`) before merging to `dev`. The numerical part of the new version should reflect what the version *will be* once a release is made.
  - For example, if the current version is `3.0.1.2` and the changes you want to merge amount to a point release, you should change the version number to `3.0.1.3-dev`. If they amount to a schema release, you should change it to `3.1.0.0-dev`. In both cases, the `-dev` suffix will be removed during the release process.
- If the current version is already a development version, you should only change it if the changes you want to merge would justify a larger version bump than the one currently planned.
  - For example, if the current version is `3.0.1.3-dev`, and the changes you want to merge amount to a point release, then the version should stay as-is. If instead they amount to a results release, you should change it to `3.0.2.0-dev`.
- At the time a release is made, the only change needed to the version number should be the removal of the `-dev` suffix. As such, the new version number should always reflect the largest changes made since the last release.

### CHANGELOG.md
- For a PR into `dev` to pass CI, it must include updates to `CHANGELOG.md`, and the topmost heading in that file must match the version in `pyproject.toml`.
- If the topmost heading in the changelog is for a non-development version, create a new entry for the development version in which to put updates. Do not change any entry for a non-development version.
- If the topmost heading is for a development version, you should (1) update it according to the same procedure outlined for the `pyproject.toml` version, then (2) add new suggested changes to that entry.
- The CHANGELOG should never contain entries for multiple development versions at once.

### Backwards Compatibility Trackers
`pyproject.toml` contains two compatibility version fields:
- `index-min-pipeline-version`: Minimum pipeline version needed to use indexes built with this version
- `pipeline-min-index-version`: Minimum index version required by this pipeline version

**When to update these:** Only when changes create incompatibilities between the index and RUN/DOWNSTREAM workflows. Most PRs do NOT need to update these. Examples requiring updates:
- Changes to index file structure or naming
- Changes to how RUN/DOWNSTREAM consume index files
- New required index components

When in doubt, ask the user.

## Maintaining This File

**Before context compaction:** Review the current conversation for user suggestions, workflow patterns, or lessons learned that should be documented here. If the user has provided guidance on preferred workflows or corrections to your approach, consider adding them to CLAUDE.md so future sessions benefit from this context.
