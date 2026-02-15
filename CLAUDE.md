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
gh pr create --base dev --draft --assignee @me --title "Brief descriptive title" --body "$(cat <<'EOF'
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
- Always use `--draft` (PRs start as drafts for self-review before requesting human review)
- Use `--assignee @me` to assign the PR to its creator for tracking
- Keep titles under 70 characters

### Stacked PRs

When decomposing large feature branches into smaller PRs:
1. Create PRs that build on each other: PR2 targets PR1's branch, PR3 targets PR2's branch, etc.
2. Document the dependency chain in PR descriptions
3. To bring specific files from a source branch, use `git checkout feature/source-branch -- path/to/file.nf path/to/other/file.py`

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
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          comments(first: 100) {
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

## Testing

See `docs/testing.md` for comprehensive testing guidelines. Key commands:

```bash
bin/run-nf-test.sh tests/modules/local/kraken/main.nf.test  # Run a specific test
bin/run-nf-test.sh tests/subworkflows/local/qc/              # Run all tests in a directory
bin/run-nf-test.sh tests/                                     # Run all tests
```

When snapshot tests fail, **always verify the output changes are intentional** before updating snapshots. See the "Updating snapshots" section in `docs/testing.md` for the full procedure.

## Code Style

Refer to `docs/developer.md` for comprehensive coding style guidelines. Key points:
- Nextflow: `lower_snake_case` for variables, `UPPER_SNAKE_CASE` for processes
- Keep PRs small and focused
- Avoid over-engineering; only make requested changes

## Versioning and Changelog

**Both of these are required for PRs to `dev` — CI will fail if they're missing.**

- Every PR must include a version bump in `pyproject.toml` and a corresponding update to `CHANGELOG.md`. The topmost CHANGELOG heading must match the version in `pyproject.toml`.
- See `docs/versioning.md` for the versioning scheme and guidance on which version component to increment. See `docs/developer.md` for CHANGELOG formatting conventions.
- Development versions use the `-dev` suffix (e.g. `3.0.1.3-dev`). If the current version is already a `-dev` version, only change it if the new changes justify a larger bump.

### Backwards Compatibility Trackers
`pyproject.toml` contains two compatibility version fields:
- `index-min-pipeline-version`: Minimum pipeline version needed to use indexes built with this version
- `pipeline-min-index-version`: Minimum index version required by this pipeline version

**When to update these:** Only when changes create incompatibilities between the index and RUN/DOWNSTREAM workflows. Most PRs do NOT need to update these. When in doubt, ask the user.

## Maintaining This File

This file should be kept in sync with the repository's code and documentation. When making changes that affect workflows, conventions, or tooling described here, update this file as part of the same PR.

**Before context compaction:** Review the current conversation for user suggestions, workflow patterns, or lessons learned that should be documented here. If the user has provided guidance on preferred workflows or corrections to your approach, consider adding them to CLAUDE.md so future sessions benefit from this context.
