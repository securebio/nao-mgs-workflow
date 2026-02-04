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
- Is it out of scope for the current PR?
- Is it a minor stylistic suggestion?

**2. Get user approval:**
Wait for the user to approve your suggested responses before taking any action. The user may modify suggestions or provide different guidance.

**3. Make any required fixes:**
If comments require code changes, implement them first and push the changes.

**4. Respond to comment threads:**
**CRITICAL: Always prefix comments with `[Claude Code]`** to make it clear the response is from Claude Code, not the user. The user's GitHub account is used for these interactions, so clarity is essential.

Response patterns (always include the prefix):
- For implemented fixes: `[Claude Code] Done`
- For out-of-scope suggestions: `[Claude Code] Reasonable suggestion but out of scope`
- For declined minor suggestions: `[Claude Code] Minor stylistic nit, not done`

**5. Resolve concluded threads:**
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
