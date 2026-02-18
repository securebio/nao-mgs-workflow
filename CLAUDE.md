# Claude Code Guidelines

This file contains guidelines for Claude Code when working on this repository.

**Repository:** `securebio/nao-mgs-workflow` on GitHub.

## GitHub Interaction Policies

When interacting with GitHub, prefer `gh` CLI subcommands (e.g., `gh pr view`, `gh issue view`) over raw `gh api` calls where possible — they're simpler and don't require individual user approval.

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

When the user asks you to handle PR review comments:

1. **Summarize and suggest responses** for each comment — categorize as actionable fix, minor nit, or out-of-scope (propose creating a GitHub issue for substantive out-of-scope suggestions).
2. **Wait for user approval** before taking any action.
3. **Implement fixes** and push changes.
4. **Respond to threads.** Always prefix with `[Claude Code]` (e.g., `[Claude Code] Done`) — the user's GitHub account is used, so attribution is essential.
5. **Create issues** for approved out-of-scope suggestions. Issues must be self-contained (they sync to external tools), so quote the original suggestion and explain it fully.

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

**Both of these are required for PRs to `dev` — CI will fail if they're missing.**

- Every PR must include a version bump in `pyproject.toml` and a corresponding update to `CHANGELOG.md`. The topmost CHANGELOG heading must match the version in `pyproject.toml`.
- See `docs/versioning.md` for the versioning scheme and guidance on which version component to increment. See `docs/developer.md` for CHANGELOG formatting conventions.
- Development versions use the `-dev` suffix (e.g. `3.0.1.3-dev`). If the current version is already a `-dev` version, only change it if the new changes justify a larger bump.

### Backwards Compatibility Trackers
`pyproject.toml` contains two compatibility version fields:
- `index-min-pipeline-version`: Minimum pipeline version needed to use indexes built with this version
- `pipeline-min-index-version`: Minimum index version required by this pipeline version

**When to update these:** Only when changes create incompatibilities between the index and RUN/DOWNSTREAM workflows. Most PRs do NOT need to update these. When in doubt, ask the user.

### Schemas
If your changes affect pipeline output files, review the corresponding schema files in `schemas/`. Changes to schema fields beyond `title` and `description` require a schema (2nd-number) version bump. See the Schemas section of `docs/developer.md` for details.

## Maintaining This File

This file should be kept in sync with the repository's code and documentation. When making changes that affect workflows, conventions, or tooling described here, update this file as part of the same PR.

**Before context compaction:** Review the current conversation for user suggestions, workflow patterns, or lessons learned that should be documented here. If the user has provided guidance on preferred workflows or corrections to your approach, consider adding them to CLAUDE.md so future sessions benefit from this context.
