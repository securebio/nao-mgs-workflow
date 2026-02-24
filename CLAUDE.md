# Claude Code Guidelines

This file contains guidelines for Claude Code when working on this repository.

**Repository:** `securebio/nao-mgs-workflow` on GitHub.

## GitHub Interaction Policies

Keep shell commands simple and direct so they can be auto-approved. Avoid complex piped commands, `gh api` calls, or multi-step one-liners when a simpler alternative exists. For GitHub operations, prefer `gh` CLI subcommands (e.g., `gh pr view`, `gh issue view`) over raw `gh api` calls. For the same reason, run git commands directly (e.g. `git add ...`), not with `-C` paths — unless otherwise specified by the user, assume the working directory is already the repo root.

### Branching and PR Targets

- **Create new branches from `dev`** (not `main`), unless the user specifically instructs otherwise (e.g. for stacked PRs)
- **PRs should target `dev`** (not `main`), unless the user specifically instructs otherwise
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
1. **Prose summary**: One or two sentences explaining the core problem or motivation, oriented toward a reviewer who hasn't seen the code yet. Focus on *why* the change is needed and the key design decision, not just *what* changed
2. **Changes list**: Bulleted list of specific changes with their justifications
3. **Footer**: Always include the "Generated with Claude Code" attribution

**Before creating the PR**, check whether your changes require documentation updates. If you modified behavior, added features, or changed workflows, update the relevant docs (e.g. `docs/`, `CLAUDE.md`) in the same PR — don't leave documentation for a follow-up.

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

When analyzing a PR's scope (e.g. for splitting), always check the actual base branch with `gh pr view` rather than assuming `dev` — stacked PRs target other feature branches.

### Responding to Reviewers

When the user asks you to handle PR review comments:

1. **Summarize and suggest responses** for each comment — categorize as actionable fix, minor nit, or out-of-scope (propose creating a GitHub issue for substantive out-of-scope suggestions).
2. **Wait for user approval** before taking any action.
3. **Implement fixes** and push changes.
4. **Respond to threads.** Always prefix with `[Claude Code]` (e.g., `[Claude Code] Done`) — the user's GitHub account is used, so attribution is essential.
5. **Create issues** for approved out-of-scope suggestions. Issues must be self-contained (they sync to external tools), so quote the original suggestion and explain it fully.

### Checking CI Status

When working on changes to an existing PR branch, proactively check CI status with `gh pr checks` to identify test failures, timeouts, or version-check errors. Don't wait for the user to point out failures — catch and address them as part of your workflow.

### GitHub Actions Workflows

When writing or modifying GitHub Actions workflows:
- **Use local composite actions** from `.github/actions/` instead of standard actions where available. For example, use `./.github/actions/setup-python` (which handles pip caching and project dependency installation) rather than `actions/setup-python@v5` directly. Check `.github/actions/` for existing reusable actions before writing setup steps inline.
- **Never interpolate `${{ }}` expressions directly in `run:` scripts** — this is a script injection vector. Always pass them through `env:` variables instead (e.g., `env: CONFIRM: ${{ inputs.confirm }}` then use `$CONFIRM` in the script).
- Add explicit `permissions` blocks to workflows to document intent and follow least privilege (e.g., `permissions: contents: write`).

### Committing Changes

Follow the repository's standard commit practices from `docs/developer.md`:
- Use descriptive commit messages
- Include `Co-Authored-By: Claude <model> <noreply@anthropic.com>` (with the actual model name, e.g. `Claude Opus 4.6`) for commits authored with Claude Code
- Stage specific files rather than using `git add -A`
- Run git commands directly (e.g. `git add file`, not `git -C /path git add file`) — the working directory is already the repo root.
- Before committing, verify you're on the expected branch with `git branch --show-current` to avoid committing code to the wrong branch.

### Resolving Merge Conflicts

When asked to resolve merge conflicts:

1. **Summarize the conflicts** — for each conflicted file, describe the differences between the two sides and recommend a resolution.
2. **Wait for user approval** before resolving.
3. When the chosen resolution is to take one side of a conflict entirely (for a given file or the whole merge), prefer git-native resolution commands (e.g., `git checkout --ours <file>` or `git checkout --theirs <file>`) over manually rewriting affected files.

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
- When the user or a linter modifies a file between your edits, preserve those changes — never revert formatting the user has applied

### Python Style

For all Python scripts, follow the patterns established in `bin/build_tiny_test_databases.py`:

- Flexible `open_by_suffix()` pattern for handling files that may or may not be compressed
- Use Python 3.12+ native type hints, not the `typing` module (e.g. `list[str]` instead of `List[str]`, etc)
- Logging with the `logging` standard library and `UTCFormatter` class
- `parse_arguments()` function for argparse
- `main()` entry point with timing and logging. `main()` should raise exceptions on failure (not return exit codes). Use a bare `main()` call under `if __name__ == "__main__"`, not `sys.exit(main())`
- `DESC` docstring at the top describing the script's purpose
- Google-style docstrings with `Args:` and `Returns:` sections for functions
- Use context managers (`with` statements) instead of try/finally where appropriate
- Section headers with `###` dividers, e.g.:

```
###########
# IMPORTS #
###########
```

All Python scripts should have corresponding Pytest scripts in the same directory (`**/script.py` -> `**/test_script.py`). See `docs/testing.md` for general pytest conventions. Additional guidance for Claude Code:

- The order of tests should match the order of functions/methods in the source script.
- Use `pytest.mark.parametrize` for tests that share the same structure but differ in inputs/expected outputs. After writing tests, always review them again to identify and remove unnecessary redundancy.
- Distinguish clearly between unit tests of high-level functions (which can be useful even if heavily mocked) and integration tests of the whole code stack (which should keep mocks to a minimum).
- Use `@patch` decorators (including class-level decorators for shared mocks) rather than `with patch(...)` context managers.

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

When creating new schemas, always include `primaryKey` and `example` fields on every schema and field respectively, matching the conventions in existing schemas (e.g. `read_counts.schema.json`).

## Maintaining This File

This file should be kept in sync with the repository's code and documentation. When making changes that affect workflows, conventions, or tooling described here, update this file as part of the same PR.

**Before context compaction:** Review the current conversation for user suggestions, workflow patterns, or lessons learned that should be documented here. If the user has provided guidance on preferred workflows or corrections to your approach, consider adding them to CLAUDE.md so future sessions benefit from this context.
