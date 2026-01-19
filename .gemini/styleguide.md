# Style Guide For Code Review for `nao-mgs-workflow`

## Required Review Scope

**IMPORTANT: You MUST review ALL changed files in the PR, not just source code.** This includes:

- **CI/CD files (`.github/` directory)**: Review all workflow changes, action configurations, and CI scripts for correctness, security, and best practices.
- **Documentation (`docs/` folder)**: Review for accuracy, clarity, and completeness relative to code changes.
- **Changelog (`CHANGELOG.md`)**: Verify it has been updated. If not, suggest appropriate additions.

## Review Guidelines

### CI/CD (`.github/`)
- Verify workflow syntax is correct
- Check for security issues (exposed secrets, overly permissive permissions)
- Ensure job dependencies and conditions are logical

### Documentation (`docs/`)
- Ensure documentation reflects any API or behavior changes in the PR
- If code changes affect user-facing behavior and docs are not updated, flag this as an issue

### Changelog
- All PRs should modify `CHANGELOG.md` to summarize the changes
- If missing, suggest concise additions documenting the changes made

### Source Code
- Review for correctness, efficiency, maintainability, and security