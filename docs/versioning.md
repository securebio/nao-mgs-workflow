# Pipeline versioning

From version 2.6.0.0 we're adopting a new 4-number versioning scheme, described below. The primary purpose of this system is to communicate clearly to users of the pipeline what changes they must make when interpreting or using the outputs of the pipeline in downstream applications

1. **Major:** The first number in the version will be incremented each time we seriously rework the pipeline, requiring potentially major changes to downstream code.
2. **Schema:** The second number will be incremented each time we restructure or rename pipeline outputs, requiring downstream code to be changed to correctly access output files. This includes any changes to `schemas/*.schema.json` files beyond `title` and `description` fields (which are documentation-only and permitted in point releases).
3. **Results:** The third number will be incremented each time the results are no longer directly comparable to previous versions.
4. **Point:** The fourth number will be incremented any other time the pipeline code changes in a manner that doesn't meet the criteria above, such as changes that impact performance but not results; changes to documentation; options that are off by default; and new outputs that don't interfere with existing outputs.

Users relying on pipeline outputs should take the following actions in response to pipeline changes:

1. **Point change or higher:** Review changes for new outputs and options that could be relevant to user's application.
2. **Results change or higher:** Review changes for effects on output interpretation; show caution in comparing outputs across the version boundary.
3. **Schema change or higher:** Update downstream code to reflect new output schema.
4. **Major change:** Review new code and outputs thoroughly; be prepared for major changes to downstream code.

During development, changes are tracked in the `# Unreleased` section of `CHANGELOG.md`. The `bump_type:` directive below `# Unreleased` specifies which version number will be incremented on merge to `main`. Version numbers are automatically updated by GitHub Actions when changes are merged to `main`.