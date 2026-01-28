# Rust Tools

Rust utilities that run as part of the Nextflow pipeline. These are compiled into the
`nao-rust-tools` container and used by processes with `label 'rust_tools'`.

## Current Tools

- **mark_duplicates** — Marks duplicate alignments in SAM/BAM data

## Adding or Modifying Tools

See [docs/developer.md](../docs/developer.md#rust) for build instructions, local
development workflow, and how to add new tools.

## Note on post-processing/rust_dedup/

A separate Rust tool exists in `post-processing/rust_dedup/` for similarity-based
duplicate marking. That tool is currently a standalone post-processing utility (not
part of the Nextflow pipeline) and may be consolidated here in a future PR.
