# Rust Tools

Rust utilities that run as part of the Nextflow pipeline. These are compiled into the
`nao-rust-tools` container and used by processes with `label "rust_tools"`.

## Workspace Tools

- **mark_duplicates** — Marks duplicate alignments in SAM/BAM data
- **mark_duplicates_similarity** — Marks similarity-based duplicates among alignment-unique reads using [nao-dedup](https://github.com/securebio/nao-dedup)
- **process_vsearch_cluster_output** — Processes tabular output from VSEARCH clustering

## External Tools

These tools are not workspace members but are installed into the `nao-rust-tools`
container via `cargo install` in the Dockerfile:

- **nucleaze** — K-mer based read filtering against a reference index ([source](https://github.com/jackdougle/nucleaze))

## Adding or Modifying Tools

See [docs/developer.md](../docs/developer.md#rust) for build instructions, local
development workflow, and how to add new tools.

## Note on post-processing/rust_dedup/

The similarity-based duplicate marking tool has been copied into this workspace as
`mark_duplicates_similarity`. The original source in `post-processing/rust_dedup/`
is retained for backwards compatibility with `securebio/nao-mgs-partner-reports`
and will be removed once that dependency is migrated.
