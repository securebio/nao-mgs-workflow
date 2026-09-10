# Rust Tools

Rust utilities that run as part of the Nextflow pipeline. These are compiled into the
`nao-rust-tools` container and used by processes with `label "rust_tools"`.

## Workspace Tools

- **mark_duplicates** — Marks duplicate alignments in SAM/BAM data
- **mark_duplicates_similarity** — Marks similarity-based duplicates among alignment-unique reads using [nao-dedup](https://github.com/securebio/nao-dedup)

## External Tools

These tools are not workspace members but are installed into the `nao-rust-tools`
container via `cargo install` in the Dockerfile:

- **nucleaze** — K-mer based read filtering against a reference index ([source](https://github.com/jackdougle/nucleaze))

## Adding or Modifying Tools

See [docs/developer.md](../docs/developer.md#rust) for build instructions, local
development workflow, and how to add new tools.

## Note on mark_duplicates_similarity

Similarity-based duplicate marking used to live in `post-processing/rust_dedup/`, to be
run by hand on workflow outputs. DOWNSTREAM runs it itself as of v3.4.0.0, so this
workspace holds the only copy.
