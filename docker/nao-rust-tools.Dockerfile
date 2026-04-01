# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM rust:1.88-alpine3.21 AS builder

# musl-dev provides headers for C compilation (needed by bzip2-sys)
# git is needed for cargo install from GitHub repositories
RUN apk add --no-cache musl-dev git

WORKDIR /build

# Copy the entire Rust workspace and build all tools
# The workspace Cargo.toml lists all members; --workspace builds them all
COPY rust-tools/ ./rust-tools/
RUN cd rust-tools && cargo build --workspace --release

# Install nucleaze from GitHub (external tool, not a workspace member)
RUN cargo install nucleaze --git https://github.com/jackdougle/nucleaze.git --rev 2208f36

# =============================================================================
# Stage 2: Runtime
# Alpine eliminates Debian glibc/zlib CVEs; musl + panic=abort makes Rust
# binaries fully static, so no libgcc runtime dependency is needed.
# =============================================================================
FROM alpine:3.21

# bash:   Nextflow script blocks default to /bin/bash
# grep:   GNU grep with PCRE support (-oP) used in Nextflow modules
# procps: Nextflow resource monitoring
RUN apk add --no-cache bash grep procps

# Copy compiled binaries from builder
# Add additional binaries here as tools are added to the workspace
COPY --from=builder /build/rust-tools/target/release/mark_duplicates /usr/local/bin/
COPY --from=builder /build/rust-tools/target/release/process_vsearch_cluster_output /usr/local/bin/
COPY --from=builder /usr/local/cargo/bin/nucleaze /usr/local/bin/

# Verify binaries are executable
RUN mark_duplicates --help
RUN process_vsearch_cluster_output --help
RUN nucleaze --help

# Default to shell for debugging
CMD ["/bin/sh"]
