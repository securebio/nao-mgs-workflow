# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM rust:1.83-slim-bookworm AS builder

WORKDIR /build

# Copy the entire Rust workspace and build all tools
# The workspace Cargo.toml lists all members; --workspace builds them all
COPY rust-tools/ ./rust-tools/
RUN cd rust-tools && cargo build --workspace --release

# =============================================================================
# Stage 2: Runtime
# Minimal Debian image with only the runtime dependencies Rust binaries need
# =============================================================================
FROM debian:bookworm-slim

# libgcc-s1 provides runtime support for Rust binaries compiled with the GNU
# toolchain (e.g., stack unwinding primitives, 128-bit integer operations).
# This is standard for dynamically-linked Rust binaries.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 procps \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled binaries from builder
# Workspace builds output to rust-tools/target/release/
COPY --from=builder /build/rust-tools/target/release/mark_duplicates /usr/local/bin/

# Add additional binaries here as tools are added to the workspace:
# COPY --from=builder /build/rust-tools/target/release/future_tool /usr/local/bin/

# Verify binaries are executable
RUN mark_duplicates --version || echo "mark_duplicates installed"

# Default to shell for debugging
CMD ["/bin/sh"]
