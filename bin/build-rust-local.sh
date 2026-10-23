#!/usr/bin/env bash
# Build the Rust tools container locally for development
#
# Usage:
#   ./bin/build-rust-local.sh              # Build with default tag :local
#   ./bin/build-rust-local.sh my-feature   # Build with custom tag :my-feature
#
# Then run the workflow with:
#   nextflow run main.nf -profile rust_dev ...

set -euo pipefail

TAG="${1:-local}"
IMAGE_NAME="nao-rust-tools:${TAG}"

echo "Building Rust tools container: ${IMAGE_NAME}"
echo "================================================"

# Build from repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

docker build \
    -f "${REPO_ROOT}/docker/nao-rust-tools.Dockerfile" \
    -t "${IMAGE_NAME}" \
    "${REPO_ROOT}"

echo ""
echo "================================================"
echo "Build complete: ${IMAGE_NAME}"
echo ""
echo "To use this container, run your workflow with:"
echo "  nextflow run main.nf -profile rust_dev ..."
echo ""
echo "To publish the image and pin it in configs/containers.config, run:"
echo "  bin/build_rust_tools_container.py"
