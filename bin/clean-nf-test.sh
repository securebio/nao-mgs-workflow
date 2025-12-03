#!/usr/bin/env bash
set -euo pipefail

# Get the directory of this script and go to the parent directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

# nf-test clean needs root privileges to remove test files created by
# Docker.  We use sudo and pass along PATH and HOME to ensure nf-test and its
# dependencies are found.
sudo env PATH="${PATH}" HOME="${HOME}" nf-test clean
