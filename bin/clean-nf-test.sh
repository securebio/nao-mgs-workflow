#!/usr/bin/env bash

# Get the directory of this script and go to the parent directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

# Since we're running as root we need to pass along the environment variables
# that make nextflow work.
sudo env PATH=${PATH} HOME=${HOME} nf-test clean
