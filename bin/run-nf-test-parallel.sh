#!/usr/bin/env bash
set -euo pipefail

# Check required environment variables
missing_vars=()
if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]]; then
  missing_vars+=("AWS_ACCESS_KEY_ID")
fi
if [[ -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  missing_vars+=("AWS_SECRET_ACCESS_KEY")
fi

if [[ ${#missing_vars[@]} -gt 0 ]]; then
  echo "Error: Required environment variables are not set:" >&2
  for var in "${missing_vars[@]}"; do
    echo "  - $var" >&2
  done
  echo "" >&2
  echo "Please set these variables before running this script." >&2
  exit 1
fi

# Get the directory of this script and go to the parent directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

# The Python script needs root privileges to access test files created by Docker.
# We use sudo and pass along PATH, HOME, and AWS credentials to ensure
# the script and its dependencies are found and AWS access is preserved.
sudo env \
  PATH="${PATH}" \
  HOME="${HOME}" \
  AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  python3 bin/run-nf-test-parallel.py "$@"
