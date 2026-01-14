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

# Check if --num-workers flag is present
use_parallel=false
for arg in "$@"; do
  if [[ "$arg" == "--num-workers" ]]; then
    use_parallel=true
    break
  fi
done

# Run tests and fix ownership of files created by Docker.
# Docker creates files as root, so we use sudo to change ownership back to the user.
# We temporarily disable exit-on-error to ensure cleanup happens regardless of test results.
set +e
if [[ "$use_parallel" == "true" ]]; then
  python3 bin/run_nf_test_parallel.py "$@"
  exit_code=$?
else
  nf-test test "$@"
  exit_code=$?
fi
set -e

# Fix ownership of test artifacts created by Docker
sudo chown -R "$(id -u):$(id -g)" .nf-test

exit $exit_code
