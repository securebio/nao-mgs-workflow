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

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse --repo-root and --num-workers from arguments
use_parallel=false
repo_root=""
args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root) repo_root="$2"; shift 2 ;;
    --num-workers) use_parallel=true; args+=("$1" "$2"); shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
set -- "${args[@]}"

# Default to parent of script directory; resolve to absolute
repo_root="$(cd "${repo_root:-$SCRIPT_DIR/..}" && pwd)"
cd "$repo_root"

# Run tests and fix ownership of files created by Docker.
# Docker creates files as root, so we use sudo to change ownership back to the user.
# We temporarily disable exit-on-error to ensure cleanup happens regardless of test results.
set +e
if [[ "$use_parallel" == "true" ]]; then
  python3 "$SCRIPT_DIR/run_nf_test_parallel.py" --repo-root "$repo_root" "$@"
  exit_code=$?
else
  nf-test test "$@"
  exit_code=$?
fi
set -e

# Fix ownership of test artifacts created by Docker
sudo chown -R "$(id -u):$(id -g)" .nf-test

exit $exit_code
