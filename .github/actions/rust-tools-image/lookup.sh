#!/usr/bin/env bash
# Print the digest of image $1, or nothing if it doesn't exist. Polls for up to
# $2 minutes while the image is absent. Registry errors other than "not found"
# are retried, then fail, so a flaky lookup is never mistaken for a missing image.
set -euo pipefail
IMAGE=$1
WAIT_MINUTES=${2:-0}
POLL_SECONDS=${POLL_SECONDS:-30}
MAX_ERRORS=${MAX_ERRORS:-5}
DEADLINE=$(( $(date +%s) + WAIT_MINUTES * 60 ))
ERRORS=0
ERR=$(mktemp)
trap 'rm -f "$ERR"' EXIT
while true; do
  # Plain {{.Manifest.Digest}} prints the whole manifest summary; json doesn't
  if DIGEST=$(docker buildx imagetools inspect "$IMAGE" --format '{{json .Manifest.Digest}}' 2>"$ERR" | tr -d '"'); then
    echo "Found $IMAGE at $DIGEST" >&2
    echo "$DIGEST"
    exit 0
  fi
  # Match the image-specific message, not e.g. a credential helper "not found in $PATH"
  if grep -qF "$IMAGE: not found" "$ERR"; then
    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
      echo "$IMAGE not found" >&2
      exit 0
    fi
    echo "Waiting for $IMAGE..." >&2
  else
    ERRORS=$((ERRORS + 1))
    if [ "$ERRORS" -ge "$MAX_ERRORS" ]; then
      echo "::error::Could not inspect $IMAGE: $(cat "$ERR")" >&2
      exit 1
    fi
    echo "Inspect failed (attempt $ERRORS/$MAX_ERRORS), retrying: $(cat "$ERR")" >&2
  fi
  sleep "$POLL_SECONDS"
done
