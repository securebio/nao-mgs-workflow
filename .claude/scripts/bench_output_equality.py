#!/usr/bin/env python3
DESC = """
Verify per-sample output equality between two pipeline runs (typically dev vs
PR cohorts in scratch S3) by hashing decompressed-and-sorted content of each
file under both prefixes.

Matches the canonical recipe from `.claude/benchmarking.md`:
    - For each common object: fetch, gunzip if `.gz`, sort lines, md5sum.
    - Compare hashes.

Annotates known-noise files (Kraken2 HyperLogLog n_minimizers_distinct, FastQC
percent_duplicates) whose DIFFs are order-sensitive estimator drift, not real
result changes — so PR-description writeups can distinguish results-preserving
PRs that happen to perturb read order from PRs that actually change content.

JSON always goes to stdout. Pass `--md FILE` to also write a markdown
summary to that path — emitting both formats in one invocation (useful
for the bench subagents that need both).
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import hashlib
import json
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urlparse

import boto3


###############
# KNOWN NOISE #
###############

# Files whose DIFFs are order-sensitive estimator drift, not real content
# changes. Matched as a suffix of the relative key. Keep this list in sync with
# `.claude/benchmarking.md`'s "Known noise sources" section.
KNOWN_NOISE_SUFFIXES: tuple[str, ...] = (
    "kraken.tsv.gz",
    "qc_basic_stats_cleaned.tsv.gz",
)


###############
# DATA SHAPES #
###############


class FileResult(TypedDict):
    """Per-key comparison result."""

    key: str
    status: str  # "ok" | "diff_unexpected" | "diff_known_noise" | "dev_only" | "pr_only"
    dev_hash: str | None
    pr_hash: str | None
    note: str | None


class Summary(TypedDict):
    """Top-level summary counts."""

    total: int
    ok: int
    diff_unexpected: int
    diff_known_noise: int
    dev_only: int
    pr_only: int


class Report(TypedDict):
    """Full output report."""

    dev_prefix: str
    pr_prefix: str
    summary: Summary
    files: list[FileResult]


##############
# S3 HELPERS #
##############


@dataclass(frozen=True)
class S3Uri:
    """Parsed S3 URI."""

    bucket: str
    prefix: str

    @classmethod
    def parse(cls, uri: str) -> "S3Uri":
        """Parse `s3://bucket/prefix/`. The prefix is normalized to end with `/`."""
        parsed = urlparse(uri)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"not an s3:// URI: {uri}")
        prefix = parsed.path.lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return cls(bucket=parsed.netloc, prefix=prefix)


def list_keys(s3_client: Any, uri: S3Uri) -> Iterator[str]:
    """List object keys under a prefix, yielding relative keys (prefix stripped).

    Args:
        s3_client: A boto3 S3 client.
        uri: Parsed S3 prefix to enumerate.

    Yields:
        Each object's key with `uri.prefix` removed. Sub-directory structure is
        preserved (so `foo/bar.tsv` stays `foo/bar.tsv`).
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=uri.bucket, Prefix=uri.prefix):
        for obj in page.get("Contents", []):
            full_key = obj["Key"]
            if not full_key.startswith(uri.prefix):
                continue
            rel = full_key[len(uri.prefix) :]
            if not rel:
                continue
            yield rel


def fetch_and_hash(s3_client: Any, bucket: str, key: str) -> str:
    """Download an object, decompress if `.gz`, sort lines, return md5 hex.

    Matches the canonical recipe from `.claude/benchmarking.md`. The sort step
    canonicalizes content emitted in non-deterministic order by multi-threaded
    processes (so e.g. pigz parallel-write reorderings don't show as DIFFs).

    Binary files are unlikely in `output/results/` — the convention is text
    (TSV/CSV/JSON, possibly gzipped). If a non-text gzipped file appears, sort
    will still produce a deterministic hash since it operates on byte sequences
    interpreted as lines.

    Args:
        s3_client: A boto3 S3 client.
        bucket: Source bucket.
        key: Full object key.

    Returns:
        Hex-encoded md5 of `sort(decompressed_content)`.
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    if key.endswith(".gz"):
        body = gzip.decompress(body)
    sorted_body = b"\n".join(sorted(body.split(b"\n")))
    return hashlib.md5(sorted_body, usedforsecurity=False).hexdigest()


###################
# CLASSIFICATION  #
###################


def is_known_noise(key: str) -> bool:
    """Return True if `key` matches a `KNOWN_NOISE_SUFFIXES` entry."""
    return any(key.endswith(suffix) for suffix in KNOWN_NOISE_SUFFIXES)


def classify_diff(key: str) -> str:
    """Classify a DIFF as known-noise or unexpected, based on the file."""
    return "diff_known_noise" if is_known_noise(key) else "diff_unexpected"


###############
# COMPARISON  #
###############


def compare_prefixes(s3_client: Any, dev: S3Uri, pr: S3Uri) -> Report:
    """Compare every object under two S3 prefixes.

    Args:
        s3_client: A boto3 S3 client.
        dev: Dev-cohort results prefix.
        pr: PR-cohort results prefix.

    Returns:
        A `Report` with per-file results + a summary count by status.
    """
    dev_keys = set(list_keys(s3_client, dev))
    pr_keys = set(list_keys(s3_client, pr))
    all_keys = sorted(dev_keys | pr_keys)

    results: list[FileResult] = []
    counts = {"ok": 0, "diff_unexpected": 0, "diff_known_noise": 0, "dev_only": 0, "pr_only": 0}

    for rel in all_keys:
        in_dev = rel in dev_keys
        in_pr = rel in pr_keys

        if in_dev and not in_pr:
            results.append(
                FileResult(key=rel, status="dev_only", dev_hash=None, pr_hash=None, note=None)
            )
            counts["dev_only"] += 1
            continue
        if in_pr and not in_dev:
            results.append(
                FileResult(key=rel, status="pr_only", dev_hash=None, pr_hash=None, note=None)
            )
            counts["pr_only"] += 1
            continue

        dev_hash = fetch_and_hash(s3_client, dev.bucket, dev.prefix + rel)
        pr_hash = fetch_and_hash(s3_client, pr.bucket, pr.prefix + rel)
        if dev_hash == pr_hash:
            results.append(
                FileResult(key=rel, status="ok", dev_hash=dev_hash, pr_hash=pr_hash, note=None)
            )
            counts["ok"] += 1
        else:
            status = classify_diff(rel)
            note = "order-sensitive estimator drift" if status == "diff_known_noise" else None
            results.append(
                FileResult(key=rel, status=status, dev_hash=dev_hash, pr_hash=pr_hash, note=note)
            )
            counts[status] += 1

    summary = Summary(
        total=len(all_keys),
        ok=counts["ok"],
        diff_unexpected=counts["diff_unexpected"],
        diff_known_noise=counts["diff_known_noise"],
        dev_only=counts["dev_only"],
        pr_only=counts["pr_only"],
    )
    return Report(
        dev_prefix=f"s3://{dev.bucket}/{dev.prefix}",
        pr_prefix=f"s3://{pr.bucket}/{pr.prefix}",
        summary=summary,
        files=results,
    )


##############
# FORMATTING #
##############


def render_markdown(report: Report) -> str:
    """Render the report as a compact markdown summary + per-file table."""
    s = report["summary"]
    lines = [
        "## Output equality",
        "",
        f"Comparing {report['dev_prefix']} (dev) vs {report['pr_prefix']} (PR)",
        "",
        "| Status | Count |",
        "|---|---:|",
        f"| OK (byte-identical) | {s['ok']} |",
        f"| DIFF (known-noise) | {s['diff_known_noise']} |",
        f"| DIFF (unexpected) | {s['diff_unexpected']} |",
        f"| dev-only | {s['dev_only']} |",
        f"| pr-only | {s['pr_only']} |",
        f"| **Total** | **{s['total']}** |",
        "",
    ]
    diffs = [f for f in report["files"] if f["status"].startswith("diff") or f["status"].endswith("_only")]
    if diffs:
        lines += [
            "### Files needing attention",
            "",
            "| Key | Status | Note |",
            "|---|---|---|",
        ]
        for f in diffs:
            note = f["note"] or ""
            lines.append(f"| `{f['key']}` | {f['status']} | {note} |")
        lines.append("")
    else:
        lines += ["All files byte-identical.", ""]
    return "\n".join(lines)


########
# MAIN #
########


def parse_arguments(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=DESC, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dev_prefix", type=str, help="s3:// URI of the dev cohort results prefix")
    parser.add_argument("pr_prefix", type=str, help="s3:// URI of the PR cohort results prefix")
    parser.add_argument(
        "--md",
        type=Path,
        default=None,
        help="If set, write a markdown summary to this path. JSON always goes to stdout.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main() -> None:
    """Compare two S3 result prefixes, emit JSON to stdout (and markdown to a file if `--md` given)."""
    args = parse_arguments()
    dev = S3Uri.parse(args.dev_prefix)
    pr = S3Uri.parse(args.pr_prefix)
    s3_client = boto3.client("s3")
    report = compare_prefixes(s3_client, dev, pr)

    json.dump(report, sys.stdout, indent=2)
    sys.stdout.write("\n")
    if args.md is not None:
        args.md.write_text(render_markdown(report))


if __name__ == "__main__":
    main()
