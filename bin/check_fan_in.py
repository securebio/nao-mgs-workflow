#!/usr/bin/env python3
DESC = """
Static check that every channel operator gathering many items into one, in the
workflows and subworkflows, says why its input is complete.

Under the pipeline's retry-then-ignore error strategy, a task that fails its
retry emits nothing and the run carries on. Tasks depending on that item never
run, so the failure skips its own subtree. An operator that gathers without
knowing how many items to expect does not wait for the missing one: it emits
whatever arrived, and everything downstream runs on the partial result. For
example, an unsized `groupTuple()` over per-species downsampling tasks validated
a group without the failed species.

Each gathering operator (`collect`, `collectFile`, `toList`, `toSortedList`,
`reduce`, `count`, `groupTuple`, or a `join` with `remainder: true`) must carry
a `// complete: <reason>` comment on its line or in the comment block directly
above that line. The reason is usually that the gather is sized with
`groupKey(key, n)`, which drops an incomplete group, or that its input comes
from the head node rather than from tasks that can fail.

Exit codes:
  0 - Every gathering operator is annotated
  1 - One or more gathering operators lack a `// complete:` annotation
"""

###########
# IMPORTS #
###########

import argparse
import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path

###########
# LOGGING #
###########


class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

#############
# CONSTANTS #
#############

# Channel operators that gather many items into one emission. The closure form of `collect`
# is matched only on channel-looking receivers (a `*_ch` name or a `channel.` expression),
# since Groovy's list `collect { ... }` maps a list instead.
GATHER = re.compile(
    r"\.(collect|collectFile|toList|toSortedList|count|groupTuple)\s*\("
    r"|(\b\w+_ch|\bchannel\.\w+\([^)]*\))\s*\.collect\s*\{"
    r"|\.reduce\s*[({]"
    r"|\bremainder\s*:\s*true"
)
ANNOTATION = re.compile(r"//\s*complete:\s*\S")
COMMENT_LINE = re.compile(r"\s*//")
# Workflow sources to scan, relative to the repository root.
SOURCE_GLOBS = ("main.nf", "workflows/*.nf", "subworkflows/local/*/main.nf")

####################
# HELPER FUNCTIONS #
####################


def is_annotated(lines: list[str], index: int) -> bool:
    """
    Check whether the line at `index` carries a `// complete:` annotation.

    Args:
        lines: Lines of a Nextflow source file.
        index: Index of the line holding a gathering operator.
    Returns:
        True if the annotation is on that line or in the run of comment lines
        directly above it.
    """
    if ANNOTATION.search(lines[index]):
        return True
    above = index - 1
    while above >= 0 and COMMENT_LINE.match(lines[above]):
        if ANNOTATION.search(lines[above]):
            return True
        above -= 1
    return False


def find_unannotated_gathers(content: str) -> list[tuple[int, str]]:
    """
    Find gathering operators without a `// complete:` annotation.

    Args:
        content: Full text of a Nextflow source file.
    Returns:
        List of `(line_number, line)` pairs, 1-based, in file order. Operators
        inside comments are ignored.
    """
    lines = content.splitlines()
    unannotated: list[tuple[int, str]] = []
    for index, line in enumerate(lines):
        code = line.split("//", 1)[0]
        if GATHER.search(code) and not is_annotated(lines, index):
            unannotated.append((index + 1, line.strip()))
    return unannotated


def scan_sources(root: Path) -> dict[Path, list[tuple[int, str]]]:
    """
    Scan the workflow sources under a repository root for unannotated gathers.

    Args:
        root: Repository root holding `main.nf`, `workflows/` and `subworkflows/local/`.
    Returns:
        Mapping from file path to its unannotated `(line_number, line)` pairs.
        Files with none are omitted.
    """
    found: dict[Path, list[tuple[int, str]]] = {}
    for pattern in SOURCE_GLOBS:
        for path in sorted(root.glob(pattern)):
            unannotated = find_unannotated_gathers(path.read_text())
            if unannotated:
                found[path] = unannotated
    return found


###################
# ARGUMENT PARSER #
###################


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Repository root to scan (default: current directory).",
    )
    return parser.parse_args()


########
# MAIN #
########


def main() -> None:
    start = time.time()
    args = parse_arguments()
    if not (args.root / "workflows").is_dir():
        raise FileNotFoundError(f"No workflows/ directory under {args.root}")
    found = scan_sources(args.root)
    if found:
        for path, unannotated in found.items():
            for line_number, line in unannotated:
                logger.error("%s:%d: %s", path, line_number, line)
        total = sum(len(v) for v in found.values())
        raise ValueError(
            f"{total} gathering operator(s) lack a `// complete: <reason>` comment. "
            "Under the ignore error strategy an unsized gather passes on a partial result "
            "when an upstream task fails: size it with groupKey(key, n), or say why its "
            "input is complete."
        )
    logger.info("Every gathering operator is annotated")
    logger.info("Completed in %.2fs", time.time() - start)


if __name__ == "__main__":
    main()
