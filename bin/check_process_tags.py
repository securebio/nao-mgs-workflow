#!/usr/bin/env python3
DESC = """
Static check that every Nextflow process in modules/local/ declares a
well-formed `tag` directive, used for per-task cost attribution in trace files
and logs.

Two complementary checks guard the tag invariant:
  - This static lint verifies, by scanning source, that every process declares
    a `tag` directive and that its literal template is well-formed
    (`id=<value>` optionally followed by `,<key>=<value>` components). It covers
    every process, including orphaned ones never executed by any workflow.
  - The runtime `assertTraceTagsValid` helper in the workflow tests verifies the
    *rendered* tag values (post-interpolation, non-empty, correct grammar) for
    processes actually executed. Only execution can validate interpolated
    `${...}` values, so that check remains necessary.

A process is considered tagged if a `tag` directive appears in its directive
section (before the first `input:`/`output:`/`when:`/`script:`/`shell:`/`exec:`
label). When the directive's argument is a string literal, its template is also
format-checked; non-literal (dynamic) tag expressions are accepted as present
but not format-checked, since their value is only known at runtime.

Exit codes:
  0 - Every process declares a well-formed tag directive
  1 - One or more processes have a missing or malformed tag directive
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

# A process directive section ends at the first of these body labels.
BODY_LABEL = re.compile(r"\s*(input|output|when|script|shell|exec)\s*:")
# Process declarations use UPPER_SNAKE_CASE names.
PROCESS_START = re.compile(r"\s*process\s+([A-Z_][A-Z0-9_]*)\s*\{")
# A line that is a `tag` directive: `tag "..."`, `tag '...'`, or `tag( ... )`.
TAG_DIRECTIVE = re.compile(r"\s*tag[\s(]")
# Extract the string-literal argument of a tag directive, if it has one.
TAG_LITERAL = re.compile(r"""tag\s*\(?\s*['"]([^'"]*)['"]""")
# Well-formed tag template: an `id=<value>` component optionally followed by
# `,<key>=<value>` components. <value> excludes `=`, `,`, and whitespace, which
# permits `${...}` interpolation placeholders and plain identifiers alike. This
# mirrors the runtime grammar enforced by assertTraceTagsValid, modulo the
# interpolated values that only execution can resolve.
VALID_TAG = re.compile(r"id=[^=,\s]+(?:,[a-z_][a-z0-9_]*=[^=,\s]+)*")

####################
# HELPER FUNCTIONS #
####################


def find_tag_violations(content: str) -> list[tuple[str, str]]:
    """
    Find processes in a single module file with a missing or malformed tag.

    Scans each `process NAME { ... }` declaration. A process is a violation if
    its directive section contains no `tag` directive, or if the directive's
    string-literal template is not well-formed (see VALID_TAG). Dynamic
    (non-literal) tag expressions are accepted as present without format checks.

    Args:
        content: Full text of a Nextflow module file.
    Returns:
        List of `(process_name, reason)` tuples in declaration order, one per
        violating process. Empty if every process is well-formed.
    """
    violations: list[tuple[str, str]] = []
    current: str | None = None
    in_directives = False
    tag_line: str | None = None

    def flush() -> None:
        if current is None:
            return
        if tag_line is None:
            violations.append((current, "missing tag directive"))
            return
        match = TAG_LITERAL.search(tag_line)
        if match is None:
            # Dynamic tag expression; value is only known at runtime.
            return
        value = match.group(1)
        if not VALID_TAG.fullmatch(value):
            violations.append(
                (
                    current,
                    f'malformed tag "{value}" (expected id=<value>[,key=<value>...])',
                )
            )

    for line in content.splitlines():
        start = PROCESS_START.match(line)
        if start:
            # A new process begins: finalize the previous one first.
            flush()
            current = start.group(1)
            in_directives = True
            tag_line = None
            continue
        if current is None:
            continue
        if in_directives and BODY_LABEL.match(line):
            # Directive section is over; the verdict for this process is fixed.
            in_directives = False
            continue
        if in_directives and tag_line is None and TAG_DIRECTIVE.match(line):
            tag_line = line

    flush()
    return violations


def scan_modules(modules_dir: Path) -> dict[Path, list[tuple[str, str]]]:
    """
    Scan every `*/main.nf` under a modules directory for tag violations.

    Args:
        modules_dir: Directory holding one subdirectory per module, each with a
            `main.nf` file.
    Returns:
        Mapping from module file path to the list of `(process_name, reason)`
        violations in that file. Files with no violations are omitted.
    """
    violations: dict[Path, list[tuple[str, str]]] = {}
    for main_nf in sorted(modules_dir.glob("*/main.nf")):
        file_violations = find_tag_violations(main_nf.read_text())
        if file_violations:
            violations[main_nf] = file_violations
    return violations


###################
# ARGUMENT PARSER #
###################


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument(
        "--modules-dir",
        type=Path,
        default=Path("modules/local"),
        help="Directory containing module subdirectories (default: modules/local).",
    )
    return parser.parse_args()


########
# MAIN #
########


def main() -> None:
    start = time.time()
    args = parse_arguments()
    modules_dir = args.modules_dir
    if not modules_dir.is_dir():
        raise FileNotFoundError(f"Modules directory not found: {modules_dir}")

    violations = scan_modules(modules_dir)
    if violations:
        for path, file_violations in violations.items():
            for name, reason in file_violations:
                logger.error("Process %s in %s: %s", name, path, reason)
        total = sum(len(v) for v in violations.values())
        raise ValueError(
            f"{total} process(es) in {modules_dir} have a missing or malformed tag "
            'directive. Every process must declare `tag "id=<value>"` (optionally '
            "with `,<key>=<value>` components) for per-task cost attribution; see "
            "the tag conventions in CLAUDE.md."
        )

    n_files = len(list(modules_dir.glob("*/main.nf")))
    logger.info(
        "All processes across %d module files declare a well-formed tag directive",
        n_files,
    )
    logger.info("Completed in %.2fs", time.time() - start)


if __name__ == "__main__":
    main()
