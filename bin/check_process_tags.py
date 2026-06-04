#!/usr/bin/env python3
DESC = """
Static check that every Nextflow process in modules/local/ declares a
well-formed `tag` directive, used for per-task cost attribution in trace files
and logs.

Two complementary checks guard the tag invariant:
  - This static lint verifies, by scanning source, that every process declares
    a `tag` directive, that its literal template is well-formed
    (`id=<value>` optionally followed by `,<key>=<value>` components), and that
    every `${...}` variable it interpolates is a declared input of that process
    (or a Nextflow global like `params`/`task`). It covers every process,
    including orphaned ones never executed by any workflow.
  - The runtime `assertTraceTagsValid` helper in the workflow tests verifies the
    *rendered* tag values (post-interpolation, non-empty, correct grammar) for
    processes actually executed. Only execution can validate interpolated
    values, so that check remains necessary.

The variable check catches a class of bug the others miss: a tag whose template
references an input that a refactor renamed or removed (e.g. `name=${taxid}`
after the input became `path(accession_chunk)`). Such a tag is well-formed and
the process may never run in a test, yet it raises `MissingPropertyException`
at trace-resolution time whenever it does run.

A process is considered tagged if a `tag` directive appears in its directive
section (before the first `input:`/`output:`/`when:`/`script:`/`shell:`/`exec:`
label). When the directive's argument is a string literal, its template and
variables are checked; non-literal (dynamic) tag expressions are accepted as
present but not further checked, since their value is only known at runtime.

Exit codes:
  0 - Every process declares a well-formed, resolvable tag directive
  1 - One or more processes have a missing, malformed, or unresolvable tag
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

# A process directive/body section starts at one of these labels.
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
# Declared input names: the identifier inside val()/path()/env()/each(),
# including those nested inside a `tuple` declaration.
INPUT_DECL = re.compile(r"\b(?:val|path|env|each)\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)")
# The root variable of a `${...}` (or `$name`) interpolation in a tag template,
# e.g. `${accession_chunk.baseName}` -> `accession_chunk`, `${sample}` -> sample.
TAG_VAR = re.compile(r"\$\{?\s*([A-Za-z_][A-Za-z0-9_]*)")
# Globals always in scope for a tag directive, so never "unresolved".
ALLOWED_GLOBALS = frozenset({"params", "task", "workflow"})

####################
# HELPER FUNCTIONS #
####################


def split_processes(content: str) -> list[tuple[str, list[str]]]:
    """
    Split a module file into per-process bodies.

    Args:
        content: Full text of a Nextflow module file.
    Returns:
        List of `(process_name, body_lines)` in declaration order, where
        `body_lines` are the lines after the `process NAME {` line up to (but
        not including) the next process declaration.
    """
    processes: list[tuple[str, list[str]]] = []
    name: str | None = None
    body: list[str] = []
    for line in content.splitlines():
        start = PROCESS_START.match(line)
        if start:
            if name is not None:
                processes.append((name, body))
            name = start.group(1)
            body = []
            continue
        if name is not None:
            body.append(line)
    if name is not None:
        processes.append((name, body))
    return processes


def directive_tag_line(body_lines: list[str]) -> str | None:
    """Return the `tag` directive line in the directive section, or None."""
    for line in body_lines:
        if BODY_LABEL.match(line):
            return None  # reached the body before any tag directive
        if TAG_DIRECTIVE.match(line):
            return line
    return None


def input_names(body_lines: list[str]) -> set[str]:
    """Collect declared input names from a process body's `input:` block."""
    names: set[str] = set()
    in_input = False
    for line in body_lines:
        label = BODY_LABEL.match(line)
        if label:
            in_input = label.group(1) == "input"
            continue
        if in_input:
            names.update(INPUT_DECL.findall(line))
    return names


def find_tag_violations(content: str) -> list[tuple[str, str]]:
    """
    Find processes in a single module file with a missing, malformed, or
    unresolvable tag directive.

    For each `process NAME { ... }`, reports a violation when its directive
    section has no `tag`, when the directive's string-literal template is not
    well-formed (see VALID_TAG), or when the template interpolates a variable
    that is neither a declared input nor a Nextflow global (ALLOWED_GLOBALS).
    Dynamic (non-literal) tag expressions are accepted without further checks.

    Args:
        content: Full text of a Nextflow module file.
    Returns:
        List of `(process_name, reason)` tuples in declaration order, at most
        one per violating process. Empty if every process is well-formed.
    """
    violations: list[tuple[str, str]] = []
    for name, body in split_processes(content):
        tag_line = directive_tag_line(body)
        if tag_line is None:
            violations.append((name, "missing tag directive"))
            continue
        match = TAG_LITERAL.search(tag_line)
        if match is None:
            # Dynamic tag expression; value is only known at runtime.
            continue
        value = match.group(1)
        if not VALID_TAG.fullmatch(value):
            violations.append(
                (
                    name,
                    f'malformed tag "{value}" (expected id=<value>[,key=<value>...])',
                )
            )
            continue
        declared = input_names(body)
        unresolved = [
            var
            for var in TAG_VAR.findall(value)
            if var not in declared and var not in ALLOWED_GLOBALS
        ]
        if unresolved:
            violations.append(
                (
                    name,
                    f'tag references unknown variable "{unresolved[0]}" '
                    "(not a declared input of the process)",
                )
            )
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
            f"{total} process(es) in {modules_dir} have a missing, malformed, or "
            'unresolvable tag directive. Every process must declare `tag "id=<value>"` '
            "(optionally with `,<key>=<value>` components) referencing declared "
            "inputs, for per-task cost attribution; see the tag conventions in CLAUDE.md."
        )

    n_files = len(list(modules_dir.glob("*/main.nf")))
    logger.info(
        "All processes across %d module files declare a well-formed tag directive",
        n_files,
    )
    logger.info("Completed in %.2fs", time.time() - start)


if __name__ == "__main__":
    main()
