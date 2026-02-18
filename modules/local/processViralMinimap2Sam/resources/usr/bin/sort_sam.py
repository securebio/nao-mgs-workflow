#!/usr/bin/env python3
"""Sort a gzipped SAM file by read ID (QNAME), preserving headers."""

import gzip
import os
import subprocess


def sort_sam(input_sam_gz: str, output_sam: str, sort_memory: str = "2G") -> None:
    """Sort a gzipped SAM file by QNAME using GNU sort.

    Header lines (@-prefixed) are written first, then alignment lines are
    streamed through GNU sort. Sort manages its own memory budget via -S,
    spilling to disk only if needed — no Python-side temp files required.
    """
    with gzip.open(input_sam_gz, "rt") as fh, open(output_sam, "w") as out:
        sort_proc = subprocess.Popen(
            ["sort", "-t\t", "-k1,1", f"-S{sort_memory}", "-T."],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env={**os.environ, "LC_ALL": "C"},
            text=True,
        )
        # Asserts placate the type checker; stdin/stdout are set because we passed PIPE above
        assert sort_proc.stdin is not None and sort_proc.stdout is not None

        for line in fh:
            if line.startswith("@"):
                out.write(line)
            else:
                sort_proc.stdin.write(line)

        sort_proc.stdin.close()

        # Append sorted alignment lines after headers
        for line in sort_proc.stdout:
            out.write(line)

        rc = sort_proc.wait()
        if rc != 0:
            raise RuntimeError(f"sort exited with code {rc}")
