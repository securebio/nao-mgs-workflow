#!/usr/bin/env python3
"""Sort a gzipped FASTQ file by read ID.
We use this Python wrapper to keep the sorting logic pytest-able and out of the process
shell block, and we sort internally to the process so that it doesn't require sorted
inputs."""

import gzip
import os
import subprocess


def sort_fastq(input_fastq_gz: str, output_fastq: str, sort_memory: str = "2G") -> None:
    """Sort a gzipped FASTQ file by read ID using GNU sort.

    Reads 4-line FASTQ records, joins them into tab-delimited single lines
    (equivalent to `paste - - - -`), pipes through GNU sort, then converts
    tabs back to newlines. Sort manages its own memory budget via -S.
    """
    sort_proc = subprocess.Popen(
        ["sort", "-k1,1", f"-S{sort_memory}", "-T."],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env={**os.environ, "LC_ALL": "C"},  # sort by byte code, not locale ordering
        text=True,
    )
    # Asserts placate the type checker; stdin/stdout are set because we passed PIPE above
    assert sort_proc.stdin is not None and sort_proc.stdout is not None

    with gzip.open(input_fastq_gz, "rt") as fh:
        while True:
            lines = [fh.readline() for _ in range(4)]
            if not lines[0]:
                break
            sort_proc.stdin.write("\t".join(line.rstrip("\n") for line in lines) + "\n")

    sort_proc.stdin.close()

    with open(output_fastq, "w") as out:
        for line in sort_proc.stdout:
            out.write(line.rstrip("\n").replace("\t", "\n") + "\n")

    rc = sort_proc.wait()
    if rc != 0:
        raise RuntimeError(f"sort exited with code {rc}")
