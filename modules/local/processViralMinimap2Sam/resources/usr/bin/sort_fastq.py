#!/usr/bin/env python3
"""Sort a gzipped FASTQ file by read ID."""

import gzip
import os
import subprocess


def sort_fastq(input_fastq_gz: str, output_fastq: str, sort_memory: str = "1G") -> None:
    """Sort a gzipped FASTQ file by read ID using GNU sort.

    Reads 4-line FASTQ records, joins them into tab-delimited single lines
    (equivalent to `paste - - - -`), pipes through GNU sort, then converts
    tabs back to newlines. Sort manages its own memory budget via -S.
    """
    sort_proc = subprocess.Popen(
        ["sort", "-k1,1", f"-S{sort_memory}"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env={**os.environ, "LC_ALL": "C"},
        text=True,
    )

    with gzip.open(input_fastq_gz, "rt") as fh:
        while True:
            lines = [fh.readline() for _ in range(4)]
            if not lines[0]:
                break
            sort_proc.stdin.write(  # type: ignore[union-attr]
                "\t".join(line.rstrip("\n") for line in lines) + "\n"
            )

    sort_proc.stdin.close()  # type: ignore[union-attr]

    with open(output_fastq, "w") as out:
        for line in sort_proc.stdout:  # type: ignore[union-attr]
            out.write(line.rstrip("\n").replace("\t", "\n") + "\n")

    rc = sort_proc.wait()
    if rc != 0:
        raise RuntimeError(f"sort exited with code {rc}")
