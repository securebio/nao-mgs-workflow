// Shared helpers for modules that stream a gzipped file through GNU sort
// (SORT_FILE, SORT_FASTQ). Files in lib/ are automatically loaded by Nextflow
// and callable from script: blocks.
//
// Why this exists
// ---------------
// GNU sort sizes its internal buffer from the st_size of its input. When the
// input is a pipe there is no size to stat, so sort falls back to a ~12 MB
// floor no matter how much memory the task reserved. Every one of these
// modules feeds sort from `pigz -dc |`, so they all hit that floor.
//
// The consequence is quadratic-ish, not marginal: with a 12 MB buffer a U-byte
// input produces U/12MB sorted runs, which sort then merges 16 at a time, so it
// rewrites the whole dataset once per merge level. A 400 GB input becomes
// ~32,000 runs and 4 merge levels, i.e. ~1.6 TB spilled to local disk. In
// production several such tasks share one host volume with Fusion's chunk
// cache, and the volume ran out of space.
//
// Passing an explicit --buffer-size restores the intended single-pass
// behaviour. The remaining spill (one pass over the data, unavoidable once the
// input exceeds memory) is compressed, which costs almost nothing at level 1
// and cuts the bytes on disk by ~9x.

class SortUtils {

    // Fraction of the task's memory reservation given to sort's buffer. The
    // remainder covers the pigz processes on either end of the pipe, sort's
    // per-line bookkeeping (a pointer + struct per line, which is significant
    // for short lines), and container overhead. sort is OOM-killed against the
    // cgroup limit rather than host memory, so this must stay well under 1.
    // NOTE: --buffer-size accepts a percentage, but that percentage is of HOST
    // physical memory, which on these instances is ~30x the task reservation.
    // It must therefore always be passed as an absolute size.
    static final BigDecimal BUFFER_FRACTION = 0.6

    // Name of the generated helper that sort uses to compress spill files.
    static final String COMPRESS_PROGRAM = "sort_compress"

    // Buffer to use when the task has no memory reservation at all. That only
    // happens under a config that does not define the process's resource label:
    // in practice the test configs, which deliberately omit
    // configs/resources.config so that test processes are not scheduled with
    // production-sized reservations.
    //
    // Kept deliberately small rather than matching a real tier, because with no
    // reservation there is no reservation to size against and the container may
    // be tiny. sort only faults in the pages it actually fills, so this costs
    // nothing on small inputs while still being ~20x the pipe-input floor it
    // replaces.
    static final String DEFAULT_BUFFER = "256M"

    // sort merges this many runs at a time. Pinned rather than left to the
    // built-in default (currently also 16) because the compression wrapper's
    // thread budget is reasoned against it: sort runs up to this many
    // decompressors concurrently during a merge.
    static final int MERGE_BATCH_SIZE = 16

    // Buffer size for a given task memory reservation, as a sort -S argument.
    static String bufferSize(memory) {
        if (memory == null) return DEFAULT_BUFFER
        return "${(long) (memory.toMega() * BUFFER_FRACTION)}M"
    }

    // Shell prelude: create the spill directory and the compression helper.
    //
    // sort invokes the compress program with no arguments to compress and with
    // -d to decompress, so the compression level and thread count can only be
    // pinned via a wrapper. Both matter:
    //   - Level 1 compresses sorted text ~9x at ~250 MB/s per core, against
    //     ~11x at ~20 MB/s for pigz's default level. For files that are written
    //     once and read once, a 10x slowdown to gain 20% ratio is a bad trade.
    //   - Threads are pinned to 1 because sort runs up to MERGE_BATCH_SIZE
    //     decompressors *concurrently* during a merge. Letting each default to
    //     nproc would spawn hundreds of threads: Batch sets ECS CpuShares
    //     rather than a cpuset, so nproc inside the container reports the whole
    //     host (48-64), not task.cpus.
    //
    // Defines $SORT_TMPDIR, which options() below refers to; the two must be
    // used together.
    // Lines are joined with the indentation used inside a process script block,
    // so that Nextflow's own stripIndent() still sees a uniform indent across
    // the whole script and dedents it cleanly. Interpolating an unindented
    // multi-line string would drop the common indent to zero and leave the rest
    // of the caller's script indented in the generated .command.sh.
    static final String SCRIPT_INDENT = " " * 8

    static String prelude(String dir) {
        return [
            "# Spill directory: kept on local disk (sort's existing default), not",
            "# the Fusion-backed work dir, where merges would become S3 churn.",
            "export SORT_TMPDIR=\$(mktemp -d -p ${dir} sort.XXXXXXXX)",
            "trap 'rm -rf \"\$SORT_TMPDIR\"' EXIT",
            "printf '#!/bin/sh\\nexec pigz -1 -p 1 \"\$@\"\\n' > \"\$SORT_TMPDIR/${COMPRESS_PROGRAM}\"",
            "chmod +x \"\$SORT_TMPDIR/${COMPRESS_PROGRAM}\"",
        ].join("\n" + SCRIPT_INDENT)
    }

    // Options to add to a `sort` invocation reading from a pipe. Requires
    // prelude() to have run earlier in the same script, which defines
    // $SORT_TMPDIR and writes the compression helper into it.
    //   cpus   : task.cpus
    //   memory : task.memory
    static String options(int cpus, memory) {
        return "--buffer-size=${bufferSize(memory)} " +
               "--parallel=${cpus} " +
               "--batch-size=${MERGE_BATCH_SIZE} " +
               "--temporary-directory=\"\$SORT_TMPDIR\" " +
               "--compress-program=\"\$SORT_TMPDIR/${COMPRESS_PROGRAM}\""
    }

}
