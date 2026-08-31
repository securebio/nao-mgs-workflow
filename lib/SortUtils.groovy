// Shared helpers for modules that stream a gzipped file through GNU sort. Aimed
// at reducing disk usage and increasing sort speeds via:
//
// 1. Sizing the internal buffer based on memory. Otherwise, GNU sort sizes its buffer
// based on the size of its input, which is not defined for a pipe and therefore
// defaults to 12MB and forces many slow merge passes.
// 2. Using `pigz -1` to lightly compress intermediate files to prevent disk overflow.
// 3. Parallelizing sort with `--parallel` based on allocated CPUs.
// 4. Setting up a local temporary directory for intermediate files.

class SortUtils {

    // Fraction of the task's memory reservation given to sort's buffer.
    // `--buffer-size=<pct>%` alone would use the instance's total memory, not the per-task allocation.
    static final BigDecimal BUFFER_FRACTION = 0.6

    // Name of the generated helper that sort uses to compress spill files.
    static final String COMPRESS_PROGRAM = "sort_compress"

    // Default buffer size for when a task has no memory reservation; production
    // processes should always have one via their resource label. ~20x sort's 12MB floor.
    static final String DEFAULT_BUFFER = "256M"

    // How many sort runs to merge at a time, pinned to built-in default.
    static final int MERGE_BATCH_SIZE = 16

    // Buffer size for a given task memory reservation, passed to sort
    // with `--buffer-size`.
    static String bufferSize(memory) {
        if (memory == null) return DEFAULT_BUFFER
        return "${(long) (memory.toMega() * BUFFER_FRACTION)}M"
    }

    // Shell prelude: pin the collation locale, then create the spill directory
    // and the compression helper.
    //
    // sort invokes the compress program with no arguments to compress and with
    // -d to decompress, so the compression level and thread count can only be
    // pinned via a wrapper.
    //
    // Spill directory is kept on local disk rather than Fusion-backed work dir to prevent
    // Fusion-driven S3 file churn.
    //
    // Lines are joined with the indentation used inside a Nextflow process script block
    // so commands are well-formatted in .command.sh.
    static final String SCRIPT_INDENT = " " * 8

    static String prelude(String dir) {
        return [
            // Sort byte-wise, not by locale collation. Containers currently run
            // C.UTF-8, which already collates by byte, so this pins behaviour
            // that callers depend on rather than changing it: grouping a sorted
            // table by a key column is only correct if equal keys are adjacent
            // and ordering is the same everywhere the table is produced.
            "export LC_ALL=C",
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
