// Sort a TSV file by a specified column header, preserving the header line
// Sort buffer, spill directory and spill compression are configured by
// lib/SortUtils.groovy.
process SORT_TSV {
    label "coreutils"
    label "sort_resources"
    tag "id=${sample}"
    input:
        tuple val(sample), path(input_file)
        val(sort_field)
    output:
        tuple val(sample), path("sorted_${sort_field}_${input_file}"), emit: sorted
        tuple val(sample), path("input_${input_file}"), emit: input
    script:
        def gzipped = input_file.toString().endsWith(".gz")
        // Inflate is essentially serial, so decompression gains nothing from more
        // threads; cap it at 2 as MINIMAP2 does rather than reserving task.cpus.
        def read_cmd = gzipped ? "pigz -dc -p 2" : "cat"
        // These tables are pipeline intermediates, so compress at level 1 (#944).
        def write_cmd = gzipped ? "pigz -p ${task.cpus} -1" : "cat"
        def out = "sorted_${sort_field}_${input_file}"
        def sort_opts = SortUtils.options(task.cpus, task.memory)
        """
        set -euo pipefail
        ${SortUtils.prelude('"${TMPDIR:-/tmp}"')}
        tab=\$(printf '\\t')
        # The task working directory is the Fusion mount, i.e. an S3 prefix; only
        # \$TMPDIR (/tmp) is instance-local disk, on the container overlayfs. Peel the
        # header off the stream and hand the remaining rows straight to sort so the
        # table is never materialised in the work directory, and let SortUtils put
        # sort's spill under \$TMPDIR rather than alongside it.
        ${read_cmd} ${input_file} | {
            # A final line with no trailing newline still populates header, but read
            # returns non-zero at EOF; keep what it read rather than clearing it.
            IFS= read -r header || true
            if [ -z "\$header" ]; then
                # No header means an empty input; emit an empty output to match
                : | ${write_cmd} > ${out}
            else
                col=\$(printf '%s\\n' "\$header" \\
                    | awk -F'\\t' -v f="${sort_field}" '{for (i=1;i<=NF;i++) if (\$i==f) {print i; exit}}')
                if [ -z "\$col" ]; then
                    echo "Could not find sort field in input header: '${sort_field}', \$header" >&2
                    exit 1
                fi
                {
                    printf '%s\\n' "\$header"
                    sort -t "\$tab" -k\$col,\$col ${sort_opts}
                } | ${write_cmd} > ${out}
            fi
        }
        # Link input to output for testing
        ln -s ${input_file} input_${input_file}
        """
}
