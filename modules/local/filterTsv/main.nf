// Filter a gzipped TSV to keep the first line for each combination of values in the specified columns
process FILTER_TSV {
    label "coreutils"
    label "single"
    input:
        tuple val(sample), path(tsv)
        val(filter_cols) // One-indexed, comma-separated list of columns to filter on
        val(label)
    output:
        tuple val(sample), path("${sample}_${label}_filtered.tsv.gz"), emit: output
        tuple val(sample), path("${sample}_${label}_in.tsv.gz"), emit: input
    script:
        """
        set -euo pipefail
        zcat ${tsv} | awk -F'\\t' -v cols="${filter_cols}" '
            BEGIN {
                split(cols, colIdx, ",")
                maxCol = 0
                for (i in colIdx) {
                    if (colIdx[i] > maxCol) { maxCol = colIdx[i] }
                }
                prevKey = ""
            }
            {
                if (NF < maxCol) {
                    printf "ERROR: Not enough columns (expected %d, got %d)", maxCol, NF > "/dev/stderr"
                    exit 1
                }
                key = ""
                for (i = 1; i <= length(colIdx); i++) {
                    if (i > 1) { key = key "\\t" }
                    key = key \$(colIdx[i])
                }
                if (prevKey != "" && key < prevKey) {
                    printf "ERROR: File is out of sorted order (line %d > line %d)", FNR-1, FNR > "/dev/stderr"
                    exit 1
                }
                if (key != prevKey) {
                    print
                    prevKey = key
                }
            }' | gzip > ${sample}_${label}_filtered.tsv.gz
        # Link input to output for testing
        ln -s ${tsv} ${sample}_${label}_in.tsv.gz
        """
}
