// Deterministically downsample a TSV to at most N rows, selecting rows by hash of a key
// column (a bottom-N sketch). The selection is reproducible across runs, independent of
// input row order, and nested in N. See downsample_tsv_by_hash.py for the guarantees.
//
// The output name prefixes the input name rather than rebuilding it, so consumers that
// parse a partition key out of the filename keep working unchanged.
//
// Peak memory scales with n_sample keys rather than n_sample rows, so a "single"
// (1 CPU / 4 GB) label suffices even when n_sample is set high enough to retain every
// row of a long-read partition, where each row carries a full read sequence.
process DOWNSAMPLE_TSV_BY_HASH {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(tsv)
        val(key_column) // Column to hash when selecting rows (e.g. "seq_id")
        val(n_sample) // Maximum rows to retain
    output:
        tuple val(sample), path("downsampled_${tsv}"), emit: output
        tuple val(sample), path("input_${tsv}"), emit: input
    script:
        """
        downsample_tsv_by_hash.py -i ${tsv} -o downsampled_${tsv} -k ${key_column} -n ${n_sample}
        # Link input to output for testing
        ln -s ${tsv} input_${tsv}
        """
}
