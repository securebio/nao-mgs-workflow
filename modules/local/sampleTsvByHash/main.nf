// Deterministically downsample each TSV in a list to at most N rows, selecting rows by
// hash of a key column (bottom-N sketch). See sample_tsv_by_hash.py for the guarantees.
//
// Output names prefix the input names rather than rebuilding them, so downstream
// processes that parse the partition taxid out of the filename (e.g.
// EXTRACT_VIRAL_HITS_TO_FASTQ_NOREF_LABELED_LIST) keep working unchanged.
//
// The script reads each file twice so that peak memory scales with n_sample keys rather
// than n_sample rows; a "single" (1 CPU / 4 GB) label is therefore sufficient even when
// n_sample is set high enough to select every read of a long-read partition.
process SAMPLE_TSV_BY_HASH_LIST {
    label "python"
    label "single"
    tag "id=${sample}"
    input:
        tuple val(sample), path(tsvs)
        val(key_column) // Column to hash when selecting rows (e.g. "seq_id")
        val(n_sample) // Maximum rows to retain per input file
    output:
        tuple val(sample), path("sampled_*"), emit: output
        tuple val(sample), path("input_*"), emit: input
    script:
        """
        for tsv in ${tsvs}; do
            sample_tsv_by_hash.py -i \${tsv} -o sampled_\${tsv} -k ${key_column} -n ${n_sample}
            ln -s \${tsv} input_\${tsv}
        done
        """
}
