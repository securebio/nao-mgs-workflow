// Test-only workflow that exercises the input-size-aware memory closure
// attached to the `bbmask_resources` label in configs/resources.config.
// MAKE_SPARSE_FILE creates a logical-size-only file of a given byte count via
// truncate (consuming negligible disk). PROBE_BBMASK_MEMORY then takes that
// file under the bbmask_resources label and emits the resolved
// task.memory.toBytes() value. Per-input rows are aggregated into a single
// sorted CSV so the workflow's only output emission is one Path — avoiding
// nf-test channel-sort warnings on tuple emissions containing Long values.

process MAKE_SPARSE_FILE {
    label "single"
    label "coreutils"

    input:
        val size_bytes

    output:
        tuple val(size_bytes), path("sparse.bin")

    script:
        """
        truncate -s ${size_bytes} sparse.bin
        """
}

process PROBE_BBMASK_MEMORY {
    label "bbmask_resources"
    label "coreutils"

    input:
        tuple val(size_bytes), path(reads)

    output:
        path "row.csv"

    script:
        """
        echo "${size_bytes},${task.memory.toBytes()}" > row.csv
        """
}

// The sort labels tier both cpus and memory, so these probes emit both. Note
// the input variable names: the closures in configs/resources.config close over
// `input_file` and `fastq` respectively, matching SORT_FILE and SORT_FASTQ.
process PROBE_SORT_FILE_RESOURCES {
    label "sort_file_resources"
    label "coreutils"

    input:
        tuple val(size_bytes), path(input_file)

    output:
        path "row.csv"

    script:
        """
        echo "${size_bytes},${task.cpus},${task.memory.toBytes()},${SortUtils.bufferSize(task.memory)}" > row.csv
        """
}

process PROBE_SORT_FASTQ_RESOURCES {
    label "sort_fastq_resources"
    label "coreutils"

    input:
        tuple val(size_bytes), path(fastq)

    output:
        path "row.csv"

    script:
        """
        echo "${size_bytes},${task.cpus},${task.memory.toBytes()},${SortUtils.bufferSize(task.memory)}" > row.csv
        """
}

workflow PROBE_RESOURCE_TIER {
    take:
        sizes_ch  // val: logical file size in bytes

    main:
        sparse_ch = MAKE_SPARSE_FILE(sizes_ch)
        rows_ch = PROBE_BBMASK_MEMORY(sparse_ch)
        report_ch = rows_ch.collectFile(name: 'report.csv', sort: true)

    emit:
        report = report_ch
}

workflow PROBE_SORT_RESOURCE_TIERS {
    take:
        sizes_ch  // val: logical file size in bytes

    main:
        sparse_ch = MAKE_SPARSE_FILE(sizes_ch)
        file_rows_ch = PROBE_SORT_FILE_RESOURCES(sparse_ch)
        fastq_rows_ch = PROBE_SORT_FASTQ_RESOURCES(sparse_ch)
        file_report_ch = file_rows_ch.collectFile(name: 'sort_file.csv', sort: true)
        fastq_report_ch = fastq_rows_ch.collectFile(name: 'sort_fastq.csv', sort: true)

    emit:
        sort_file = file_report_ch
        sort_fastq = fastq_report_ch
}
